defmodule AgentHarness.SessionFailureTest do
  use ExUnit.Case, async: false

  import Mox

  alias AgentHarness.{Event, Provider, ProviderMock}

  setup :set_mox_global
  setup :verify_on_exit!

  test "a provider-open failure leaves no registered session" do
    expect(ProviderMock, :open_session, fn _config, _sink -> {:error, :not_authenticated} end)

    id = "failed-session-#{System.unique_integer([:positive])}"

    assert {:error, {:provider_open_failed, :not_authenticated}} =
             AgentHarness.start_session(:test, id: id, provider_module: ProviderMock)

    assert AgentHarness.whereis(id) == nil
  end

  test "a rejected turn leaves the session idle" do
    expect(ProviderMock, :open_session, fn _config, _sink ->
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :start_turn, fn :provider_handle, _turn, "Reject", [] ->
      {:error, :rate_limited}
    end)

    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    {:ok, session} = AgentHarness.start_session(:test, provider_module: ProviderMock)
    assert {:error, :rate_limited} = AgentHarness.start_turn(session, "Reject")
    assert %{status: :idle, current_turn: nil} = AgentHarness.status(session)
    assert :ok = AgentHarness.stop_session(session)
  end

  test "cancellation remains pending until the provider emits a terminal event" do
    test_pid = self()

    expect(ProviderMock, :open_session, fn _config, sink ->
      send(test_pid, {:sink, sink})
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :start_turn, fn :provider_handle, _turn, "Cancel me", [] ->
      {:ok, "provider-turn-1"}
    end)

    expect(ProviderMock, :cancel, fn :provider_handle, "provider-turn-1" -> :ok end)
    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    {:ok, session} = AgentHarness.start_session(:test, provider_module: ProviderMock)
    assert_receive {:sink, sink}
    {:ok, subscription} = AgentHarness.subscribe(session, from: :latest)
    {:ok, turn} = AgentHarness.start_turn(session, "Cancel me")

    assert :ok = AgentHarness.cancel(turn)
    assert :ok = AgentHarness.cancel(turn)
    assert %{status: :cancelling, current_turn: %{id: turn_id}} = AgentHarness.status(session)
    assert turn_id == turn.id

    assert_receive {:agent_harness, ref, %Event{type: :turn_started}}
    assert ref == subscription.ref
    assert_receive {:agent_harness, ^ref, %Event{type: :cancel_requested}}

    Provider.Sink.finish(sink, turn.id, :interrupted, %{reason: :cancelled})

    assert_receive {:agent_harness, ^ref, %Event{type: :turn_interrupted}}
    assert %{status: :idle, current_turn: nil} = AgentHarness.status(session)
    assert :ok = AgentHarness.stop_session(session)
  end

  test "pending requests expire before a terminal failure" do
    test_pid = self()

    expect(ProviderMock, :open_session, fn _config, sink ->
      send(test_pid, {:sink, sink})
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :start_turn, fn :provider_handle, _turn, "Fail", [] ->
      {:ok, "provider-turn-1"}
    end)

    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    {:ok, session} = AgentHarness.start_session(:test, provider_module: ProviderMock)
    assert_receive {:sink, sink}
    {:ok, subscription} = AgentHarness.subscribe(session, from: :latest)
    {:ok, turn} = AgentHarness.start_turn(session, "Fail")

    Provider.Sink.request(sink, turn.id, 7,
      kind: :permission,
      prompt: "Allow this?"
    )

    assert_receive {:agent_harness, ref, %Event{type: :turn_started}}
    assert ref == subscription.ref
    assert_receive {:agent_harness, ^ref, %Event{type: :request_created}}

    Provider.Sink.finish(sink, turn.id, :failed, %{reason: :provider_error})

    assert_receive {:agent_harness, ^ref, %Event{type: :request_expired, seq: expired_seq}}
    assert_receive {:agent_harness, ^ref, %Event{type: :turn_failed, seq: terminal_seq}}
    assert expired_seq < terminal_seq

    assert :ok = AgentHarness.stop_session(session)
  end

  test "events bearing an obsolete sink reference are ignored" do
    expect(ProviderMock, :open_session, fn _config, _sink ->
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :start_turn, fn :provider_handle, _turn, "Run", [] ->
      {:ok, "provider-turn-1"}
    end)

    expect(ProviderMock, :cancel, fn :provider_handle, "provider-turn-1" -> :ok end)
    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    {:ok, session} = AgentHarness.start_session(:test, provider_module: ProviderMock)
    {:ok, subscription} = AgentHarness.subscribe(session, from: :latest)
    {:ok, turn} = AgentHarness.start_turn(session, "Run")
    assert_receive {:agent_harness, ref, %Event{type: :turn_started}}
    assert ref == subscription.ref

    send(
      AgentHarness.whereis(session.id),
      {:agent_harness_provider, make_ref(),
       {:event, turn.id, :message_delta, %{text: "stale"}, nil}}
    )

    refute_receive {:agent_harness, ^ref, %Event{type: :message_delta}}, 20
    assert :ok = AgentHarness.stop_session(session, force: true)
  end
end
