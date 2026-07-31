defmodule AgentHarness.SessionFailureTest do
  use ExUnit.Case, async: false

  import Mox

  alias AgentHarness.{Event, Provider, ProviderMock, Request, Response}

  setup :set_mox_global
  setup :verify_on_exit!

  defmodule FailingStore do
    def fetch_session(_owner, _session_id), do: :not_found
    def save_session(_owner, _session_id, _snapshot), do: {:error, :disk_full}
  end

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

  test "a crashing provider call returns an error instead of exiting the caller" do
    expect(ProviderMock, :open_session, fn _config, _sink ->
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :start_turn, fn :provider_handle, _turn, "Crash", [] ->
      raise "provider callback crashed"
    end)

    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    {:ok, session} = AgentHarness.start_session(:test, provider_module: ProviderMock)

    assert {:error, {:session_call_failed, _reason}} =
             AgentHarness.start_turn(session, "Crash")

    assert eventually(fn -> AgentHarness.whereis(session.id) == nil end)
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

  test "provider-side request resolution expires the matching pending request" do
    test_pid = self()

    expect(ProviderMock, :open_session, fn _config, sink ->
      send(test_pid, {:sink, sink})
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :start_turn, fn :provider_handle, _turn, "Review", [] ->
      {:ok, "provider-turn-1"}
    end)

    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    {:ok, session} = AgentHarness.start_session(:test, provider_module: ProviderMock)
    assert_receive {:sink, sink}
    {:ok, subscription} = AgentHarness.subscribe(session, from: :latest)
    {:ok, turn} = AgentHarness.start_turn(session, "Review")

    Provider.Sink.request(sink, turn.id, "approval-1",
      kind: :permission,
      prompt: "Allow this?"
    )

    assert_receive {:agent_harness, ref, %Event{type: :turn_started}}
    assert ref == subscription.ref

    assert_receive {:agent_harness, ^ref,
                    %Event{type: :request_created, data: %Request{} = request}}

    raw = %{"type" => "server_request_resolved", "request_id" => "approval-1"}

    Provider.Sink.expire_request(
      sink,
      turn.id,
      "approval-1",
      :provider_resolved,
      raw
    )

    assert_receive {:agent_harness, ^ref,
                    %Event{
                      type: :request_expired,
                      data: %Request{id: request_id, status: :expired},
                      raw: ^raw
                    }}

    assert request_id == request.id
    assert %{status: :running, pending_requests: []} = AgentHarness.status(session)

    assert {:error, :request_expired} =
             AgentHarness.respond(request, Response.approve())

    Provider.Sink.finish(sink, turn.id, :completed)
    assert_receive {:agent_harness, ^ref, %Event{type: :turn_completed}}
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

  test "provider process loss fails the active turn and leaves a stoppable session" do
    test_pid = self()

    provider_pid =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    expect(ProviderMock, :open_session, fn _config, sink ->
      send(test_pid, {:sink, sink})
      {:ok, provider_pid, %{}}
    end)

    expect(ProviderMock, :start_turn, fn ^provider_pid, _turn, "Crash", [] ->
      {:ok, "provider-turn-1"}
    end)

    expect(ProviderMock, :close_session, fn ^provider_pid -> :ok end)

    {:ok, session} = AgentHarness.start_session(:test, provider_module: ProviderMock)
    assert_receive {:sink, _sink}
    {:ok, subscription} = AgentHarness.subscribe(session, from: :latest)
    {:ok, turn} = AgentHarness.start_turn(session, "Crash")

    assert_receive {:agent_harness, ref, %Event{type: :turn_started}}
    assert ref == subscription.ref

    Process.exit(provider_pid, :kill)

    assert_receive {:agent_harness, ^ref,
                    %Event{type: :transport_error, data: %{reason: :killed}}}

    assert_receive {:agent_harness, ^ref,
                    %Event{type: :turn_failed, turn_id: turn_id, data: %{result: result}}}

    assert turn_id == turn.id
    assert result == %{reason: :killed}
    assert %{status: :unavailable, current_turn: nil} = AgentHarness.status(session)
    assert {:error, :session_unavailable} = AgentHarness.start_turn(session, "Retry")
    assert {:error, :session_unavailable} = AgentHarness.capabilities(session)
    assert :ok = AgentHarness.stop_session(session)
  end

  test "initialization closes an opened provider when the Store write fails" do
    provider_pid =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    provider_monitor = Process.monitor(provider_pid)

    expect(ProviderMock, :open_session, fn _config, _sink ->
      {:ok, provider_pid, %{}}
    end)

    expect(ProviderMock, :close_session, fn ^provider_pid ->
      send(provider_pid, :stop)
      :ok
    end)

    assert {:error, {:session_initialization_failed, _reason}} =
             AgentHarness.start_session(:test,
               provider_module: ProviderMock,
               store: {FailingStore, :owner}
             )

    assert_receive {:DOWN, ^provider_monitor, :process, ^provider_pid, :normal}
  end

  test "cancellation expires pending requests and late requests cannot revive the turn" do
    test_pid = self()

    expect(ProviderMock, :open_session, fn _config, sink ->
      send(test_pid, {:sink, sink})
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :start_turn, fn :provider_handle, _turn, "Cancel", [] ->
      {:ok, "provider-turn-1"}
    end)

    expect(ProviderMock, :cancel, fn :provider_handle, "provider-turn-1" -> :ok end)
    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    {:ok, session} = AgentHarness.start_session(:test, provider_module: ProviderMock)
    assert_receive {:sink, sink}
    {:ok, subscription} = AgentHarness.subscribe(session, from: :latest)
    {:ok, turn} = AgentHarness.start_turn(session, "Cancel")

    Provider.Sink.request(sink, turn.id, :pending,
      kind: :permission,
      prompt: "Allow?"
    )

    assert_receive {:agent_harness, ref, %Event{type: :turn_started}}
    assert ref == subscription.ref

    assert_receive {:agent_harness, ^ref,
                    %Event{type: :request_created, data: %Request{} = request}}

    assert :ok = AgentHarness.cancel(turn)
    assert_receive {:agent_harness, ^ref, %Event{type: :cancel_requested}}
    assert_receive {:agent_harness, ^ref, %Event{type: :request_expired}}
    assert {:error, :request_expired} = AgentHarness.respond(request, Response.approve())

    Provider.Sink.request(sink, turn.id, :late,
      kind: :permission,
      prompt: "Too late?"
    )

    refute_receive {:agent_harness, ^ref, %Event{type: :request_created}}, 20
    assert %{status: :cancelling, pending_requests: []} = AgentHarness.status(session)
    assert :ok = AgentHarness.cancel(turn)

    Provider.Sink.finish(sink, turn.id, :interrupted)
    assert_receive {:agent_harness, ^ref, %Event{type: :turn_interrupted}}
    assert :ok = AgentHarness.stop_session(session)
  end

  test "a turn id cannot be reused within a session" do
    test_pid = self()

    expect(ProviderMock, :open_session, fn _config, sink ->
      send(test_pid, {:sink, sink})
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :start_turn, fn :provider_handle, _turn, "First", [] ->
      {:ok, "provider-turn-1"}
    end)

    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    {:ok, session} = AgentHarness.start_session(:test, provider_module: ProviderMock)
    assert_receive {:sink, sink}
    {:ok, first} = AgentHarness.start_turn(session, "First", id: "turn-1")
    Provider.Sink.finish(sink, first.id, :completed, %{generation: 1})
    assert {:ok, %{result: %{generation: 1}}} = AgentHarness.await(first, timeout: 1_000)

    assert {:error, {:turn_id_already_used, "turn-1"}} =
             AgentHarness.start_turn(session, "Second", id: "turn-1")

    assert %{status: :idle, current_turn: nil} = AgentHarness.status(session)
    assert :ok = AgentHarness.stop_session(session)
  end

  defp eventually(fun, attempts \\ 20)

  defp eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      true
    else
      Process.sleep(10)
      eventually(fun, attempts - 1)
    end
  end

  defp eventually(_fun, 0), do: false
end
