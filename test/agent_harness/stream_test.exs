defmodule AgentHarness.StreamTest do
  use ExUnit.Case, async: false

  import Mox

  alias AgentHarness.{Provider, ProviderMock}

  setup :set_mox_global
  setup :verify_on_exit!

  test "replays a completed turn without leaking session-level events" do
    test_pid = self()

    expect(ProviderMock, :open_session, fn _config, sink ->
      send(test_pid, {:sink, sink})
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :start_turn, fn :provider_handle, _turn, "Hello", [] ->
      {:ok, "provider-turn-1"}
    end)

    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    {:ok, session} = AgentHarness.start_session(:test, provider_module: ProviderMock)
    assert_receive {:sink, sink}

    {:ok, turn} = AgentHarness.start_turn(session, "Hello")
    Provider.Sink.emit(sink, turn.id, :message_delta, %{text: "Hello"})
    Provider.Sink.finish(sink, turn.id, :completed, %{text: "Hello"})

    assert {:ok, %{status: :completed}} = AgentHarness.await(turn, timeout: 1_000)
    assert {:ok, stream} = AgentHarness.stream(turn, from: :start, timeout: 1_000)

    assert Enum.map(stream, & &1.type) == [
             :turn_started,
             :message_delta,
             :turn_completed
           ]

    assert :ok = AgentHarness.stop_session(session)
  end

  test "await returns a timeout without losing the live turn" do
    expect(ProviderMock, :open_session, fn _config, _sink ->
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :start_turn, fn :provider_handle, _turn, "Wait", [] ->
      {:ok, "provider-turn-1"}
    end)

    expect(ProviderMock, :cancel, fn :provider_handle, "provider-turn-1" -> :ok end)
    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    {:ok, session} = AgentHarness.start_session(:test, provider_module: ProviderMock)
    {:ok, turn} = AgentHarness.start_turn(session, "Wait")

    assert {:error, :timeout} = AgentHarness.await(turn, timeout: 1)
    assert %{status: :running} = AgentHarness.status(session)

    assert :ok = AgentHarness.stop_session(session, force: true)
  end
end
