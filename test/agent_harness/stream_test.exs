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

    assert {:error, :replay_unavailable} =
             AgentHarness.stream(turn, from: :latest, timeout: 10)

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

  @tag capture_log: true
  test "await returns when its SessionServer exits without a terminal event" do
    expect(ProviderMock, :open_session, fn _config, _sink ->
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :start_turn, fn :provider_handle, _turn, "Crash", [] ->
      {:ok, "provider-turn-1"}
    end)

    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    {:ok, session} = AgentHarness.start_session(:test, provider_module: ProviderMock)
    {:ok, turn} = AgentHarness.start_turn(session, "Crash")
    test_pid = self()

    task =
      Task.async(fn ->
        send(test_pid, :await_started)
        AgentHarness.await(turn)
      end)

    assert_receive :await_started
    session_pid = AgentHarness.whereis(session.id)
    wait_for_subscription(session_pid)
    :ok = GenServer.stop(session_pid, :store_failed)

    assert {:error, {:session_down, :store_failed}} = Task.await(task, 1_000)
  end

  @tag capture_log: true
  test "a stream raises when its SessionServer exits without a terminal event" do
    test_pid = self()

    expect(ProviderMock, :open_session, fn _config, _sink ->
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :start_turn, fn :provider_handle, _turn, "Crash stream", [] ->
      {:ok, "provider-turn-1"}
    end)

    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    {:ok, session} = AgentHarness.start_session(:test, provider_module: ProviderMock)
    {:ok, turn} = AgentHarness.start_turn(session, "Crash stream")

    task =
      Task.Supervisor.async_nolink(AgentHarness.RunnerSupervisor, fn ->
        {:ok, stream} = AgentHarness.stream(turn)
        send(test_pid, :stream_started)
        Enum.to_list(stream)
      end)

    assert_receive :stream_started
    session_pid = AgentHarness.whereis(session.id)
    :ok = GenServer.stop(session_pid, :store_failed)

    assert {:exit, {%AgentHarness.SessionDownError{reason: :store_failed}, _stacktrace}} =
             Task.yield(task, 1_000)
  end

  test "subscriptions reject invalid replay cursors and unknown turns" do
    expect(ProviderMock, :open_session, fn _config, _sink ->
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    {:ok, session} = AgentHarness.start_session(:test, provider_module: ProviderMock)

    assert {:error, {:invalid_replay_cursor, :yesterday}} =
             AgentHarness.subscribe(session, from: :yesterday)

    missing_turn = AgentHarness.Turn.new(session.id, "Missing", id: "missing")
    assert {:error, :turn_not_found} = AgentHarness.subscribe(missing_turn, from: :start)
    assert :ok = AgentHarness.stop_session(session)
  end

  test "await uses terminal state and stream reports evicted replay history" do
    test_pid = self()

    expect(ProviderMock, :open_session, fn _config, sink ->
      send(test_pid, {:sink, sink})
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :start_turn, 2, fn :provider_handle, turn, _input, [] ->
      {:ok, turn.id}
    end)

    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    {:ok, session} =
      AgentHarness.start_session(:test,
        provider_module: ProviderMock,
        store: false,
        event_buffer_size: 2
      )

    assert_receive {:sink, sink}
    {:ok, first} = AgentHarness.start_turn(session, "First")
    Provider.Sink.finish(sink, first.id, :completed, %{generation: 1})
    assert {:ok, %{status: :completed}} = AgentHarness.await(first, timeout: 1_000)

    {:ok, second} = AgentHarness.start_turn(session, "Second")
    Provider.Sink.emit(sink, second.id, :message_delta, %{text: "noise"})
    Provider.Sink.finish(sink, second.id, :completed, %{generation: 2})
    assert {:ok, %{status: :completed}} = AgentHarness.await(second, timeout: 1_000)

    assert {:ok, %{status: :completed, result: %{generation: 1}}} =
             AgentHarness.await(first)

    assert {:error, :replay_unavailable} = AgentHarness.stream(first, from: :start)
    assert :ok = AgentHarness.stop_session(session)
  end

  defp wait_for_subscription(session_pid, attempts \\ 50)

  defp wait_for_subscription(_session_pid, 0), do: flunk("await did not subscribe")

  defp wait_for_subscription(session_pid, attempts) do
    case :sys.get_state(session_pid).subscriptions do
      subscriptions when map_size(subscriptions) > 0 ->
        :ok

      _subscriptions ->
        Process.sleep(2)
        wait_for_subscription(session_pid, attempts - 1)
    end
  end
end
