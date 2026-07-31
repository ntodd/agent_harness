defmodule AgentHarness.StartupFinalizationTest do
  use ExUnit.Case, async: false

  alias AgentHarness.{Event, SessionRef}
  alias AgentHarness.Internal.OwnedTask
  alias AgentHarness.Store.Memory

  defmodule ProbeProvider do
    @behaviour AgentHarness.Provider

    alias AgentHarness.{Capabilities, Provider.Sink, Turn}

    @impl true
    def open_session(config, sink) do
      options = config.provider_options

      if reason = Map.get(options, :transport_down_before_open),
        do: Sink.transport_down(sink, reason)

      Process.sleep(Map.get(options, :open_delay, 0))
      send(options.test_pid, {:probe_provider_opened, self()})

      {:ok,
       %{
         test_pid: options.test_pid,
         close_delay: Map.get(options, :close_delay, 0)
       }, %{}}
    end

    @impl true
    def start_turn(_handle, %Turn{} = turn, _input, _options), do: {:ok, turn.id}

    @impl true
    def respond(_handle, _provider_ref, _response), do: :ok

    @impl true
    def cancel(_handle, _provider_turn_ref), do: :ok

    @impl true
    def close_session(handle) do
      send(handle.test_pid, {:probe_provider_close_entered, self()})
      Process.sleep(handle.close_delay)
      send(handle.test_pid, :probe_provider_closed)
      :ok
    end

    @impl true
    def capabilities(_handle), do: Capabilities.new()
  end

  defmodule ProbeStore do
    @behaviour AgentHarness.Store

    alias AgentHarness.Internal.OwnedTask
    alias AgentHarness.Store.Memory

    @impl true
    def save_session(%{mode: :commit_then_hang} = owner, session_id, %{status: :idle} = snapshot) do
      :ok = Memory.save_session(owner.memory, session_id, snapshot)
      send(owner.test_pid, {:probe_store_idle_committed, self()})
      Process.sleep(:infinity)
    end

    def save_session(%{mode: :commit_then_wait} = owner, session_id, %{status: :idle} = snapshot) do
      :ok = Memory.save_session(owner.memory, session_id, snapshot)
      send(owner.test_pid, {:probe_store_idle_committed, self()})

      receive do
        :finish_idle_commit -> :ok
      end
    end

    def save_session(owner, session_id, snapshot) do
      send(owner.test_pid, {:probe_store_save, self(), snapshot.status})
      maybe_delay(owner, :save_session)
      Memory.save_session(owner.memory, session_id, snapshot)
    end

    @impl true
    def fetch_session(owner, session_id), do: Memory.fetch_session(owner.memory, session_id)

    @impl true
    def list_sessions(owner), do: Memory.list_sessions(owner.memory)

    @impl true
    def delete_session(owner, session_id) do
      send(owner.test_pid, {:probe_store_delete, self(), OwnedTask.owner()})
      Memory.delete_session(owner.memory, session_id)
    end

    @impl true
    def save_turn(owner, turn), do: Memory.save_turn(owner.memory, turn)

    @impl true
    def fetch_turn(owner, session_id, turn_id) do
      Memory.fetch_turn(owner.memory, session_id, turn_id)
    end

    @impl true
    def list_turns(owner, session_id), do: Memory.list_turns(owner.memory, session_id)

    @impl true
    def append_event(%{mode: :hang_ready} = owner, %Event{type: :session_ready}) do
      send(owner.test_pid, {:probe_store_append_blocked, self()})
      Process.sleep(:infinity)
    end

    def append_event(%{mode: :fail_ready} = owner, %Event{type: :session_ready}) do
      send(owner.test_pid, {:probe_store_append_failed, self()})
      {:error, :ready_write_failed}
    end

    def append_event(owner, event) do
      send(owner.test_pid, {:probe_store_append, self(), event.type})
      maybe_delay(owner, :append_event)
      Memory.append_event(owner.memory, event)
    end

    @impl true
    def events(owner, session_id, options), do: Memory.events(owner.memory, session_id, options)

    @impl true
    def latest_sequence(owner, session_id), do: Memory.latest_sequence(owner.memory, session_id)

    @impl true
    def save_request(owner, request), do: Memory.save_request(owner.memory, request)

    @impl true
    def fetch_request(owner, session_id, request_id) do
      Memory.fetch_request(owner.memory, session_id, request_id)
    end

    @impl true
    def list_requests(owner, session_id, options) do
      Memory.list_requests(owner.memory, session_id, options)
    end

    defp maybe_delay(%{mode: :slow_success, delay: delay}, operation)
         when operation in [:save_session, :append_event] do
      Process.sleep(delay)
    end

    defp maybe_delay(_owner, _operation), do: :ok
  end

  test "near-deadline provider opening gets a separate bounded finalization budget" do
    store = start_supervised!({Memory, id: make_ref()})
    session_id = unique_id("near-deadline-finalization")

    owner = %{
      memory: store,
      test_pid: self(),
      mode: :slow_success,
      delay: 70
    }

    started_at = System.monotonic_time(:millisecond)

    assert {:ok, %SessionRef{} = session} =
             AgentHarness.start_session(:probe,
               id: session_id,
               provider_module: ProbeProvider,
               provider_options: %{test_pid: self(), open_delay: 80},
               startup_timeout: 100,
               startup_finalization_timeout: 500,
               store: {ProbeStore, owner}
             )

    elapsed = System.monotonic_time(:millisecond) - started_at
    assert elapsed >= 200
    assert elapsed < 700
    assert {:ok, %{status: :idle}} = Memory.fetch_session(store, session_id)
    assert {:ok, [%Event{type: :session_ready}]} = Memory.events(store, session_id)
    assert :ok = AgentHarness.stop_session(session)
    assert_receive :probe_provider_closed

    assert {:ok, %{status: :closed} = snapshot} = Memory.fetch_session(store, session_id)
    refute Map.has_key?(snapshot, :startup)

    assert {:error, :session_id_already_used} =
             AgentHarness.start_session(:probe,
               id: session_id,
               provider_module: ProbeProvider,
               provider_options: %{test_pid: self()},
               store: {Memory, store}
             )
  end

  test "a hanging readiness write is rolled back and does not burn a new session id" do
    store = start_supervised!({Memory, id: make_ref()})
    session_id = unique_id("hanging-finalization")
    owner = %{memory: store, test_pid: self(), mode: :hang_ready}

    assert {:error, :session_start_finalization_timeout} =
             AgentHarness.start_session(:probe,
               id: session_id,
               provider_module: ProbeProvider,
               provider_options: %{test_pid: self()},
               startup_timeout: 50,
               startup_finalization_timeout: 50,
               store: {ProbeStore, owner}
             )

    assert_receive {:probe_store_append_blocked, finalizer}
    refute Process.alive?(finalizer)
    assert_receive {:probe_store_delete, rollback, server}
    assert is_pid(rollback)
    assert is_pid(server)
    assert_receive :probe_provider_closed
    assert :not_found = Memory.fetch_session(store, session_id)

    assert {:ok, retry} =
             AgentHarness.start_session(:probe,
               id: session_id,
               provider_module: ProbeProvider,
               provider_options: %{test_pid: self()},
               store: {Memory, store}
             )

    assert :ok = AgentHarness.stop_session(retry)
    assert_receive :probe_provider_closed
  end

  test "a committed but unacknowledged startup is rolled back when its starter dies" do
    store = start_supervised!({Memory, id: make_ref()})
    session_id = unique_id("unacknowledged-finalization")
    test_pid = self()
    owner = %{memory: store, test_pid: test_pid, mode: :commit_then_hang}

    starter =
      Task.Supervisor.async_nolink(AgentHarness.RunnerSupervisor, fn ->
        AgentHarness.start_session(:probe,
          id: session_id,
          provider_module: ProbeProvider,
          provider_options: %{test_pid: test_pid},
          startup_timeout: 100,
          startup_finalization_timeout: 5_000,
          store: {ProbeStore, owner}
        )
      end)

    assert_receive {:probe_store_idle_committed, finalizer}
    assert Process.alive?(finalizer)
    assert {:ok, %{status: :idle}} = Memory.fetch_session(store, session_id)

    server = AgentHarness.whereis(session_id)
    server_monitor = Process.monitor(server)
    finalizer_monitor = Process.monitor(finalizer)
    Process.exit(starter.pid, :kill)

    assert_receive {:DOWN, ^finalizer_monitor, :process, ^finalizer, :killed}
    assert_receive {:probe_store_delete, rollback, ^server}
    assert is_pid(rollback)
    assert_receive {:DOWN, ^server_monitor, :process, ^server, :normal}
    assert_receive :probe_provider_closed
    assert :not_found = Memory.fetch_session(store, session_id)

    assert {:ok, retry} =
             AgentHarness.start_session(:probe,
               id: session_id,
               provider_module: ProbeProvider,
               provider_options: %{test_pid: self()},
               store: {Memory, store}
             )

    assert :ok = AgentHarness.stop_session(retry)
    assert_receive :probe_provider_closed
  end

  test "a pending idle snapshot survives hard kill and is reclaimed without explicit reuse" do
    store = start_supervised!({Memory, id: make_ref()})
    session_id = unique_id("hard-killed-finalization")
    test_pid = self()
    owner = %{memory: store, test_pid: test_pid, mode: :commit_then_hang}

    starter =
      Task.Supervisor.async_nolink(AgentHarness.RunnerSupervisor, fn ->
        AgentHarness.start_session(:probe,
          id: session_id,
          provider_module: ProbeProvider,
          provider_options: %{test_pid: test_pid},
          startup_timeout: 100,
          startup_finalization_timeout: 5_000,
          store: {ProbeStore, owner}
        )
      end)

    assert_receive {:probe_store_idle_committed, finalizer}

    assert {:ok, %{status: :idle, startup: %{acknowledged: false}}} =
             Memory.fetch_session(store, session_id)

    server = AgentHarness.whereis(session_id)
    server_monitor = Process.monitor(server)
    finalizer_monitor = Process.monitor(finalizer)
    Process.exit(server, :kill)

    assert_receive {:DOWN, ^server_monitor, :process, ^server, :killed}
    assert_receive {:DOWN, ^finalizer_monitor, :process, ^finalizer, :killed}
    assert {:error, :killed} = Task.await(starter, 1_000)

    assert {:ok, retry} =
             AgentHarness.start_session(:probe,
               id: session_id,
               provider_module: ProbeProvider,
               provider_options: %{test_pid: self()},
               store: {Memory, store}
             )

    assert :ok = AgentHarness.stop_session(retry)
    assert_receive :probe_provider_closed
  end

  test "a queued ready message cannot return a dead session before startup acknowledgement" do
    store = start_supervised!({Memory, id: make_ref()})
    session_id = unique_id("ready-down-race")
    test_pid = self()
    owner = %{memory: store, test_pid: test_pid, mode: :commit_then_wait}

    starter =
      Task.Supervisor.async_nolink(AgentHarness.RunnerSupervisor, fn ->
        AgentHarness.start_session(:probe,
          id: session_id,
          provider_module: ProbeProvider,
          provider_options: %{test_pid: test_pid},
          store: {ProbeStore, owner}
        )
      end)

    assert_receive {:probe_store_idle_committed, finalizer}
    true = :erlang.suspend_process(starter.pid)
    send(finalizer, :finish_idle_commit)

    server = AgentHarness.whereis(session_id)

    assert %{status: :idle} =
             eventually(fn ->
               case AgentHarness.status(SessionRef.new(:probe, id: session_id)) do
                 %{status: :idle} = status -> status
                 _not_ready -> false
               end
             end)

    assert {:ok, %{startup: %{acknowledged: false}}} = Memory.fetch_session(store, session_id)

    server_monitor = Process.monitor(server)
    Process.exit(server, :kill)
    assert_receive {:DOWN, ^server_monitor, :process, ^server, :killed}
    true = :erlang.resume_process(starter.pid)
    assert {:error, :killed} = Task.await(starter, 1_000)

    assert {:ok, retry} =
             AgentHarness.start_session(:probe,
               id: session_id,
               provider_module: ProbeProvider,
               provider_options: %{test_pid: self()},
               store: {Memory, store}
             )

    assert :ok = AgentHarness.stop_session(retry)
    assert_receive :probe_provider_closed
  end

  test "early transport failure gets the finalization budget for bounded provider cleanup" do
    started_at = System.monotonic_time(:millisecond)

    assert {:error, {:provider_open_failed, {:transport_down, :early_disconnect}}} =
             AgentHarness.start_session(:probe,
               provider_module: ProbeProvider,
               provider_options: %{
                 test_pid: self(),
                 transport_down_before_open: :early_disconnect,
                 close_delay: 250
               },
               startup_timeout: 20,
               startup_finalization_timeout: 500,
               store: false
             )

    elapsed = System.monotonic_time(:millisecond) - started_at
    assert elapsed >= 240
    assert elapsed < 750
    assert_receive {:probe_provider_close_entered, cleanup_task}
    assert is_pid(cleanup_task)
    assert_receive :probe_provider_closed
  end

  test "an unrelated owned task still cannot delete a live Memory aggregate" do
    store = start_supervised!({Memory, id: make_ref()})
    session_id = unique_id("owned-delete")

    assert {:ok, session} =
             AgentHarness.start_session(:probe,
               id: session_id,
               provider_module: ProbeProvider,
               provider_options: %{test_pid: self()},
               store: {Memory, store}
             )

    task =
      OwnedTask.async_nolink(AgentHarness.RunnerSupervisor, fn ->
        Memory.delete_session(store, session_id)
      end)

    assert {:ok, {:error, :session_active}} = Task.yield(task, 1_000)
    assert {:ok, %{status: :idle}} = Memory.fetch_session(store, session_id)
    assert :ok = AgentHarness.stop_session(session)
    assert_receive :probe_provider_closed
  end

  test "a returned finalization Store failure can degrade to a live-only session" do
    store = start_supervised!({Memory, id: make_ref()})
    session_id = unique_id("degraded-finalization")
    owner = %{memory: store, test_pid: self(), mode: :fail_ready}

    assert {:ok, session} =
             AgentHarness.start_session(:probe,
               id: session_id,
               provider_module: ProbeProvider,
               provider_options: %{test_pid: self()},
               store_failure: :degrade,
               store: {ProbeStore, owner}
             )

    assert_receive {:probe_store_append_failed, finalizer}
    refute Process.alive?(finalizer)
    assert_receive {:probe_store_delete, rollback, server}
    assert is_pid(rollback)
    assert server == AgentHarness.whereis(session_id)
    assert :not_found = Memory.fetch_session(store, session_id)

    assert %{status: :idle, durability: {:degraded, %{operation: :append_event}}} =
             AgentHarness.status(session)

    assert {:ok, subscription} = AgentHarness.subscribe(session, from: :start)

    assert_receive {:agent_harness, ref, %Event{type: :store_failed}}
    assert ref == subscription.ref
    assert_receive {:agent_harness, ^ref, %Event{type: :session_ready}}

    assert :ok = AgentHarness.stop_session(session)
    assert_receive :probe_provider_closed
  end

  test "the SessionServer-owned finalizer may destructively replace a closed aggregate" do
    store = start_supervised!({Memory, id: make_ref()})
    session_id = unique_id("owned-replacement")
    owner = %{memory: store, test_pid: self(), mode: :normal}

    assert {:ok, first} =
             AgentHarness.start_session(:probe,
               id: session_id,
               provider_module: ProbeProvider,
               provider_options: %{test_pid: self()},
               store: {ProbeStore, owner}
             )

    assert :ok = AgentHarness.stop_session(first)
    assert_receive :probe_provider_closed

    assert {:ok, second} =
             AgentHarness.start_session(:probe,
               id: session_id,
               reuse: :closed,
               provider_module: ProbeProvider,
               provider_options: %{test_pid: self()},
               store: {ProbeStore, owner}
             )

    assert_receive {:probe_store_delete, finalizer, server}
    assert is_pid(finalizer)
    assert server == AgentHarness.whereis(session_id)
    assert {:ok, [%Event{type: :session_ready}]} = Memory.events(store, session_id)
    assert :ok = AgentHarness.stop_session(second)
    assert_receive :probe_provider_closed
  end

  defp eventually(fun, attempts \\ 100)

  defp eventually(_fun, 0), do: flunk("condition did not become true")

  defp eventually(fun, attempts) do
    case fun.() do
      false ->
        Process.sleep(2)
        eventually(fun, attempts - 1)

      result ->
        result
    end
  end

  defp unique_id(prefix), do: "#{prefix}-#{System.unique_integer([:positive])}"
end
