defmodule AgentHarness.SessionPersistenceTest do
  use ExUnit.Case, async: false

  import Mox

  alias AgentHarness.{Provider, ProviderMock, Request, Response}
  alias AgentHarness.Store
  alias AgentHarness.Store.Memory

  setup :set_mox_global
  setup :verify_on_exit!

  defmodule StrictStore do
    @behaviour AgentHarness.Store

    alias AgentHarness.Store.Memory

    defdelegate save_session(owner, session_id, snapshot), to: Memory
    defdelegate fetch_session(owner, session_id), to: Memory
    defdelegate list_sessions(owner), to: Memory
    defdelegate delete_session(owner, session_id), to: Memory
    defdelegate save_turn(owner, turn), to: Memory
    defdelegate fetch_turn(owner, session_id, turn_id), to: Memory
    defdelegate list_turns(owner, session_id), to: Memory
    defdelegate append_event(owner, event), to: Memory
    defdelegate events(owner, session_id, options), to: Memory
    defdelegate latest_sequence(owner, session_id), to: Memory
    defdelegate save_request(owner, request), to: Memory
    defdelegate fetch_request(owner, session_id, request_id), to: Memory
    defdelegate list_requests(owner, session_id, options), to: Memory
  end

  defmodule CrashingReplayStore do
    @behaviour AgentHarness.Store

    alias AgentHarness.Store.Memory

    defdelegate save_session(owner, session_id, snapshot), to: Memory
    defdelegate fetch_session(owner, session_id), to: Memory
    defdelegate list_sessions(owner), to: Memory
    defdelegate delete_session(owner, session_id), to: Memory
    defdelegate save_turn(owner, turn), to: Memory
    defdelegate fetch_turn(owner, session_id, turn_id), to: Memory
    defdelegate list_turns(owner, session_id), to: Memory
    defdelegate append_event(owner, event), to: Memory
    defdelegate latest_sequence(owner, session_id), to: Memory
    defdelegate save_request(owner, request), to: Memory
    defdelegate fetch_request(owner, session_id, request_id), to: Memory
    defdelegate list_requests(owner, session_id, options), to: Memory

    def events(_owner, _session_id, _options), do: raise("store unavailable")
  end

  defmodule TurnFailingStore do
    @behaviour AgentHarness.Store

    alias AgentHarness.Store.Memory

    defdelegate save_session(owner, session_id, snapshot), to: Memory
    defdelegate fetch_session(owner, session_id), to: Memory
    defdelegate list_sessions(owner), to: Memory
    defdelegate delete_session(owner, session_id), to: Memory
    def save_turn(_owner, _turn), do: {:error, :disk_full}
    defdelegate fetch_turn(owner, session_id, turn_id), to: Memory
    defdelegate list_turns(owner, session_id), to: Memory
    defdelegate append_event(owner, event), to: Memory
    defdelegate events(owner, session_id, options), to: Memory
    defdelegate latest_sequence(owner, session_id), to: Memory
    defdelegate save_request(owner, request), to: Memory
    defdelegate fetch_request(owner, session_id, request_id), to: Memory
    defdelegate list_requests(owner, session_id, options), to: Memory
  end

  test "checkpoints sessions, provider ids, turns, requests, and ordered events" do
    test_pid = self()
    store = start_supervised!({Memory, id: make_ref()})

    expect(ProviderMock, :open_session, fn _config, sink ->
      send(test_pid, {:sink, sink})
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :start_turn, fn :provider_handle, _turn, "Persist me", [] ->
      {:ok, "provider-turn-1"}
    end)

    expect(ProviderMock, :respond, fn :provider_handle, :question_ref, %Response{} ->
      :ok
    end)

    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    {:ok, session} =
      AgentHarness.start_session(:test,
        provider_module: ProviderMock,
        store: {Memory, store},
        event_buffer_size: 2
      )

    assert_receive {:sink, sink}
    assert {:ok, %{status: :idle}} = Memory.fetch_session(store, session.id)

    Provider.Sink.session_updated(sink, %{provider_session_id: "provider-session-1"})
    {:ok, turn} = AgentHarness.start_turn(session, "Persist me")

    Provider.Sink.request(sink, turn.id, :question_ref,
      kind: :question,
      prompt: "Continue?"
    )

    {:ok, %Request{} = request} =
      eventually(fn ->
        case Memory.list_requests(store, session.id, status: :pending) do
          {:ok, [request]} -> {:ok, request}
          _other -> false
        end
      end)

    assert :ok = AgentHarness.respond(request, Response.answer("Yes"))

    Provider.Sink.finish(sink, turn.id, :completed, %{text: "Done"})

    assert {:ok, %{status: :completed}} =
             eventually(fn ->
               case Memory.fetch_turn(store, session.id, turn.id) do
                 {:ok, %{status: :completed}} = result -> result
                 _other -> false
               end
             end)

    assert {:ok, stream} = AgentHarness.stream(turn, from: :start, timeout: 1_000)

    assert Enum.map(stream, & &1.type) == [
             :turn_started,
             :request_created,
             :request_resolved,
             :turn_completed
           ]

    assert :ok = AgentHarness.stop_session(session)

    assert {:ok,
            %{
              status: :closed,
              provider_session_id: "provider-session-1",
              current_turn_id: nil
            }} = Memory.fetch_session(store, session.id)

    assert {:ok, %{status: :completed, result: %{text: "Done"}}} =
             Memory.fetch_turn(store, session.id, turn.id)

    assert {:ok, [%Request{status: :resolved, response: %Response{action: :answer}}]} =
             Memory.list_requests(store, session.id)

    assert {:ok, events} = Memory.events(store, session.id)
    assert Enum.map(events, & &1.seq) == Enum.to_list(1..length(events))

    assert Enum.map(events, & &1.type) == [
             :session_ready,
             :session_updated,
             :turn_started,
             :request_created,
             :request_resolved,
             :turn_completed,
             :session_closed
           ]
  end

  test "replay calls the Store behaviour's events/3 callback" do
    test_pid = self()
    store = start_supervised!({Memory, id: make_ref()})

    expect(ProviderMock, :open_session, fn _config, sink ->
      send(test_pid, {:sink, sink})
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :start_turn, fn :provider_handle, _turn, "Strict", [] ->
      {:ok, "provider-turn-1"}
    end)

    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    {:ok, session} =
      AgentHarness.start_session(:test,
        provider_module: ProviderMock,
        store: {StrictStore, store}
      )

    assert_receive {:sink, sink}
    {:ok, turn} = AgentHarness.start_turn(session, "Strict")
    Provider.Sink.finish(sink, turn.id, :completed)

    assert {:ok, stream} = AgentHarness.stream(turn, from: :start, timeout: 1_000)
    assert Enum.map(stream, & &1.type) == [:turn_started, :turn_completed]
    assert :ok = AgentHarness.stop_session(session)
  end

  test "replay falls back to the in-memory buffer when the Store raises" do
    test_pid = self()
    store = start_supervised!({Memory, id: make_ref()})

    expect(ProviderMock, :open_session, fn _config, sink ->
      send(test_pid, {:sink, sink})
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :start_turn, fn :provider_handle, _turn, "Fallback", [] ->
      {:ok, "provider-turn-1"}
    end)

    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    {:ok, session} =
      AgentHarness.start_session(:test,
        provider_module: ProviderMock,
        store: {CrashingReplayStore, store}
      )

    assert_receive {:sink, sink}
    {:ok, turn} = AgentHarness.start_turn(session, "Fallback")
    Provider.Sink.finish(sink, turn.id, :completed)

    assert {:ok, stream} = AgentHarness.stream(turn, from: :start, timeout: 1_000)
    assert Enum.map(stream, & &1.type) == [:turn_started, :turn_completed]
    assert %{status: :idle} = AgentHarness.status(session)
    assert :ok = AgentHarness.stop_session(session)
  end

  test "a retained Store rejects reuse of a stopped logical session id before opening a provider" do
    store = start_supervised!({Memory, id: make_ref()})
    session_id = "retained-#{System.unique_integer([:positive])}"

    expect(ProviderMock, :open_session, fn _config, _sink ->
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    {:ok, session} =
      AgentHarness.start_session(:test,
        id: session_id,
        provider_module: ProviderMock,
        store: {Memory, store}
      )

    assert :ok = AgentHarness.stop_session(session)

    assert {:error, :session_id_already_used} =
             AgentHarness.start_session(:test,
               id: session_id,
               provider_module: ProviderMock,
               store: {Memory, store}
             )

    assert {:ok, %{status: :closed}} = Store.Memory.fetch_session(store, session_id)
  end

  test "a live Memory aggregate cannot be purged" do
    test_pid = self()
    store = start_supervised!({Memory, id: make_ref()})

    expect(ProviderMock, :open_session, fn _config, sink ->
      send(test_pid, {:sink, sink})
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :start_turn, fn :provider_handle, _turn, "Still alive", [] ->
      {:ok, "provider-turn"}
    end)

    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    {:ok, session} =
      AgentHarness.start_session(:test,
        id: "live-purge-#{System.unique_integer([:positive])}",
        provider_module: ProviderMock,
        store: {Memory, store}
      )

    assert_receive {:sink, sink}
    assert {:error, :session_active} = Memory.delete_session(store, session.id)
    assert {:error, :session_active} = AgentHarness.purge_session(session, store: {Memory, store})

    assert {:ok, turn} = AgentHarness.start_turn(session, "Still alive")
    Provider.Sink.finish(sink, turn.id, :completed)
    assert {:ok, %{status: :completed}} = AgentHarness.await(turn, timeout: 1_000)
    assert :ok = AgentHarness.stop_session(session)
  end

  test "store write failures explicitly degrade a live session instead of crashing it" do
    store = start_supervised!({Memory, id: make_ref()})

    expect(ProviderMock, :open_session, fn _config, _sink ->
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :start_turn, fn :provider_handle, _turn, "Degrade", [] ->
      {:error, :rejected}
    end)

    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    {:ok, session} =
      AgentHarness.start_session(:test,
        provider_module: ProviderMock,
        store: {TurnFailingStore, store}
      )

    {:ok, subscription} = AgentHarness.subscribe(session)
    assert {:ok, turn} = AgentHarness.start_turn(session, "Degrade")

    assert_receive {:agent_harness, ref,
                    %AgentHarness.Event{
                      type: :store_failed,
                      data: %{operation: :save_turn, reason: :disk_full}
                    }}

    assert ref == subscription.ref

    assert {:error, %AgentHarness.Event{type: :turn_failed}} =
             AgentHarness.await(turn, timeout: 1_000)

    assert %{status: :idle, durability: {:degraded, %{reason: :disk_full}}} =
             AgentHarness.status(session)

    assert :ok = AgentHarness.stop_session(session)
  end

  test "an explicitly reused closed id starts a fresh aggregate" do
    store = start_supervised!({Memory, id: make_ref()})
    session_id = "reused-#{System.unique_integer([:positive])}"

    expect(ProviderMock, :open_session, 2, fn _config, _sink ->
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :close_session, 2, fn :provider_handle -> :ok end)

    {:ok, first} =
      AgentHarness.start_session(:test,
        id: session_id,
        provider_module: ProviderMock,
        store: {Memory, store}
      )

    assert :ok = AgentHarness.stop_session(first)

    assert {:ok, second} =
             AgentHarness.start_session(:test,
               id: session_id,
               reuse: :closed,
               provider_module: ProviderMock,
               store: {Memory, store}
             )

    assert {:ok, events} = Memory.events(store, session_id)
    assert Enum.map(events, & &1.type) == [:session_ready]
    assert :ok = AgentHarness.stop_session(second)
    assert :ok = AgentHarness.purge_session(second, store: {Memory, store})
    assert :not_found = Memory.fetch_session(store, session_id)
  end

  test "shutdown closes the provider and persists the final session state" do
    store = start_supervised!({Memory, id: make_ref()})

    expect(ProviderMock, :open_session, fn _config, _sink ->
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    {:ok, session} =
      AgentHarness.start_session(:test,
        provider_module: ProviderMock,
        store: {Memory, store}
      )

    server = AgentHarness.whereis(session.id)
    monitor = Process.monitor(server)
    Process.exit(server, :shutdown)

    assert_receive {:DOWN, ^monitor, :process, ^server, :shutdown}
    assert {:ok, %{status: :closed}} = Memory.fetch_session(store, session.id)
    assert {:ok, events} = Memory.events(store, session.id)
    assert List.last(events).type == :session_closed
  end

  @tag capture_log: true
  test "inventory exposes stale snapshots for explicit replacement" do
    store = start_supervised!({Memory, id: make_ref()})
    session_id = "reconcile-#{System.unique_integer([:positive])}"

    expect(ProviderMock, :open_session, 2, fn _config, _sink ->
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    {:ok, first} =
      AgentHarness.start_session(:test,
        id: session_id,
        provider_module: ProviderMock,
        store: {Memory, store}
      )

    assert first in AgentHarness.list_sessions()

    assert {:ok, [%{session_id: ^session_id, live?: true}]} =
             AgentHarness.list_stored_sessions(store: {Memory, store})

    server = AgentHarness.whereis(session_id)
    monitor = Process.monitor(server)
    Process.exit(server, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^server, :killed}

    assert {:ok, [%{session_id: ^session_id, live?: false}]} =
             AgentHarness.list_stored_sessions(store: {Memory, store})

    assert {:ok, replacement} =
             AgentHarness.start_session(:test,
               id: session_id,
               reuse: :replace,
               provider_module: ProviderMock,
               store: {Memory, store}
             )

    assert :ok = AgentHarness.stop_session(replacement)
  end

  defp eventually(fun, attempts \\ 50)

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
end
