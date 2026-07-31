defmodule AgentHarness.Store.MemoryTest do
  use ExUnit.Case, async: true

  alias AgentHarness.{Event, Request, Turn}
  alias AgentHarness.Store.Memory

  setup do
    store = start_supervised!(Memory)
    %{store: store}
  end

  test "stores independent session snapshots and replaces them by session id", %{store: store} do
    assert :not_found = Memory.fetch_session(store, "session-1")

    assert :ok = Memory.save_session(store, "session-1", %{status: :idle, revision: 1})
    assert :ok = Memory.save_session(store, "session-2", %{status: :running})
    assert :ok = Memory.save_session(store, "session-1", %{status: :running, revision: 2})

    assert {:ok, %{status: :running, revision: 2}} =
             Memory.fetch_session(store, "session-1")

    assert Memory.list_sessions(store) == [
             {"session-1", %{status: :running, revision: 2}},
             {"session-2", %{status: :running}}
           ]
  end

  test "turns belong to an existing session aggregate", %{store: store} do
    turn = Turn.new("session-1", "Implement it", id: "turn-1")

    assert {:error, :session_not_found} = Memory.save_turn(store, turn)

    assert :ok = Memory.save_session(store, "session-1", %{status: :idle})
    assert :ok = Memory.save_turn(store, turn)
    assert {:ok, ^turn} = Memory.fetch_turn(store, "session-1", "turn-1")
    assert :not_found = Memory.fetch_turn(store, "session-1", "missing")
    assert Memory.list_turns(store, "session-1") == {:ok, [turn]}

    updated = %{turn | status: :running, started_at: DateTime.utc_now()}
    assert :ok = Memory.save_turn(store, updated)
    assert {:ok, ^updated} = Memory.fetch_turn(store, "session-1", "turn-1")
  end

  test "events append in sequence order and read through an exclusive cursor", %{store: store} do
    assert :ok = Memory.save_session(store, "session-1", %{status: :running})
    assert :ok = Memory.save_turn(store, Turn.new("session-1", "Run", id: "turn-1"))

    events = [
      event(1, id: "event-1", type: :turn_started),
      event(2, id: "event-2", type: :assistant_delta),
      event(4, id: "event-4", type: :turn_completed)
    ]

    assert Enum.map(events, &Memory.append_event(store, &1)) == [:ok, :ok, :ok]
    assert Memory.events(store, "session-1") == {:ok, events}
    assert Memory.events(store, "session-1", after: 1) == {:ok, Enum.drop(events, 1)}
    assert Memory.events(store, "session-1", after: 1, limit: 1) == {:ok, [Enum.at(events, 1)]}
    assert Memory.events(store, "session-1", after: 4) == {:ok, []}
    assert Memory.latest_sequence(store, "session-1") == {:ok, 4}
  end

  test "turn event queries use the turn index before cursor and limit", %{store: store} do
    assert :ok = Memory.save_session(store, "session-1", %{status: :running})
    assert :ok = Memory.save_turn(store, Turn.new("session-1", "One", id: "turn-1"))
    assert :ok = Memory.save_turn(store, Turn.new("session-1", "Two", id: "turn-2"))

    first = event(1, id: "event-1")
    second = %{event(2, id: "event-2") | turn_id: "turn-2"}
    third = event(3, id: "event-3")

    assert :ok = Memory.append_event(store, first)
    assert :ok = Memory.append_event(store, second)
    assert :ok = Memory.append_event(store, third)

    assert {:ok, [^third]} =
             Memory.events(store, "session-1", turn_id: "turn-1", after: 1, limit: 1)

    assert {:ok, [^second]} = Memory.events(store, "session-1", turn_id: "turn-2")
  end

  test "event appends are retry-safe and reject conflicts or older sequences", %{store: store} do
    assert :ok = Memory.save_session(store, "session-1", %{status: :running})
    assert :ok = Memory.save_turn(store, Turn.new("session-1", "Run", id: "turn-1"))

    second = event(2, id: "event-2")
    assert :ok = Memory.append_event(store, second)
    assert :ok = Memory.append_event(store, second)

    assert {:error, {:event_conflict, 2}} =
             Memory.append_event(store, event(2, id: "other-event"))

    assert {:error, {:event_id_conflict, "event-2"}} =
             Memory.append_event(store, event(3, id: "event-2"))

    assert {:error, {:out_of_order, 2}} =
             Memory.append_event(store, event(1, id: "event-1"))

    assert Memory.events(store, "session-1") == {:ok, [second]}
  end

  test "turn events require the referenced turn to belong to the same session", %{store: store} do
    assert :ok = Memory.save_session(store, "session-1", %{})

    assert {:error, :turn_not_found} =
             Memory.append_event(store, event(1, id: "event-1"))

    session_event =
      Event.new(
        id: "event-2",
        seq: 1,
        session_id: "session-1",
        provider: :codex,
        type: :session_ready
      )

    assert :ok = Memory.append_event(store, session_event)
  end

  test "requests belong to an existing turn and may be updated in place", %{store: store} do
    request =
      Request.new(
        id: "request-1",
        session_id: "session-1",
        turn_id: "turn-1",
        kind: :question,
        prompt: "Which database?",
        provider_ref: 42
      )

    assert {:error, :session_not_found} = Memory.save_request(store, request)
    assert :ok = Memory.save_session(store, "session-1", %{status: :running})
    assert {:error, :turn_not_found} = Memory.save_request(store, request)
    assert :ok = Memory.save_turn(store, Turn.new("session-1", "Run", id: "turn-1"))
    assert :ok = Memory.save_request(store, request)

    assert {:ok, ^request} = Memory.fetch_request(store, "session-1", "request-1")
    assert Memory.list_requests(store, "session-1") == {:ok, [request]}
    assert Memory.list_requests(store, "session-1", turn_id: "turn-1") == {:ok, [request]}
    assert Memory.list_requests(store, "session-1", status: :resolved) == {:ok, []}

    resolved = %{request | status: :resolved, response: %{answer: "Postgres"}}
    assert :ok = Memory.save_request(store, resolved)
    assert {:ok, ^resolved} = Memory.fetch_request(store, "session-1", "request-1")
    assert Memory.list_requests(store, "session-1", status: :resolved) == {:ok, [resolved]}
  end

  test "deleting a session explicitly removes its owned turns, events, and requests", %{
    store: store
  } do
    assert :ok = Memory.save_session(store, "session-1", %{status: :running})
    assert :ok = Memory.save_turn(store, Turn.new("session-1", "Run", id: "turn-1"))

    request =
      Request.new(
        id: "request-1",
        session_id: "session-1",
        turn_id: "turn-1",
        kind: :permission,
        prompt: "Allow?",
        provider_ref: 42
      )

    assert :ok = Memory.save_request(store, request)
    assert :ok = Memory.append_event(store, event(1, id: "event-1"))
    assert :ok = Memory.delete_session(store, "session-1")

    assert :not_found = Memory.fetch_session(store, "session-1")
    assert {:error, :session_not_found} = Memory.fetch_turn(store, "session-1", "turn-1")
    assert {:error, :session_not_found} = Memory.events(store, "session-1")
    assert {:error, :session_not_found} = Memory.fetch_request(store, "session-1", "request-1")
    assert :ok = Memory.delete_session(store, "session-1")
  end

  test "the store process, not the writing process, owns retained values", %{store: store} do
    parent = self()

    writer =
      spawn(fn ->
        :ok = Memory.save_session(store, "session-1", %{owner: self()})
        send(parent, :saved)
      end)

    monitor = Process.monitor(writer)
    assert_receive :saved
    assert_receive {:DOWN, ^monitor, :process, ^writer, :normal}
    assert {:ok, %{owner: ^writer}} = Memory.fetch_session(store, "session-1")
  end

  test "different session aggregates have independent event sequences", %{store: store} do
    assert :ok = Memory.save_session(store, "session-1", %{})
    assert :ok = Memory.save_session(store, "session-2", %{})

    first =
      Event.new(
        id: "event-1",
        seq: 10,
        session_id: "session-1",
        provider: :codex,
        type: :session_ready
      )

    second =
      Event.new(
        id: "event-2",
        seq: 1,
        session_id: "session-2",
        provider: :claude,
        type: :session_ready
      )

    assert :ok = Memory.append_event(store, first)
    assert :ok = Memory.append_event(store, second)
    assert Memory.latest_sequence(store, "session-1") == {:ok, 10}
    assert Memory.latest_sequence(store, "session-2") == {:ok, 1}
  end

  defp event(sequence, opts) do
    Event.new(
      id: Keyword.fetch!(opts, :id),
      seq: sequence,
      session_id: "session-1",
      turn_id: "turn-1",
      provider: :codex,
      type: Keyword.get(opts, :type, :assistant_delta),
      data: %{sequence: sequence}
    )
  end
end
