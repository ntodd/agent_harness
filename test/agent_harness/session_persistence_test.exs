defmodule AgentHarness.SessionPersistenceTest do
  use ExUnit.Case, async: false

  import Mox

  alias AgentHarness.{Provider, ProviderMock, Request, Response}
  alias AgentHarness.Store.Memory

  setup :set_mox_global
  setup :verify_on_exit!

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
