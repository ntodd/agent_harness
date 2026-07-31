defmodule AgentHarness.SessionRetentionTest do
  use ExUnit.Case, async: false

  import Mox

  alias AgentHarness.{Provider, ProviderMock, Request, Response}
  alias AgentHarness.Store.Memory

  setup :set_mox_global
  setup :verify_on_exit!

  test "evicts heavy completed state FIFO and recovers it through the Store" do
    test_pid = self()
    store = start_supervised!({Memory, id: make_ref()})

    expect(ProviderMock, :open_session, fn _config, sink ->
      send(test_pid, {:sink, sink})
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :start_turn, 2, fn :provider_handle, turn, _input, [] ->
      {:ok, turn.id}
    end)

    expect(ProviderMock, :respond, fn :provider_handle, :resolved_provider_ref, %Response{} ->
      :ok
    end)

    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    large_input = {:old_input, String.duplicate("i", 100_000)}
    large_result = %{text: String.duplicate("r", 100_000)}
    large_raw = {:old_raw, String.duplicate("w", 100_000)}
    old_provider_ref = {:old_provider_ref, String.duplicate("p", 100_000)}

    {:ok, session} =
      AgentHarness.start_session(:test,
        provider_module: ProviderMock,
        store: {Memory, store},
        event_buffer_size: 1,
        completed_turn_cache_size: 1
      )

    assert_receive {:sink, sink}

    {:ok, first} = AgentHarness.start_turn(session, large_input, id: "retained-turn-1")

    Provider.Sink.request(sink, first.id, :resolved_provider_ref,
      kind: :question,
      prompt: "Resolve now?"
    )

    assert %Request{} = resolved_request = pending_request(session)
    assert :ok = AgentHarness.respond(resolved_request, Response.answer("Now"))

    Provider.Sink.request(sink, first.id, old_provider_ref,
      kind: :question,
      prompt: "Continue?"
    )

    assert %Request{} = expired_request = pending_request(session)
    Provider.Sink.finish(sink, first.id, :completed, large_result, large_raw)

    assert {:ok, %{result: ^large_result}} = AgentHarness.await(first, timeout: 1_000)

    {:ok, second} = AgentHarness.start_turn(session, "second", id: "retained-turn-2")
    Provider.Sink.finish(sink, second.id, :completed, %{generation: 2})
    assert {:ok, %{result: %{generation: 2}}} = AgentHarness.await(second, timeout: 1_000)

    state = session.id |> AgentHarness.whereis() |> :sys.get_state()
    cache = state.completed_turn_cache

    refute Map.has_key?(state.turns, first.id)
    refute Map.has_key?(cache.events, first.id)
    refute Enum.any?(state.requests, fn {_id, saved} -> saved.turn_id == first.id end)
    assert Map.keys(state.turns) == [second.id]
    assert Map.keys(cache.events) == [second.id]
    assert cache.count == 1
    assert cache.pruned?
    assert :queue.to_list(cache.order) == [second.id]

    refute Enum.any?(state.turns, fn {_id, turn} -> turn.input == large_input end)
    refute Enum.any?(state.turns, fn {_id, turn} -> turn.result == large_result end)
    refute Enum.any?(cache.events, fn {_id, event} -> event.raw == large_raw end)

    refute Enum.any?(AgentHarness.EventBuffer.to_list(state.event_buffer), fn event ->
             event.raw == large_raw
           end)

    refute Enum.any?(state.requests, fn {_id, saved} ->
             saved.provider_ref == old_provider_ref
           end)

    assert {:ok, %{status: :completed, result: ^large_result}} =
             AgentHarness.await(first, timeout: 1_000)

    assert {:ok, stream} = AgentHarness.stream(first, from: :start, timeout: 1_000)
    events = Enum.to_list(stream)

    assert Enum.map(events, & &1.type) == [
             :turn_started,
             :request_created,
             :request_resolved,
             :request_created,
             :request_expired,
             :turn_completed
           ]

    assert %AgentHarness.Event{raw: ^large_raw} = List.last(events)

    assert {:error, :already_resolved} =
             AgentHarness.respond(resolved_request, Response.answer("Again"))

    assert {:error, :request_expired} =
             AgentHarness.respond(expired_request, Response.answer("Yes"))

    assert {:error, {:turn_id_already_used, "retained-turn-1"}} =
             AgentHarness.start_turn(session, "duplicate", id: "retained-turn-1")

    assert :ok = AgentHarness.stop_session(session)
  end

  test "a finite no-store cache uses the buffer then fails without hanging" do
    test_pid = self()

    expect(ProviderMock, :open_session, fn _config, sink ->
      send(test_pid, {:sink, sink})
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :start_turn, 3, fn :provider_handle, turn, _input, [] ->
      {:ok, turn.id}
    end)

    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    {:ok, session} =
      AgentHarness.start_session(:test,
        provider_module: ProviderMock,
        store: false,
        event_buffer_size: 1,
        completed_turn_cache_size: 0
      )

    assert_receive {:sink, sink}

    {:ok, first} = AgentHarness.start_turn(session, "first", id: "ephemeral-turn-1")
    Provider.Sink.finish(sink, first.id, :completed, %{generation: 1})

    assert {:ok, %{result: %{generation: 1}}} =
             AgentHarness.await(first, timeout: 1_000)

    assert {:ok, buffered_stream} =
             AgentHarness.stream(first, from: :start, timeout: 1_000)

    assert [%AgentHarness.Event{type: :turn_completed}] = Enum.to_list(buffered_stream)

    assert {:ok, subscription} = AgentHarness.subscribe(first, from: :start)

    assert_receive {:agent_harness, subscription_ref, %AgentHarness.Event{type: :turn_completed}}

    assert subscription_ref == subscription.ref
    assert :ok = AgentHarness.unsubscribe(subscription)

    {:ok, second} = AgentHarness.start_turn(session, "second")
    assert %{status: :running} = wait_for_status(session, :running)

    state = session.id |> AgentHarness.whereis() |> :sys.get_state()
    assert state.current_turn.id == second.id
    assert Map.has_key?(state.turns, second.id)

    assert {:error, :replay_unavailable} = AgentHarness.await(first, timeout: 100)
    assert {:error, :replay_unavailable} = AgentHarness.subscribe(first, from: :start)
    assert {:error, :replay_unavailable} = AgentHarness.stream(first, from: :start)

    Provider.Sink.finish(sink, second.id, :completed, %{generation: 2})
    assert {:ok, %{result: %{generation: 2}}} = AgentHarness.await(second, timeout: 1_000)

    assert {:error, {:turn_id_history_unavailable, "ephemeral-turn-1"}} =
             AgentHarness.start_turn(session, "duplicate", id: "ephemeral-turn-1")

    {:ok, third} = AgentHarness.start_turn(session, "generated id still works")
    Provider.Sink.finish(sink, third.id, :completed, %{generation: 3})
    assert {:ok, %{result: %{generation: 3}}} = AgentHarness.await(third, timeout: 1_000)

    assert :ok = AgentHarness.stop_session(session)
  end

  defp pending_request(session, attempts \\ 100)

  defp pending_request(_session, 0), do: flunk("provider request was not registered")

  defp pending_request(session, attempts) do
    case AgentHarness.status(session) do
      %{pending_requests: [%Request{} = request]} ->
        request

      _status ->
        Process.sleep(2)
        pending_request(session, attempts - 1)
    end
  end

  defp wait_for_status(session, expected, attempts \\ 100)

  defp wait_for_status(_session, expected, 0),
    do: flunk("session did not reach #{inspect(expected)}")

  defp wait_for_status(session, expected, attempts) do
    case AgentHarness.status(session) do
      %{status: ^expected} = status ->
        status

      _status ->
        Process.sleep(2)
        wait_for_status(session, expected, attempts - 1)
    end
  end
end
