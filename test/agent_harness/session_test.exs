defmodule AgentHarness.SessionTest do
  use ExUnit.Case, async: false

  import Mox

  alias AgentHarness.{Event, Provider, ProviderMock, Request, Response, SessionRef, Turn}

  setup :set_mox_global
  setup :verify_on_exit!

  test "opens, registers, reports, and closes a supervised provider session" do
    test_pid = self()

    expect(ProviderMock, :open_session, fn config, sink ->
      send(test_pid, {:opened, config, sink})
      {:ok, :provider_handle, %{provider_session_id: "provider-session-1"}}
    end)

    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    assert {:ok, %SessionRef{id: id, provider: :test} = session} =
             AgentHarness.start_session(:test,
               provider_module: ProviderMock,
               cwd: "/work/project"
             )

    assert_receive {:opened, %{session_id: ^id, cwd: "/work/project"}, _sink}

    assert %{
             session: ^session,
             status: :idle,
             provider_session_id: "provider-session-1",
             current_turn: nil,
             pending_requests: []
           } = AgentHarness.status(session)

    assert :ok = AgentHarness.stop_session(session)
    assert {:error, :session_not_found} = AgentHarness.status(session)
  end

  test "live session inventory does not call a busy SessionServer" do
    expect(ProviderMock, :open_session, fn _config, _sink ->
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    assert {:ok, session} =
             AgentHarness.start_session(:test, provider_module: ProviderMock)

    server = AgentHarness.whereis(session.id)
    :ok = :sys.suspend(server)

    try do
      started_at = System.monotonic_time(:millisecond)
      assert session in AgentHarness.list_sessions()
      assert System.monotonic_time(:millisecond) - started_at < 100
    after
      :ok = :sys.resume(server)
    end

    assert :ok = AgentHarness.stop_session(session)
  end

  test "rejects malformed public options without terminating a session" do
    expect(ProviderMock, :open_session, fn _config, _sink ->
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    assert {:error, {:invalid_session_options, [123]}} =
             AgentHarness.start_session(:test, [123])

    assert {:error, {:invalid_session_id, 123}} =
             AgentHarness.start_session(:test,
               id: 123,
               provider_module: ProviderMock
             )

    {:ok, session} =
      AgentHarness.start_session(:test, provider_module: ProviderMock)

    assert {:error, {:invalid_turn_options, [123]}} =
             AgentHarness.start_turn(session, "Invalid", [123])

    assert {:error, {:invalid_turn_id, 123}} =
             AgentHarness.start_turn(session, "Invalid", id: 123)

    assert {:error, {:invalid_subscriber, :not_a_pid}} =
             AgentHarness.subscribe(session, pid: :not_a_pid)

    assert {:error, {:invalid_subscription_options, [123]}} =
             AgentHarness.subscribe(session, [123])

    server = AgentHarness.whereis(session.id)

    assert {:error, {:invalid_subscriber, :not_a_pid}} =
             GenServer.call(server, {:subscribe, :not_a_pid, nil, :latest})

    assert {:error, {:invalid_force, :yes}} =
             AgentHarness.stop_session(session, force: :yes)

    forged_request =
      Request.new(
        session_id: session.id,
        turn_id: "turn-1",
        kind: :permission,
        provider_ref: :permission
      )

    assert {:error, {:invalid_response, _response}} =
             AgentHarness.respond(
               forged_request,
               %Response{action: :approve, scope: :totally_bogus}
             )

    assert %{status: :idle, current_turn: nil} = AgentHarness.status(session)
    assert Process.alive?(server)
    assert :ok = AgentHarness.stop_session(session)
  end

  test "normalizes provider events and accepts another turn after authoritative completion" do
    test_pid = self()

    expect(ProviderMock, :open_session, fn _config, sink ->
      send(test_pid, {:sink, sink})
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :start_turn, fn :provider_handle,
                                         %Turn{input: "First"} = turn,
                                         "First",
                                         [] ->
      send(test_pid, {:provider_started, turn.id})
      {:ok, "provider-turn-1"}
    end)

    expect(ProviderMock, :start_turn, fn :provider_handle, %Turn{input: "Second"}, "Second", [] ->
      {:ok, "provider-turn-2"}
    end)

    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    {:ok, session} =
      AgentHarness.start_session(:test, provider_module: ProviderMock)

    assert_receive {:sink, sink}
    {:ok, subscription} = AgentHarness.subscribe(session, from: :start)

    assert_receive_event(subscription, :session_ready, 1)

    assert {:ok, %Turn{id: first_turn_id, status: :starting}} =
             AgentHarness.start_turn(session, "First")

    assert_receive {:provider_started, ^first_turn_id}
    assert_receive_event(subscription, :turn_started, 2, first_turn_id)

    assert {:error, {:turn_in_progress, %Turn{id: ^first_turn_id, status: :running}}} =
             AgentHarness.start_turn(session, "Rejected")

    raw = %{"type" => "item.agentMessage.delta"}
    :ok = Provider.Sink.emit(sink, first_turn_id, :message_delta, %{text: "Hello"}, raw)

    assert_receive {:agent_harness, subscription_ref,
                    %Event{
                      seq: 3,
                      turn_id: ^first_turn_id,
                      type: :message_delta,
                      data: %{text: "Hello"},
                      raw: ^raw
                    }}

    assert subscription_ref == subscription.ref

    :ok =
      Provider.Sink.finish(
        sink,
        first_turn_id,
        :completed,
        %{text: "Hello", usage: %{input_tokens: 2}}
      )

    assert_receive_event(subscription, :turn_completed, 4, first_turn_id)

    assert %{status: :idle, current_turn: nil} = AgentHarness.status(session)

    assert {:ok, %Turn{id: second_turn_id, status: :starting}} =
             AgentHarness.start_turn(session, "Second")

    assert_receive_event(subscription, :turn_started, 5, second_turn_id)

    :ok = AgentHarness.stop_session(session, force: true)
  end

  test "routes a structured request exactly once and resumes the turn" do
    test_pid = self()

    expect(ProviderMock, :open_session, fn _config, sink ->
      send(test_pid, {:sink, sink})
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :start_turn, fn :provider_handle, _turn, "Configure", [] ->
      {:ok, "provider-turn-1"}
    end)

    expect(ProviderMock, :respond, fn :provider_handle,
                                      42,
                                      %Response{
                                        action: :answer,
                                        value: %{"database" => ["Postgres"]}
                                      } ->
      :ok
    end)

    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    {:ok, session} =
      AgentHarness.start_session(:test, provider_module: ProviderMock)

    assert_receive {:sink, sink}
    {:ok, subscription} = AgentHarness.subscribe(session, from: :latest)
    {:ok, turn} = AgentHarness.start_turn(session, "Configure")
    assert_receive_event(subscription, :turn_started, 2, turn.id)

    questions = [
      %{
        id: "database",
        prompt: "Which database?",
        choices: [%{label: "Postgres", value: "Postgres"}]
      }
    ]

    :ok =
      Provider.Sink.request(
        sink,
        turn.id,
        42,
        kind: :question,
        prompt: "Configuration required",
        questions: questions
      )

    assert_receive {:agent_harness, subscription_ref,
                    %Event{
                      type: :request_created,
                      data: %Request{id: request_id, questions: ^questions} = request
                    }}

    assert subscription_ref == subscription.ref
    assert %{status: :awaiting_input, pending_requests: [^request]} = AgentHarness.status(session)

    response = Response.answer(%{"database" => ["Postgres"]})

    assert :ok = AgentHarness.respond(request, response)
    assert_receive_event(subscription, :request_resolved, 4, turn.id)
    assert %{status: :running, pending_requests: []} = AgentHarness.status(session)

    assert {:error, :already_resolved} = AgentHarness.respond(request, response)
    assert request_id == request.id

    Provider.Sink.finish(sink, turn.id, :completed, %{text: "Configured"})
    assert_receive_event(subscription, :turn_completed, 5, turn.id)

    assert :ok = AgentHarness.stop_session(session)
  end

  test "serializes competing responses so the provider receives only one" do
    test_pid = self()

    expect(ProviderMock, :open_session, fn _config, sink ->
      send(test_pid, {:sink, sink})
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :start_turn, fn :provider_handle, _turn, "Choose", [] ->
      {:ok, "provider-turn-1"}
    end)

    expect(ProviderMock, :respond, fn :provider_handle, :question_ref, %Response{} ->
      Process.sleep(10)
      :ok
    end)

    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    {:ok, session} = AgentHarness.start_session(:test, provider_module: ProviderMock)
    assert_receive {:sink, sink}
    {:ok, subscription} = AgentHarness.subscribe(session, from: :latest)
    {:ok, turn} = AgentHarness.start_turn(session, "Choose")

    Provider.Sink.request(sink, turn.id, :question_ref,
      kind: :question,
      prompt: "A or B?"
    )

    assert_receive {:agent_harness, ref, %Event{type: :turn_started}}
    assert ref == subscription.ref

    assert_receive {:agent_harness, ^ref,
                    %Event{type: :request_created, data: %Request{} = request}}

    response = Response.answer("A")

    results =
      for _index <- 1..2 do
        Task.async(fn -> AgentHarness.respond(request, response) end)
      end
      |> Task.await_many()

    assert :ok in results
    assert {:error, :response_in_progress} in results

    Provider.Sink.finish(sink, turn.id, :completed)
    assert_receive {:agent_harness, ^ref, %Event{type: :request_resolved}}
    assert_receive {:agent_harness, ^ref, %Event{type: :turn_completed}}
    assert :ok = AgentHarness.stop_session(session)
  end

  defp assert_receive_event(subscription, type, seq, turn_id \\ nil) do
    assert_receive {:agent_harness, subscription_ref,
                    %Event{type: ^type, seq: ^seq, turn_id: ^turn_id}}

    assert subscription_ref == subscription.ref
  end
end
