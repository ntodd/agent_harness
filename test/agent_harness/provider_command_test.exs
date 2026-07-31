defmodule AgentHarness.ProviderCommandTest do
  use ExUnit.Case, async: false

  alias AgentHarness.{Capabilities, Event, Provider, Request, Response, SessionConfig, Turn}

  defmodule CommandProvider do
    @behaviour AgentHarness.Provider

    @impl true
    def open_session(config, sink) do
      test_pid = config.provider_options.test_pid
      send(test_pid, {:provider_sink, sink})

      {:ok,
       %{
         test_pid: test_pid,
         start_turn_mode: Map.get(config.provider_options, :start_turn_mode, :immediate),
         respond_mode: Map.get(config.provider_options, :respond_mode, :controlled),
         cancel_mode: Map.get(config.provider_options, :cancel_mode, :controlled)
       }, %{}}
    end

    @impl true
    def start_turn(%{start_turn_mode: :immediate}, %Turn{} = turn, _input, _options) do
      {:ok, turn.id}
    end

    def start_turn(%{start_turn_mode: :controlled} = handle, %Turn{} = turn, _input, _options) do
      Process.flag(:trap_exit, true)
      send(handle.test_pid, {:provider_turn_start_entered, self(), turn.id})

      receive do
        {:provider_turn_start_result, result} -> result
      end
    end

    @impl true
    def respond(handle, provider_ref, response) do
      run_command(handle.test_pid, :respond, handle.respond_mode, {provider_ref, response})
    end

    @impl true
    def cancel(handle, provider_turn_ref) do
      run_command(handle.test_pid, :cancel, handle.cancel_mode, provider_turn_ref)
    end

    @impl true
    def close_session(handle) do
      send(handle.test_pid, :provider_closed)
      :ok
    end

    @impl true
    def capabilities(_handle), do: Capabilities.new(cancel: :native, questions: :native)

    defp run_command(test_pid, kind, :controlled, payload) do
      Process.flag(:trap_exit, true)
      send(test_pid, {:provider_command_entered, kind, self(), payload})

      receive do
        {:provider_command_result, result} -> result
      end
    end

    defp run_command(test_pid, kind, {:return, result}, payload) do
      send(test_pid, {:provider_command_entered, kind, self(), payload})
      result
    end

    defp run_command(test_pid, kind, :crash, payload) do
      send(test_pid, {:provider_command_entered, kind, self(), payload})
      raise "provider #{kind} crashed"
    end
  end

  test "a response command keeps the session responsive and owns its request once" do
    %{request: request, session: session, subscription: subscription} =
      start_request_session()

    response = Response.answer("A")
    caller = Task.async(fn -> AgentHarness.respond(request, response) end)

    assert_receive {:provider_command_entered, :respond, command, {_provider_ref, ^response}}
    assert %{status: :awaiting_input} = AgentHarness.status(session)
    assert {:error, :response_in_progress} = AgentHarness.respond(request, Response.answer("B"))
    refute_receive {:provider_command_entered, :respond, _duplicate, _payload}, 25

    send(command, {:provider_command_result, :ok})
    assert :ok = Task.await(caller, 1_000)

    assert_receive_event(subscription, :request_resolved)
    assert {:error, :already_resolved} = AgentHarness.respond(request, response)
    assert :ok = AgentHarness.stop_session(session, force: true)
  end

  test "a definite response rejection releases the request claim for retry" do
    %{request: request, session: session, subscription: subscription} =
      start_request_session()

    response = Response.approve()
    first = Task.async(fn -> AgentHarness.respond(request, response) end)
    assert_receive {:provider_command_entered, :respond, first_command, _payload}
    send(first_command, {:provider_command_result, {:error, :rejected}})
    assert {:error, :rejected} = Task.await(first, 1_000)

    assert %{status: :awaiting_input, pending_requests: [%Request{id: request_id}]} =
             AgentHarness.status(session)

    assert request_id == request.id

    second = Task.async(fn -> AgentHarness.respond(request, response) end)
    assert_receive {:provider_command_entered, :respond, second_command, _payload}
    refute first_command == second_command
    send(second_command, {:provider_command_result, :ok})
    assert :ok = Task.await(second, 1_000)
    assert_receive_event(subscription, :request_resolved)

    assert :ok = AgentHarness.stop_session(session, force: true)
  end

  test "forced stop interrupts locally without waiting for or invoking provider cancellation" do
    %{request: request, session: session, subscription: subscription} =
      start_request_session()

    caller = Task.async(fn -> AgentHarness.respond(request, Response.answer("A")) end)
    assert_receive {:provider_command_entered, :respond, command, _payload}
    command_monitor = Process.monitor(command)

    assert :ok = AgentHarness.stop_session(session, force: true)
    assert {:error, :session_stopping} = Task.await(caller, 1_000)
    assert_receive {:DOWN, ^command_monitor, :process, ^command, :killed}
    refute_receive {:provider_command_entered, :cancel, _cancel, _payload}, 25

    assert_receive_event(subscription, :request_expired)

    assert_receive_event(subscription, :turn_interrupted, %{
      status: :interrupted,
      result: %{reason: :forced_session_stop}
    })

    assert_receive_event(subscription, :session_closed)
  end

  test "hard session death kills a response callback even when it traps exits" do
    %{request: request, session: session} = start_request_session()

    caller = Task.async(fn -> AgentHarness.respond(request, Response.answer("A")) end)
    assert_receive {:provider_command_entered, :respond, command, _payload}
    command_monitor = Process.monitor(command)

    Process.exit(AgentHarness.whereis(session.id), :kill)

    assert_receive {:DOWN, ^command_monitor, :process, ^command, :killed}
    assert {:error, {:session_call_failed, _reason}} = Task.await(caller, 1_000)
  end

  test "cancellation is admitted locally once while its provider callback runs asynchronously" do
    %{session: session, subscription: subscription, turn: turn} = start_request_session()

    started_at = System.monotonic_time(:millisecond)
    assert :ok = AgentHarness.cancel(turn)
    assert System.monotonic_time(:millisecond) - started_at < 100

    assert_receive {:provider_command_entered, :cancel, command, provider_turn_ref}
    assert provider_turn_ref == turn.id
    command_monitor = Process.monitor(command)

    assert %{status: :cancelling, pending_requests: []} = AgentHarness.status(session)
    assert :ok = AgentHarness.cancel(turn)
    refute_receive {:provider_command_entered, :cancel, _duplicate, _payload}, 25
    assert_receive_event(subscription, :cancel_requested)
    assert_receive_event(subscription, :request_expired)

    assert :ok = AgentHarness.stop_session(session, force: true)
    assert_receive {:DOWN, ^command_monitor, :process, ^command, :killed}

    assert_receive_event(subscription, :turn_interrupted, %{
      status: :interrupted,
      result: %{reason: :forced_session_stop}
    })
  end

  test "cancellation during turn admission launches one provider cancel after admission" do
    provider_options = %{
      test_pid: self(),
      start_turn_mode: :controlled,
      respond_mode: :controlled,
      cancel_mode: :controlled
    }

    assert {:ok, session} =
             AgentHarness.start_session(:command,
               provider_module: CommandProvider,
               provider_options: provider_options,
               store: false
             )

    cleanup_session_on_exit(session)
    assert_receive {:provider_sink, _sink}
    assert {:ok, subscription} = AgentHarness.subscribe(session, from: :latest)
    assert {:ok, turn} = AgentHarness.start_turn(session, "Work")
    assert_receive {:provider_turn_start_entered, start_task, turn_id}
    assert turn_id == turn.id

    assert :ok = AgentHarness.cancel(turn)
    assert :ok = AgentHarness.cancel(turn)
    assert %{status: :cancelling} = AgentHarness.status(session)
    assert_receive_event(subscription, :cancel_requested)
    refute_receive {:provider_command_entered, :cancel, _command, _payload}, 25

    provider_turn_ref = "provider-#{turn.id}"
    send(start_task, {:provider_turn_start_result, {:ok, provider_turn_ref}})
    assert_receive_event(subscription, :turn_started)
    assert_receive {:provider_command_entered, :cancel, cancel_task, ^provider_turn_ref}
    refute_receive {:provider_command_entered, :cancel, _duplicate, _payload}, 25
    send(cancel_task, {:provider_command_result, :ok})

    assert :ok = AgentHarness.stop_session(session, force: true)
  end

  test "a result already in the mailbox wins the provider-command timeout boundary" do
    %{request: request, session: session, subscription: subscription} =
      start_request_session(provider_command_timeout: 5_000)

    response = Response.answer("A")
    caller = Task.async(fn -> AgentHarness.respond(request, response) end)
    assert_receive {:provider_command_entered, :respond, command_task, _payload}

    server = AgentHarness.whereis(session.id)
    %{provider_commands: commands} = :sys.get_state(server)
    [{task_ref, %{task: %Task{pid: ^command_task}}}] = Map.to_list(commands)
    command_monitor = Process.monitor(command_task)
    :ok = :sys.suspend(server)

    try do
      send(server, {:provider_command_timeout, task_ref})
      send(command_task, {:provider_command_result, :ok})
      assert_receive {:DOWN, ^command_monitor, :process, ^command_task, :normal}
    after
      if Process.alive?(server), do: :sys.resume(server)
    end

    assert :ok = Task.await(caller, 1_000)
    assert_receive_event(subscription, :request_resolved)
    assert %{status: :running} = AgentHarness.status(session)
    assert :ok = AgentHarness.stop_session(session, force: true)
  end

  test "a completed turn admission wins the turn-start timeout boundary" do
    provider_options = %{
      test_pid: self(),
      start_turn_mode: :controlled,
      respond_mode: :controlled,
      cancel_mode: :controlled
    }

    assert {:ok, session} =
             AgentHarness.start_session(:command,
               provider_module: CommandProvider,
               provider_options: provider_options,
               turn_start_timeout: 5_000,
               store: false
             )

    cleanup_session_on_exit(session)
    assert_receive {:provider_sink, _sink}
    assert {:ok, subscription} = AgentHarness.subscribe(session, from: :latest)
    assert {:ok, turn} = AgentHarness.start_turn(session, "Work")
    assert_receive {:provider_turn_start_entered, start_task, turn_id}
    assert turn_id == turn.id

    server = AgentHarness.whereis(session.id)
    %{turn_start_task: %Task{ref: task_ref, pid: ^start_task}} = :sys.get_state(server)
    task_monitor = Process.monitor(start_task)
    :ok = :sys.suspend(server)

    try do
      send(server, {:provider_turn_start_timeout, task_ref})
      send(start_task, {:provider_turn_start_result, {:ok, "provider-turn"}})
      assert_receive {:DOWN, ^task_monitor, :process, ^start_task, :normal}
    after
      if Process.alive?(server), do: :sys.resume(server)
    end

    assert_receive_event(subscription, :turn_started)
    assert %{status: :running, current_turn: %{id: ^turn_id}} = AgentHarness.status(session)
    assert :ok = AgentHarness.stop_session(session, force: true)
  end

  @tag capture_log: true
  test "a response watchdog retires the uncertain provider session and kills its callback" do
    %{request: request, session: session, subscription: subscription} =
      start_request_session(provider_command_timeout: 25)

    {:ok, session_monitor} = AgentHarness.monitor(session)
    caller = Task.async(fn -> AgentHarness.respond(request, Response.answer("A")) end)
    assert_receive {:provider_command_entered, :respond, command, _payload}
    command_monitor = Process.monitor(command)

    assert {:error, {:provider_command_uncertain, :provider_command_timeout}} =
             Task.await(caller, 1_000)

    assert_receive {:DOWN, ^command_monitor, :process, ^command, :killed}
    assert_receive_event(subscription, :request_expired)
    assert_receive_event(subscription, :turn_failed)

    assert_receive {:DOWN, ^session_monitor, :process, _server,
                    {:provider_command_uncertain, :respond, :provider_command_timeout}}

    assert_receive :provider_closed
  end

  @tag capture_log: true
  test "a rejected cancellation retires the session instead of letting the agent run" do
    %{session: session, subscription: subscription, turn: turn} = start_request_session()
    {:ok, session_monitor} = AgentHarness.monitor(session)

    assert :ok = AgentHarness.cancel(turn)
    assert_receive {:provider_command_entered, :cancel, command, _payload}
    send(command, {:provider_command_result, {:error, :interrupt_refused}})

    assert_receive_event(subscription, :request_expired)
    assert_receive_event(subscription, :turn_failed)

    assert_receive {:DOWN, ^session_monitor, :process, _server,
                    {:provider_cancel_failed, :interrupt_refused}}

    assert_receive :provider_closed
  end

  test "provider command timeout validates the public-over-core hierarchy" do
    session = AgentHarness.SessionRef.new(:test, id: "command-timeout-config")

    assert %SessionConfig{provider_command_timeout: 30_000} = SessionConfig.new(session)

    assert_raise ArgumentError, ~r/provider_command_timeout must be a positive integer/, fn ->
      SessionConfig.new(session, provider_command_timeout: 0)
    end

    assert_raise ArgumentError, ~r/must be less than provider_command_call_timeout/, fn ->
      SessionConfig.new(session, provider_command_timeout: 60_000)
    end
  end

  defp start_request_session(options \\ []) do
    provider_options = %{
      test_pid: self(),
      start_turn_mode: Keyword.get(options, :start_turn_mode, :immediate),
      respond_mode: Keyword.get(options, :respond_mode, :controlled),
      cancel_mode: Keyword.get(options, :cancel_mode, :controlled)
    }

    session_options =
      [
        provider_module: CommandProvider,
        provider_options: provider_options,
        store: false
      ] ++ Keyword.take(options, [:provider_command_timeout])

    assert {:ok, session} = AgentHarness.start_session(:command, session_options)
    cleanup_session_on_exit(session)
    assert_receive {:provider_sink, sink}
    assert {:ok, subscription} = AgentHarness.subscribe(session, from: :latest)
    assert {:ok, turn} = AgentHarness.start_turn(session, "Work")
    assert_receive_event(subscription, :turn_started)

    assert :ok =
             Provider.Sink.request(sink, turn.id, :question,
               kind: :question,
               prompt: "A or B?"
             )

    assert_receive {:agent_harness, subscription_ref,
                    %Event{type: :request_created, data: %Request{} = request}}

    assert subscription_ref == subscription.ref

    %{request: request, session: session, sink: sink, subscription: subscription, turn: turn}
  end

  defp assert_receive_event(subscription, type, data \\ :any) do
    assert_receive {:agent_harness, subscription_ref, %Event{type: ^type} = event}
    assert subscription_ref == subscription.ref
    if data != :any, do: assert(event.data == data)
    event
  end

  defp cleanup_session_on_exit(session) do
    on_exit(fn ->
      if AgentHarness.whereis(session.id), do: AgentHarness.stop_session(session, force: true)
    end)
  end
end
