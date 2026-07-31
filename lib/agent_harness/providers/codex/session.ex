defmodule AgentHarness.Providers.Codex.Session do
  @moduledoc false

  use GenServer

  alias AgentHarness.Provider.{OpenGuardian, Sink}
  alias AgentHarness.Providers.Codex.{Config, Normalizer, Protocol}
  alias AgentHarness.{Response, SessionConfig, Turn}
  alias Codex.Events
  alias Codex.Items

  defmodule State do
    @moduledoc false

    @enforce_keys [
      :owner,
      :owner_monitor,
      :config,
      :prepared,
      :client,
      :codex_options,
      :connection,
      :sink
    ]
    defstruct [
      :owner,
      :owner_monitor,
      :connection_monitor,
      :config,
      :prepared,
      :client,
      :codex_options,
      :connection,
      :sink,
      :provider_session_id,
      :active,
      pending: %{},
      runners: %{},
      interrupts: %{}
    ]
  end

  @spec start(SessionConfig.t(), Sink.t(), pid()) ::
          {:ok, pid(), map()} | {:error, term()}
  def start(%SessionConfig{} = config, %Sink{} = sink, owner) when is_pid(owner) do
    child = {__MODULE__, config: config, sink: sink, owner: owner}

    case DynamicSupervisor.start_child(AgentHarness.ProviderSupervisor, child) do
      {:ok, pid} ->
        case safe_call(pid, :info) do
          {:ok, info} ->
            {:ok, pid, info}

          {:error, reason} ->
            _ = DynamicSupervisor.terminate_child(AgentHarness.ProviderSupervisor, pid)
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  def child_spec(opts) do
    %{
      id: {__MODULE__, make_ref()},
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary,
      type: :worker
    }
  end

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    config = Keyword.fetch!(opts, :config)
    sink = Keyword.fetch!(opts, :sink)
    owner = Keyword.fetch!(opts, :owner)
    guardian = OpenGuardian.start(self(), owner)

    {:ok, {:opening, config, sink, owner, guardian}, {:continue, :open}}
  end

  @impl true
  def handle_continue(:open, {:opening, config, sink, owner, guardian}) do
    with {:ok, prepared} <- Config.prepare(config),
         {:ok, codex_options} <- prepared.client.options(prepared.codex_options),
         :ok <- Config.validate_resolved_options(prepared, codex_options),
         {:ok, connection} <-
           prepared.client.connect(codex_options, prepared.connect_options) do
      state = %State{
        owner: owner,
        owner_monitor: Process.monitor(owner),
        connection_monitor: monitor_connection(connection),
        config: config,
        prepared: prepared,
        client: prepared.client,
        codex_options: codex_options,
        connection: connection,
        sink: sink,
        provider_session_id: prepared.provider_session_id
      }

      OpenGuardian.disarm(guardian)
      {:noreply, state}
    else
      {:error, reason} -> {:stop, reason, {:opening, config, sink, owner, guardian}}
    end
  end

  @impl true
  def handle_call(:info, _from, state) do
    {:reply, %{provider_session_id: state.provider_session_id}, state}
  end

  def handle_call({:start_turn, _turn, _input, _options}, _from, %{connection: nil} = state) do
    {:reply, {:error, :provider_not_found}, state}
  end

  def handle_call({:start_turn, %Turn{} = turn, input, options}, _from, %{active: nil} = state) do
    with {:ok, input} <- Config.input(state.prepared, input),
         thread_options <-
           Config.thread_options(state.prepared, state.config, state.connection),
         {:ok, thread} <- build_thread(state, thread_options),
         turn_options <- Config.turn_options(state.prepared, turn.id, options),
         {:ok, streaming} <- state.client.run_streamed(thread, input, turn_options),
         {:ok, events} <- raw_events(state.client, streaming),
         {:ok, task} <- start_runner(events) do
      provider_turn_ref = make_ref()
      send(task.pid, {:consume, self(), provider_turn_ref})

      runner = %{
        task: task,
        turn_ref: provider_turn_ref,
        streaming: streaming,
        terminal?: false
      }

      active = %{
        ref: provider_turn_ref,
        harness_turn_id: turn.id,
        streaming: streaming,
        provider_session_id: provider_id(state.provider_session_id),
        provider_turn_id: nil,
        message: "",
        usage: nil,
        cancel_requested?: false,
        interrupt_started?: false
      }

      state = %{
        state
        | active: active,
          runners: Map.put(state.runners, task.ref, runner)
      }

      {:reply, {:ok, provider_turn_ref}, state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:start_turn, _turn, _input, _options}, _from, state) do
    {:reply, {:error, :turn_in_progress}, state}
  end

  def handle_call({:respond, provider_request_ref, %Response{} = response}, _from, state) do
    case Map.get(state.pending, provider_request_ref) do
      nil ->
        {:reply, {:error, :request_not_found}, state}

      %{event: event} ->
        event
        |> Protocol.encode_response(response)
        |> handle_encoded_response(state, provider_request_ref)
    end
  end

  def handle_call({:cancel, provider_turn_ref}, _from, state) do
    case state.active do
      %{ref: ^provider_turn_ref, cancel_requested?: true} ->
        {:reply, :ok, state}

      %{ref: ^provider_turn_ref} = active ->
        cancel_state = put_in(state.active.cancel_requested?, true)

        case schedule_active_interrupt(cancel_state, active) do
          {:ok, cancel_state} ->
            {:reply, :ok, cancel_state}

          {:error, reason, _cancel_state} ->
            {:reply, {:error, reason}, state}
        end

      _ ->
        {:reply, {:error, :turn_not_active}, state}
    end
  end

  def handle_call(:close, _from, state) do
    state = cleanup(state)
    {:stop, :normal, :ok, state}
  end

  @impl true
  def handle_info({:codex_stream_event, provider_turn_ref, event}, state) do
    case state.active do
      %{ref: ^provider_turn_ref} ->
        {:noreply, handle_event(state, event)}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:codex_unresolvable_request, id, method, params}, state) do
    request_type = unresolvable_request_type(method)
    raw = %{id: id, method: method, params: params}
    state = fail_unsupported_provider_request(state, request_type, raw)
    {:stop, :normal, state}
  end

  def handle_info({task_ref, result}, state) when is_reference(task_ref) do
    case Map.pop(state.interrupts, task_ref) do
      {nil, _interrupts} ->
        handle_runner_message(task_ref, result, state)

      {interrupt, interrupts} ->
        Process.demonitor(task_ref, [:flush])
        state = %{state | interrupts: interrupts}
        handle_interrupt_result(state, interrupt, result)
    end
  end

  def handle_info(
        {:DOWN, monitor, :process, _pid, _reason},
        %State{owner_monitor: monitor} = state
      ) do
    {:stop, :normal, cleanup(state)}
  end

  def handle_info(
        {:DOWN, monitor, :process, _pid, reason},
        %State{connection_monitor: monitor} = state
      ) do
    Sink.transport_down(state.sink, reason)
    state = cleanup(%{state | connection: nil, connection_monitor: nil})
    {:stop, {:transport_down, reason}, state}
  end

  def handle_info({:DOWN, task_ref, :process, _pid, reason}, state) do
    case Map.pop(state.interrupts, task_ref) do
      {nil, _interrupts} ->
        handle_runner_down(task_ref, reason, state)

      {interrupt, interrupts} ->
        state = %{state | interrupts: interrupts}
        handle_interrupt_result(state, interrupt, {:error, {:interrupt_task_down, reason}})
    end
  end

  def handle_info({:stop_after_provider_failure, _reason}, state),
    do: {:stop, :normal, state}

  @impl true
  def terminate(_reason, %State{} = state), do: cleanup(state)
  def terminate(_reason, {:opening, _config, _sink, _owner, _guardian}), do: :ok

  defp build_thread(%{provider_session_id: nil} = state, thread_options) do
    state.client.start_thread(state.codex_options, thread_options)
  end

  defp build_thread(state, thread_options) do
    state.client.resume_thread(
      state.provider_session_id,
      state.codex_options,
      thread_options
    )
  end

  defp raw_events(client, streaming) do
    {:ok, client.raw_events(streaming)}
  rescue
    error -> {:error, {:stream_setup_failed, error}}
  catch
    kind, reason -> {:error, {:stream_setup_failed, {kind, reason}}}
  end

  defp start_runner(events) do
    task =
      Task.Supervisor.async_nolink(AgentHarness.RunnerSupervisor, fn ->
        receive do
          {:consume, owner, provider_turn_ref} ->
            consume(events, owner, provider_turn_ref)
        end
      end)

    {:ok, task}
  catch
    :exit, reason -> {:error, {:runner_start_failed, reason}}
  end

  defp consume(events, owner, provider_turn_ref) do
    terminal? =
      Enum.reduce(events, false, fn event, terminal? ->
        send(owner, {:codex_stream_event, provider_turn_ref, event})
        terminal? or Normalizer.terminal?(event)
      end)

    {:ok, terminal?}
  rescue
    error -> {:error, {:stream_error, Exception.message(error)}}
  catch
    kind, reason -> {:error, {:stream_error, {kind, reason}}}
  end

  defp handle_event(state, %Events.ThreadStarted{thread_id: thread_id} = event) do
    old_thread_id = provider_id(state.provider_session_id)

    state =
      state
      |> update_provider_ids(event)
      |> Map.put(:provider_session_id, thread_id)

    if old_thread_id == thread_id do
      state
    else
      Sink.session_updated(state.sink, %{provider_session_id: thread_id})
      state
    end
  end

  defp handle_event(state, %Events.TurnStarted{} = event) do
    state = update_provider_ids(state, event)

    case state.active do
      %{cancel_requested?: true} = active ->
        case schedule_active_interrupt(state, active) do
          {:ok, state} -> state
          {:error, reason, state} -> fail_active_turn(state, {:cancel_failed, reason}, event)
        end

      _ ->
        state
    end
  end

  defp handle_event(state, %Events.ServerRequestResolved{} = event) do
    state = update_provider_ids(state, event)
    turn_id = state.active.harness_turn_id

    if Map.has_key?(state.pending, event.request_id) do
      Sink.expire_request(
        state.sink,
        turn_id,
        event.request_id,
        :provider_resolved,
        event
      )
    end

    {:event, type, data} = Normalizer.normalize(event)
    Sink.emit(state.sink, turn_id, type, data, event)
    %{state | pending: Map.delete(state.pending, event.request_id)}
  end

  defp handle_event(state, %Events.ChatgptAuthTokensRefreshRequested{} = event) do
    state = fail_unsupported_provider_request(state, :chatgpt_auth_tokens_refresh, event)
    send(self(), {:stop_after_provider_failure, :chatgpt_auth_tokens_refresh})
    state
  end

  defp handle_event(state, %Events.AttestationGenerateRequested{} = event) do
    state = fail_unsupported_provider_request(state, :attestation_generate, event)
    send(self(), {:stop_after_provider_failure, :attestation_generate})
    state
  end

  defp handle_event(state, event) do
    state =
      state
      |> update_provider_ids(event)
      |> accumulate(event)

    case Normalizer.normalize(event) do
      :ignore ->
        state

      {:session_updated, attrs} ->
        Sink.session_updated(state.sink, attrs)
        state

      {:event, type, data} ->
        Sink.emit(state.sink, state.active.harness_turn_id, type, data, event)
        state

      {:request, provider_ref, attrs} ->
        pending =
          Map.put(state.pending, provider_ref, %{
            event: event,
            turn_ref: state.active.ref
          })

        Sink.request(
          state.sink,
          state.active.harness_turn_id,
          provider_ref,
          attrs,
          event
        )

        %{state | pending: pending}

      {:finish, status, result} ->
        finish_active_turn(state, status, result, event)
    end
  end

  defp update_provider_ids(%{active: nil} = state, _event), do: state

  defp update_provider_ids(state, event) when is_struct(event) do
    thread_id = Map.get(event, :thread_id)
    turn_id = Map.get(event, :turn_id)

    active =
      state.active
      |> maybe_put_provider_id(:provider_session_id, thread_id)
      |> maybe_put_provider_id(:provider_turn_id, turn_id)

    %{state | active: active}
  end

  defp update_provider_ids(state, _event), do: state

  defp maybe_put_provider_id(active, key, value) when is_binary(value) and value != "",
    do: Map.put(active, key, value)

  defp maybe_put_provider_id(active, _key, _value), do: active

  defp accumulate(%{active: nil} = state, _event), do: state

  defp accumulate(state, %Events.ItemAgentMessageDelta{item: item}) do
    text = Map.get(item, "text", Map.get(item, :text, ""))
    put_in(state.active.message, state.active.message <> text)
  end

  defp accumulate(state, %Events.ItemCompleted{item: %Items.AgentMessage{text: text}})
       when is_binary(text) and text != "" do
    put_in(state.active.message, text)
  end

  defp accumulate(state, %Events.ThreadTokenUsageUpdated{usage: usage}) do
    put_in(state.active.usage, usage)
  end

  defp accumulate(state, %Events.TurnCompleted{usage: usage}) when not is_nil(usage) do
    put_in(state.active.usage, usage)
  end

  defp accumulate(state, _event), do: state

  defp finish_active_turn(state, status, result, raw) do
    active = state.active

    result =
      result
      |> maybe_put(:text, nonempty(active.message))
      |> maybe_put(:usage, active.usage)
      |> maybe_put(:provider_session_id, active.provider_session_id)
      |> maybe_put(:provider_turn_id, active.provider_turn_id)

    Sink.finish(state.sink, active.harness_turn_id, status, result, raw)

    runners =
      Map.new(state.runners, fn {ref, runner} ->
        if runner.turn_ref == active.ref do
          {ref, %{runner | terminal?: true}}
        else
          {ref, runner}
        end
      end)

    %{
      state
      | active: nil,
        pending: pending_without_turn(state.pending, active.ref),
        runners: runners
    }
  end

  defp handle_runner_result(state, %{terminal?: true}, _result), do: state

  defp handle_runner_result(state, runner, {:ok, true}) do
    case state.active do
      %{ref: ref} when ref == runner.turn_ref ->
        fail_active_turn(state, :terminal_event_not_processed, nil)

      _ ->
        state
    end
  end

  defp handle_runner_result(state, runner, {:ok, false}) do
    fail_runner_turn(state, runner, :stream_ended_without_terminal_event)
  end

  defp handle_runner_result(state, runner, {:error, reason}) do
    fail_runner_turn(state, runner, reason)
  end

  defp handle_runner_result(state, runner, unexpected) do
    fail_runner_turn(state, runner, {:unexpected_stream_result, unexpected})
  end

  defp handle_runner_message(task_ref, result, state) do
    case Map.pop(state.runners, task_ref) do
      {nil, _runners} ->
        {:noreply, state}

      {runner, runners} ->
        Process.demonitor(task_ref, [:flush])
        state = %{state | runners: runners}
        {:noreply, handle_runner_result(state, runner, result)}
    end
  end

  defp handle_runner_down(task_ref, reason, state) do
    case Map.pop(state.runners, task_ref) do
      {nil, _runners} ->
        {:noreply, state}

      {runner, runners} ->
        state = %{state | runners: runners}

        result =
          if reason == :normal,
            do: {:error, :stream_exited_without_result},
            else: {:error, {:stream_task_down, reason}}

        {:noreply, handle_runner_result(state, runner, result)}
    end
  end

  defp fail_runner_turn(state, runner, reason) do
    case state.active do
      %{ref: ref} when ref == runner.turn_ref ->
        _ = state.client.cancel_stream(runner.streaming, :immediate)
        state = fail_active_turn(state, reason, nil)
        transport_reason = {:stream_failure, reason}
        Sink.transport_down(state.sink, transport_reason)
        state = cleanup(state)
        send(self(), {:stop_after_provider_failure, transport_reason})
        state

      _ ->
        state
    end
  end

  defp fail_active_turn(%{active: nil} = state, _reason, _raw), do: state

  defp fail_active_turn(state, reason, raw) do
    finish_active_turn(state, :failed, %{reason: reason}, raw)
  end

  defp fail_unsupported_provider_request(state, request_type, event) do
    reason = {:unsupported_provider_request, request_type}
    state = fail_active_turn(state, reason, event)
    Sink.transport_down(state.sink, reason)
    cleanup(state)
  end

  defp unresolvable_request_type("account/chatgptAuthTokens/refresh"),
    do: :chatgpt_auth_tokens_refresh

  defp unresolvable_request_type("attestation/generate"), do: :attestation_generate
  defp unresolvable_request_type(method), do: {:unknown, method}

  defp schedule_active_interrupt(state, %{provider_session_id: nil}), do: {:ok, state}
  defp schedule_active_interrupt(state, %{provider_turn_id: nil}), do: {:ok, state}
  defp schedule_active_interrupt(state, %{interrupt_started?: true}), do: {:ok, state}

  defp schedule_active_interrupt(state, active) do
    schedule_interrupt(
      state,
      active.provider_session_id,
      active.provider_turn_id,
      nil
    )
  end

  defp schedule_interrupt(
         %{active: %{interrupt_started?: true}} = state,
         _thread_id,
         _turn_id,
         _ref
       ),
       do: {:ok, state}

  defp schedule_interrupt(state, thread_id, turn_id, request_ref)
       when is_binary(thread_id) and is_binary(turn_id) do
    client = state.client
    connection = state.connection

    try do
      task =
        Task.Supervisor.async_nolink(AgentHarness.RunnerSupervisor, fn ->
          client.turn_interrupt(connection, thread_id, turn_id)
        end)

      interrupt = %{
        task: task,
        turn_ref: state.active && state.active.ref,
        request_ref: request_ref
      }

      state =
        state
        |> put_in([Access.key(:interrupts), task.ref], interrupt)
        |> mark_interrupt_started()

      {:ok, state}
    catch
      :exit, reason -> {:error, {:interrupt_start_failed, reason}, state}
    end
  end

  defp schedule_interrupt(state, _thread_id, _turn_id, _request_ref) do
    {:error, :missing_provider_turn_identifiers, state}
  end

  defp mark_interrupt_started(%{active: nil} = state), do: state
  defp mark_interrupt_started(state), do: put_in(state.active.interrupt_started?, true)

  defp handle_interrupt_result(state, _interrupt, :ok), do: {:noreply, state}

  defp handle_interrupt_result(state, interrupt, {:error, reason}) do
    fail_interrupt(state, interrupt, reason)
  end

  defp handle_interrupt_result(state, interrupt, unexpected) do
    fail_interrupt(state, interrupt, {:unexpected_interrupt_result, unexpected})
  end

  defp fail_interrupt(state, interrupt, reason) do
    case state.active do
      %{ref: turn_ref} when turn_ref == interrupt.turn_ref ->
        failure = {:cancel_failed, reason}
        state = fail_active_turn(state, failure, nil)
        Sink.transport_down(state.sink, failure)
        state = cleanup(state)
        {:stop, :normal, state}

      _ ->
        {:noreply, state}
    end
  end

  defp handle_encoded_response({:ok, payload}, state, provider_request_ref) do
    state.client.respond(state.connection, provider_request_ref, payload)
    |> handle_provider_response(state, provider_request_ref)
  end

  defp handle_encoded_response(
         {:interrupt, thread_id, turn_id},
         state,
         provider_request_ref
       ) do
    cancel_state = mark_cancel_requested(state)

    case schedule_interrupt(cancel_state, thread_id, turn_id, provider_request_ref) do
      {:ok, cancel_state} ->
        cancel_state = %{
          cancel_state
          | pending: Map.delete(cancel_state.pending, provider_request_ref)
        }

        {:reply, :ok, cancel_state}

      {:error, reason, _cancel_state} ->
        {:reply, {:error, reason}, state}
    end
  end

  defp handle_encoded_response({:error, reason}, state, _provider_request_ref) do
    {:reply, {:error, reason}, state}
  end

  defp handle_provider_response(:ok, state, provider_request_ref) do
    {:reply, :ok, %{state | pending: Map.delete(state.pending, provider_request_ref)}}
  end

  defp handle_provider_response({:error, reason}, state, _provider_request_ref) do
    {:reply, {:error, reason}, state}
  end

  defp mark_cancel_requested(%{active: nil} = state), do: state
  defp mark_cancel_requested(state), do: put_in(state.active.cancel_requested?, true)

  defp cleanup(%{connection: nil} = state) do
    state
    |> shutdown_interrupts()
    |> shutdown_runners()
  end

  defp cleanup(state) do
    Enum.each(state.runners, fn {_ref, runner} ->
      unless runner.terminal? do
        _ = state.client.cancel_stream(runner.streaming, :immediate)
      end
    end)

    state =
      state
      |> shutdown_interrupts()
      |> shutdown_runners()

    _ = state.client.disconnect(state.connection)
    %{state | connection: nil, connection_monitor: nil}
  rescue
    _error -> %{state | connection: nil, connection_monitor: nil}
  catch
    _kind, _reason -> %{state | connection: nil, connection_monitor: nil}
  end

  defp shutdown_runners(state) do
    Enum.each(state.runners, fn {_ref, runner} ->
      _ = Task.shutdown(runner.task, :brutal_kill)
    end)

    %{state | runners: %{}, active: nil, pending: %{}}
  end

  defp shutdown_interrupts(state) do
    Enum.each(state.interrupts, fn {_ref, interrupt} ->
      _ = Task.shutdown(interrupt.task, :brutal_kill)
    end)

    %{state | interrupts: %{}}
  end

  defp pending_without_turn(pending, turn_ref) do
    Map.reject(pending, fn {_provider_ref, request} -> request.turn_ref == turn_ref end)
  end

  defp monitor_connection(connection) when is_pid(connection), do: Process.monitor(connection)
  defp monitor_connection(_connection), do: nil

  defp provider_id(value), do: value

  defp nonempty(""), do: nil
  defp nonempty(value), do: value

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp safe_call(pid, message) do
    timeout = Application.get_env(:agent_harness, :codex_startup_call_timeout, 25_000)
    {:ok, GenServer.call(pid, message, timeout)}
  catch
    :exit, reason -> {:error, reason}
  end
end
