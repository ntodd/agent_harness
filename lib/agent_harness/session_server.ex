defmodule AgentHarness.SessionServer do
  @moduledoc false

  use GenServer

  require Logger

  alias AgentHarness.{
    Event,
    EventBuffer,
    Request,
    Response,
    Subscription,
    Telemetry,
    Turn
  }

  alias AgentHarness.Provider.Sink
  alias AgentHarness.Internal.OwnedTask

  defmodule CompletedTurnCache do
    @moduledoc false

    defstruct events: %{}, order: :queue.new(), count: 0, pruned?: false
  end

  defmodule State do
    @moduledoc false

    @enforce_keys [
      :session,
      :config,
      :provider,
      :sink,
      :event_buffer,
      :store,
      :created_at,
      :config_fingerprint
    ]
    defstruct [
      :session,
      :config,
      :provider,
      :provider_handle,
      :provider_monitor,
      :provider_session_id,
      :sink,
      :starter,
      :starter_monitor,
      :open_task,
      :open_timer,
      :open_failure,
      :startup_finalization_task,
      :startup_finalization_timer,
      :startup_replacement,
      :startup_replacement_marker,
      :startup_rollback_task,
      :startup_rollback_timer,
      :startup_failure,
      :current_turn,
      :provider_turn_ref,
      :turn_started_monotonic,
      :turn_start_task,
      :turn_start_timer,
      :session_started_monotonic,
      :created_at,
      :config_fingerprint,
      :store,
      status: :opening,
      durability: :durable,
      turns: %{},
      completed_turn_cache: %CompletedTurnCache{},
      requests: %{},
      subscriptions: %{},
      monitors: %{},
      deferred_provider_messages: :queue.new(),
      open_session_attrs: %{},
      seq: 0,
      event_buffer: nil
    ]
  end

  def child_spec(opts) do
    session = Keyword.fetch!(opts, :session)

    %{
      id: {__MODULE__, session.id},
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary,
      shutdown: Application.get_env(:agent_harness, :session_shutdown_timeout, 20_000),
      type: :worker
    }
  end

  def start_link(opts) do
    session = Keyword.fetch!(opts, :session)
    GenServer.start_link(__MODULE__, opts, name: via(session))
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    session = Keyword.fetch!(opts, :session)
    config = Keyword.fetch!(opts, :config)
    provider = Keyword.fetch!(opts, :provider)
    sink = Sink.new(self())
    created_at = DateTime.utc_now()

    starter = Keyword.fetch!(opts, :starter)

    state = %State{
      session: session,
      config: config,
      provider: provider,
      sink: sink,
      starter: starter,
      starter_monitor: starter |> elem(0) |> Process.monitor(),
      event_buffer: EventBuffer.new(config.event_buffer_size),
      store: config.store,
      durability: if(config.store == false, do: :disabled, else: :durable),
      created_at: created_at,
      config_fingerprint: config_fingerprint(config)
    }

    {:ok, state, {:continue, {:open, Keyword.get(opts, :reuse, :never)}}}
  end

  @impl true
  def handle_continue({:open, reuse}, state) do
    task =
      OwnedTask.async_nolink(AgentHarness.RunnerSupervisor, fn ->
        with {:ok, replacement} <- reusable_session(state.store, state.session.id, reuse),
             {:ok, provider_handle, info} <-
               open_provider(state.provider, state.config, state.sink) do
          {:ok, provider_handle, info, replacement}
        end
      end)

    timer =
      Process.send_after(
        self(),
        {:provider_open_timeout, task.ref},
        state.config.startup_timeout
      )

    {:noreply, %{state | open_task: task, open_timer: timer}}
  catch
    :exit, reason ->
      notify_starter(state, {:error, {:provider_open_task_failed, reason}})
      {:stop, {:provider_open_task_failed, reason}, state}
  end

  @impl true
  def handle_call(:status, _from, state) do
    pending_requests =
      state.requests
      |> Map.values()
      |> Enum.filter(&(&1.status == :pending))
      |> Enum.sort_by(& &1.created_at, DateTime)

    snapshot = %{
      session: state.session,
      status: state.status,
      durability: state.durability,
      provider_session_id: state.provider_session_id,
      current_turn: state.current_turn,
      pending_requests: pending_requests
    }

    {:reply, snapshot, state}
  end

  def handle_call(:capabilities, _from, %State{status: :unavailable} = state) do
    {:reply, {:error, :session_unavailable}, state}
  end

  def handle_call(:capabilities, _from, state) do
    {:reply, state.provider.capabilities(state.provider_handle), state}
  end

  def handle_call(
        {:start_turn, %Turn{} = turn, input, provider_opts, explicit_id?},
        _from,
        %State{status: :idle} = state
      ) do
    case turn_id_availability(state, turn.id, explicit_id?) do
      :available ->
        start_provider_turn(state, turn, input, provider_opts)

      :used ->
        {:reply, {:error, {:turn_id_already_used, turn.id}}, state}

      :history_unavailable ->
        {:reply, {:error, {:turn_id_history_unavailable, turn.id}}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call(
        {:start_turn, %Turn{}, _input, _opts, _explicit_id?},
        _from,
        %State{status: :unavailable} = state
      ) do
    {:reply, {:error, :session_unavailable}, state}
  end

  def handle_call({:start_turn, %Turn{}, _input, _opts, _explicit_id?}, _from, state) do
    {:reply, {:error, {:turn_in_progress, state.current_turn}}, state}
  end

  def handle_call({:subscribe, pid, turn_id, from}, _from, state) when is_pid(pid) do
    cond do
      not valid_replay_cursor?(from) ->
        {:reply, {:error, {:invalid_replay_cursor, from}}, state}

      is_binary(turn_id) ->
        subscribe_to_turn(state, pid, turn_id, from, false)

      true ->
        subscribe(state, pid, turn_id, from, false)
    end
  end

  def handle_call({:subscribe_stream, pid, turn_id, from}, _from, state) when is_pid(pid) do
    if valid_replay_cursor?(from) do
      subscribe_to_turn(state, pid, turn_id, from, true)
    else
      {:reply, {:error, {:invalid_replay_cursor, from}}, state}
    end
  end

  def handle_call({:await, pid, turn_id}, _from, state) when is_pid(pid) do
    case lookup_turn_history(state, turn_id) do
      {:terminal, terminal_event} ->
        {:reply, {:terminal, terminal_event}, state}

      {:active, _turn} ->
        subscribe(state, pid, turn_id, :start, false)

      :not_found ->
        {:reply, {:error, :turn_not_found}, state}

      :history_unavailable ->
        {:reply, {:error, :replay_unavailable}, state}

      :terminal_unavailable ->
        {:reply, {:error, :replay_unavailable}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:subscribe, pid, _turn_id, _cursor}, _from, state) do
    {:reply, {:error, {:invalid_subscriber, pid}}, state}
  end

  def handle_call({:unsubscribe, subscription_ref}, _from, state) do
    {:reply, :ok, remove_subscription(state, subscription_ref)}
  end

  def handle_call({:respond, request_id, response}, _from, state) do
    case lookup_request(state, request_id) do
      :not_found ->
        {:reply, {:error, :request_not_found}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}

      {_source, %Request{status: :resolved}} ->
        {:reply, {:error, :already_resolved}, state}

      {_source, %Request{status: :expired}} ->
        {:reply, {:error, :request_expired}, state}

      {:cold, %Request{status: :pending}} ->
        {:reply, {:error, :request_not_active}, state}

      {:hot, %Request{status: :pending}} when state.status == :cancelling ->
        {:reply, {:error, :turn_cancelling}, state}

      {:hot, %Request{status: :pending} = request} ->
        respond_to_request(state, request, response)
    end
  end

  def handle_call({:cancel, turn_id}, _from, %State{current_turn: %Turn{id: turn_id}} = state) do
    cancel_current_turn(state)
  end

  def handle_call({:cancel, _turn_id}, _from, state) do
    {:reply, {:error, :turn_not_active}, state}
  end

  def handle_call({:stop, false}, _from, %State{current_turn: nil} = state) do
    {:stop, :normal, :ok, close_state(state)}
  end

  def handle_call({:stop, false}, _from, state) do
    {:reply, {:error, :turn_in_progress}, state}
  end

  def handle_call({:stop, true}, _from, %State{current_turn: nil} = state) do
    {:stop, :normal, :ok, close_state(state)}
  end

  def handle_call({:stop, true}, _from, %State{turn_start_task: %Task{} = task} = state) do
    _ = Task.shutdown(task, :brutal_kill)
    cancel_timer(state.turn_start_timer)

    state = %{
      state
      | turn_start_task: nil,
        turn_start_timer: nil,
        deferred_provider_messages: :queue.new()
    }

    state = emit(state, state.current_turn.id, :cancel_requested, %{forced: true})
    state = expire_pending_requests(state, state.current_turn.id)
    state = complete_turn(state, state.current_turn.id, :cancelled, %{forced: true}, nil)
    {:stop, :normal, :ok, close_state(state)}
  end

  def handle_call({:stop, true}, _from, state) do
    case state.provider.cancel(state.provider_handle, state.provider_turn_ref) do
      :ok ->
        state = emit(state, state.current_turn.id, :cancel_requested)
        state = expire_pending_requests(state, state.current_turn.id)
        state = complete_turn(state, state.current_turn.id, :cancelled, %{forced: true}, nil)
        {:stop, :normal, :ok, close_state(state)}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_info(
        {task_ref, result},
        %State{open_task: %Task{ref: task_ref} = task} = state
      ) do
    Process.demonitor(task.ref, [:flush])
    cancel_timer(state.open_timer)
    state = %{state | open_task: nil, open_timer: nil}
    finish_provider_open_result(state, result)
  end

  def handle_info(
        {task_ref, result},
        %State{startup_finalization_task: %Task{ref: task_ref} = task} = state
      ) do
    Process.demonitor(task.ref, [:flush])
    cancel_timer(state.startup_finalization_timer)

    state = %{
      state
      | startup_finalization_task: nil,
        startup_finalization_timer: nil
    }

    finish_startup_finalization(state, result)
  end

  def handle_info(
        {task_ref, result},
        %State{startup_rollback_task: %Task{ref: task_ref} = task} = state
      ) do
    Process.demonitor(task.ref, [:flush])
    cancel_timer(state.startup_rollback_timer)

    state = %{
      state
      | startup_rollback_task: nil,
        startup_rollback_timer: nil
    }

    finish_startup_rollback(state, result)
  end

  def handle_info(
        {task_ref, result},
        %State{turn_start_task: %Task{ref: task_ref} = task} = state
      ) do
    Process.demonitor(task.ref, [:flush])
    cancel_timer(state.turn_start_timer)
    state = %{state | turn_start_task: nil, turn_start_timer: nil}

    case finish_turn_start(state, result) do
      {:stop, reason, state} -> {:stop, reason, state}
      state -> {:noreply, state}
    end
  end

  def handle_info(
        {:provider_open_timeout, task_ref},
        %State{open_task: %Task{ref: task_ref} = task} = state
      ) do
    state = %{state | open_task: nil, open_timer: nil}

    case Task.shutdown(task, :brutal_kill) do
      {:ok, result} -> finish_provider_open_result(state, result)
      _not_completed -> stop_failed_startup(state, :session_start_timeout)
    end
  end

  def handle_info({:provider_open_timeout, _stale_ref}, state), do: {:noreply, state}

  def handle_info(
        {:startup_finalization_timeout, task_ref},
        %State{startup_finalization_task: %Task{ref: task_ref} = task} = state
      ) do
    state = %{
      state
      | startup_finalization_task: nil,
        startup_finalization_timer: nil
    }

    case Task.shutdown(task, :brutal_kill) do
      {:ok, result} -> finish_startup_finalization(state, result)
      _not_completed -> fail_startup(state, :session_start_finalization_timeout)
    end
  end

  def handle_info({:startup_finalization_timeout, _stale_ref}, state), do: {:noreply, state}

  def handle_info(
        {:startup_rollback_timeout, task_ref},
        %State{startup_rollback_task: %Task{ref: task_ref} = task} = state
      ) do
    state = %{
      state
      | startup_rollback_task: nil,
        startup_rollback_timer: nil
    }

    case Task.shutdown(task, :brutal_kill) do
      {:ok, result} -> finish_startup_rollback(state, result)
      _not_completed -> finish_startup_rollback(state, {:error, :rollback_timeout})
    end
  end

  def handle_info({:startup_rollback_timeout, _stale_ref}, state), do: {:noreply, state}

  def handle_info(
        {:provider_turn_start_timeout, task_ref},
        %State{turn_start_task: %Task{ref: task_ref} = task} = state
      ) do
    _ = Task.shutdown(task, :brutal_kill)

    state = %{
      state
      | turn_start_task: nil,
        turn_start_timer: nil,
        deferred_provider_messages: :queue.new()
    }

    retire_uncertain_turn_start(
      state,
      :provider_turn_start_timeout,
      :provider_turn_start_timeout
    )
  end

  def handle_info({:provider_turn_start_timeout, _stale_ref}, state), do: {:noreply, state}

  def handle_info(
        {__MODULE__, starter_ref, :starter_ack},
        %State{starter: {_pid, starter_ref}} = state
      ) do
    {:noreply, clear_starter(state)}
  end

  def handle_info(
        {:agent_harness_provider, sink_ref, {:event, turn_id, type, data, raw}},
        %State{sink: %Sink{ref: sink_ref}} = state
      ) do
    if current_turn?(state, turn_id) and turn_starting?(state) do
      {:noreply, defer_provider_message(state, {:event, turn_id, type, data, raw})}
    else
      handle_provider_event(state, turn_id, type, data, raw)
    end
  end

  def handle_info(
        {:agent_harness_provider, sink_ref, {:request, turn_id, provider_ref, attrs, raw}},
        %State{sink: %Sink{ref: sink_ref}} = state
      ) do
    cond do
      current_turn?(state, turn_id) and turn_starting?(state) ->
        {:noreply, defer_provider_message(state, {:request, turn_id, provider_ref, attrs, raw})}

      current_turn?(state, turn_id) ->
        {:noreply, handle_provider_request(state, turn_id, provider_ref, attrs, raw)}

      true ->
        {:noreply, state}
    end
  end

  def handle_info(
        {:agent_harness_provider, sink_ref,
         {:expire_request, turn_id, provider_ref, reason, raw}},
        %State{sink: %Sink{ref: sink_ref}} = state
      ) do
    if current_turn?(state, turn_id) and turn_starting?(state) do
      {:noreply,
       defer_provider_message(
         state,
         {:expire_request, turn_id, provider_ref, reason, raw}
       )}
    else
      handle_provider_request_expiration(state, turn_id, provider_ref, reason, raw)
    end
  end

  def handle_info(
        {:agent_harness_provider, sink_ref, {:finish, turn_id, status, result, raw}},
        %State{sink: %Sink{ref: sink_ref}} = state
      ) do
    if current_turn?(state, turn_id) and turn_starting?(state) do
      {:noreply, defer_provider_message(state, {:finish, turn_id, status, result, raw})}
    else
      handle_provider_finish(state, turn_id, status, result, raw)
    end
  end

  def handle_info(
        {:agent_harness_provider, sink_ref, {:session_updated, attrs}},
        %State{sink: %Sink{ref: sink_ref}} = state
      ) do
    if startup_in_progress?(state) do
      {:noreply, %{state | open_session_attrs: Map.merge(state.open_session_attrs, attrs)}}
    else
      {:noreply, apply_session_update(state, attrs)}
    end
  end

  def handle_info(
        {:apply_startup_session_attrs, sink_ref, attrs},
        %State{sink: %Sink{ref: sink_ref}} = state
      ) do
    {:noreply, apply_session_update(state, attrs)}
  end

  def handle_info({:apply_startup_session_attrs, _stale_ref, _attrs}, state),
    do: {:noreply, state}

  def handle_info(
        {:agent_harness_provider, sink_ref, {:transport_down, reason}},
        %State{sink: %Sink{ref: sink_ref}} = state
      ) do
    cond do
      startup_in_progress?(state) ->
        {:noreply, record_open_failure(state, reason)}

      turn_starting?(state) ->
        {:noreply, defer_provider_message(state, {:transport_down, reason})}

      true ->
        {:noreply, transport_down(state, reason)}
    end
  end

  def handle_info({:agent_harness_provider, _stale_ref, _message}, state) do
    {:noreply, state}
  end

  def handle_info(
        {:DOWN, monitor, :process, _pid, reason},
        %State{open_task: %Task{ref: monitor}} = state
      ) do
    cancel_timer(state.open_timer)
    reason = {:provider_open_task_down, reason}
    notify_starter(state, {:error, reason})

    {:stop, :normal,
     state
     |> clear_starter()
     |> Map.merge(%{open_task: nil, open_timer: nil})}
  end

  def handle_info(
        {:DOWN, monitor, :process, _pid, reason},
        %State{startup_finalization_task: %Task{ref: monitor}} = state
      ) do
    cancel_timer(state.startup_finalization_timer)

    state = %{
      state
      | startup_finalization_task: nil,
        startup_finalization_timer: nil
    }

    fail_startup(state, {:session_start_finalization_task_down, reason})
  end

  def handle_info(
        {:DOWN, monitor, :process, _pid, reason},
        %State{startup_rollback_task: %Task{ref: monitor}} = state
      ) do
    cancel_timer(state.startup_rollback_timer)

    state = %{
      state
      | startup_rollback_task: nil,
        startup_rollback_timer: nil
    }

    finish_startup_rollback(state, {:error, {:rollback_task_down, reason}})
  end

  def handle_info(
        {:DOWN, monitor, :process, _pid, reason},
        %State{turn_start_task: %Task{ref: monitor}} = state
      ) do
    cancel_timer(state.turn_start_timer)

    state = %{
      state
      | turn_start_task: nil,
        turn_start_timer: nil,
        deferred_provider_messages: :queue.new()
    }

    failure = {:provider_turn_start_task_down, reason}
    retire_uncertain_turn_start(state, failure, {:provider_turn_start_uncertain, failure})
  end

  def handle_info(
        {:DOWN, monitor, :process, _pid, reason},
        %State{provider_monitor: monitor} = state
      ) do
    state = %{state | provider_monitor: nil}

    cond do
      startup_in_progress?(state) ->
        {:noreply, record_open_failure(state, reason)}

      turn_starting?(state) ->
        {:noreply, defer_provider_message(state, {:transport_down, reason})}

      true ->
        {:noreply, transport_down(state, reason)}
    end
  end

  def handle_info(
        {:DOWN, monitor, :process, _pid, _reason},
        %State{starter_monitor: monitor} = state
      ) do
    {:stop, :normal, clear_starter(state)}
  end

  def handle_info({:DOWN, monitor, :process, _pid, _reason}, state) do
    case Map.pop(state.monitors, monitor) do
      {nil, _monitors} ->
        {:noreply, state}

      {subscription_ref, monitors} ->
        {:noreply,
         %{
           state
           | monitors: monitors,
             subscriptions: Map.delete(state.subscriptions, subscription_ref)
         }}
    end
  end

  def handle_info(
        {:EXIT, pid, reason},
        %State{open_task: %Task{pid: pid}} = state
      )
      when reason != :normal do
    cancel_timer(state.open_timer)
    failure = {:provider_open_task_down, reason}
    notify_starter(state, {:error, failure})

    {:stop, :normal,
     state
     |> clear_starter()
     |> Map.merge(%{open_task: nil, open_timer: nil})}
  end

  def handle_info(
        {:EXIT, pid, reason},
        %State{turn_start_task: %Task{pid: pid}} = state
      )
      when reason != :normal do
    cancel_timer(state.turn_start_timer)

    state = %{
      state
      | turn_start_task: nil,
        turn_start_timer: nil,
        deferred_provider_messages: :queue.new()
    }

    failure = {:provider_turn_start_task_down, reason}
    retire_uncertain_turn_start(state, failure, {:provider_turn_start_uncertain, failure})
  end

  # Keep this catch-all last so linked provider implementations can use normal
  # OTP shutdown without crashing a trapping SessionServer.
  def handle_info({:EXIT, _pid, :normal}, state), do: {:noreply, state}

  def handle_info({:EXIT, _pid, :shutdown}, state), do: {:stop, :shutdown, state}

  def handle_info({:EXIT, _pid, reason}, state) do
    cond do
      startup_in_progress?(state) ->
        {:noreply, record_open_failure(state, reason)}

      turn_starting?(state) ->
        {:noreply, defer_provider_message(state, {:transport_down, reason})}

      true ->
        {:noreply, transport_down(state, reason)}
    end
  end

  @impl true
  def terminate(reason, state) do
    cancel_timer(state.open_timer)
    cancel_timer(state.startup_finalization_timer)
    cancel_timer(state.startup_rollback_timer)
    cancel_timer(state.turn_start_timer)
    shutdown_task(state.open_task)
    shutdown_task(state.startup_finalization_task)
    shutdown_task(state.startup_rollback_task)
    shutdown_task(state.turn_start_task)

    state = maybe_persist_shutdown(reason, state)

    if state.provider_handle do
      safe_close_provider(state.provider, state.provider_handle)
    end

    stop_session_telemetry(state, reason)
    :ok
  end

  defp handle_provider_request_expiration(state, turn_id, provider_ref, reason, raw) do
    if current_turn?(state, turn_id) do
      {:noreply, expire_provider_request(state, turn_id, provider_ref, reason, raw)}
    else
      {:noreply, state}
    end
  end

  defp finish_provider_open_result(state, result) do
    case accept_provider_open(state, result) do
      {:ok, state} -> {:noreply, state}
      {:error, reason, state} -> stop_failed_startup(state, reason)
    end
  end

  defp accept_provider_open(
         %State{open_failure: reason} = state,
         {:ok, provider_handle, _info, _replacement}
       )
       when not is_nil(reason) do
    safe_close_provider(
      state.provider,
      provider_handle,
      startup_cleanup_timeout(state)
    )

    {:error, {:provider_open_failed, {:transport_down, reason}}, state}
  end

  defp accept_provider_open(state, {:ok, provider_handle, info, replacement})
       when is_map(info) do
    provider_session_id =
      Map.get(
        state.open_session_attrs,
        :provider_session_id,
        Map.get(info, :provider_session_id)
      )

    replacement_marker = :atomics.new(1, [])

    state = %{
      state
      | status: :finalizing,
        provider_handle: provider_handle,
        provider_monitor: monitor_provider(state.provider, provider_handle, info),
        provider_session_id: provider_session_id,
        open_session_attrs: %{},
        startup_replacement: replacement,
        startup_replacement_marker: replacement_marker
    }

    try do
      task =
        OwnedTask.async_nolink(AgentHarness.RunnerSupervisor, fn ->
          finalize_startup_store(state, replacement, replacement_marker)
        end)

      timer =
        Process.send_after(
          self(),
          {:startup_finalization_timeout, task.ref},
          state.config.startup_finalization_timeout
        )

      notify_starter(state, {:phase, :finalizing})

      {:ok,
       %{
         state
         | startup_finalization_task: task,
           startup_finalization_timer: timer
       }}
    catch
      :exit, reason ->
        {:error, {:session_start_finalization_task_failed, reason}, state}
    end
  end

  defp accept_provider_open(%State{open_failure: failure} = state, {:error, _reason})
       when not is_nil(failure) do
    {:error, {:provider_open_failed, {:transport_down, failure}}, state}
  end

  defp accept_provider_open(state, {:error, reason}) do
    {:error, normalize_provider_open_error(reason), state}
  end

  defp accept_provider_open(state, other) do
    {:error, {:provider_open_failed, {:invalid_return, other}}, state}
  end

  defp finalize_startup_store(%State{store: false} = state, _replacement, _marker) do
    {:ok, startup_ready_event(state)}
  end

  defp finalize_startup_store(%State{store: {module, owner}} = state, replacement, marker) do
    with :ok <- delete_replaced_startup(state, module, owner, replacement, marker),
         :ok <-
           startup_store_write(state, :save_session, fn ->
             module.save_session(owner, state.session.id, session_snapshot(state))
           end),
         event = startup_ready_event(state),
         :ok <-
           startup_store_write(state, :append_event, fn ->
             module.append_event(owner, event)
           end),
         :ok <-
           startup_store_write(state, :save_session, fn ->
             idle_state = %{state | status: :idle}
             module.save_session(owner, state.session.id, session_snapshot(idle_state))
           end) do
      {:ok, event}
    end
  end

  defp delete_replaced_startup(_state, _module, _owner, :none, _marker), do: :ok

  defp delete_replaced_startup(state, module, owner, :replace, marker) do
    case startup_store_write(state, :delete_session, fn ->
           module.delete_session(owner, state.session.id)
         end) do
      :ok ->
        :atomics.put(marker, 1, 1)
        :ok

      error ->
        error
    end
  end

  defp startup_store_write(state, operation, write) do
    case telemetry_store_write(state, operation, write) do
      :ok -> :ok
      {:error, reason} -> {:error, operation, reason}
    end
  end

  defp startup_ready_event(state) do
    build_event(
      state,
      nil,
      :session_ready,
      %{provider_session_id: state.provider_session_id},
      nil
    )
  end

  defp finish_startup_finalization(
         %State{open_failure: failure} = state,
         {:ok, %Event{}}
       )
       when not is_nil(failure) do
    fail_startup(state, {:provider_open_failed, {:transport_down, failure}})
  end

  defp finish_startup_finalization(state, {:ok, %Event{} = event}) do
    state =
      state
      |> Map.put(:status, :idle)
      |> publish_event(event)
      |> start_session_telemetry()
      |> clear_startup_tracking()

    pending_attrs = state.open_session_attrs
    state = %{state | open_session_attrs: %{}}

    if map_size(pending_attrs) > 0 do
      send(self(), {:apply_startup_session_attrs, state.sink.ref, pending_attrs})
    end

    notify_starter(state, {:ok, self()})
    {:noreply, state}
  end

  defp finish_startup_finalization(state, {:error, operation, reason}) do
    public_reason = {:session_initialization_failed, {operation, reason}}
    fail_startup(state, public_reason, {operation, reason})
  end

  defp finish_startup_finalization(state, other) do
    fail_startup(
      state,
      {:session_initialization_failed, {:invalid_finalization_return, other}}
    )
  end

  defp fail_startup(state, public_reason, store_failure \\ nil) do
    state = %{
      state
      | status: :rolling_back,
        startup_failure: %{reason: public_reason, store_failure: store_failure}
    }

    if startup_rollback_required?(state) do
      start_startup_rollback(state)
    else
      finish_startup_failure(state)
    end
  end

  defp startup_rollback_required?(%State{store: false}), do: false
  defp startup_rollback_required?(%State{startup_replacement: :none}), do: true

  defp startup_rollback_required?(%State{startup_replacement: :replace} = state) do
    :atomics.get(state.startup_replacement_marker, 1) == 1
  end

  defp startup_rollback_required?(_state), do: false

  defp start_startup_rollback(%State{store: {module, owner}} = state) do
    try do
      task =
        OwnedTask.async_nolink(AgentHarness.RunnerSupervisor, fn ->
          telemetry_store_write(state, :delete_session, fn ->
            module.delete_session(owner, state.session.id)
          end)
        end)

      timer =
        Process.send_after(
          self(),
          {:startup_rollback_timeout, task.ref},
          state.config.startup_finalization_timeout
        )

      {:noreply,
       %{
         state
         | startup_rollback_task: task,
           startup_rollback_timer: timer
       }}
    catch
      :exit, reason -> finish_startup_rollback(state, {:error, {:task_failed, reason}})
    end
  end

  defp finish_startup_rollback(state, result) do
    if result != :ok do
      Logger.warning(
        "AgentHarness startup rollback failed " <>
          "session_id=#{state.session.id} provider=#{state.session.provider} " <>
          "reason=#{inspect(result)}"
      )
    end

    finish_startup_failure(state)
  end

  defp finish_startup_failure(%State{startup_failure: failure} = state) do
    if degradable_startup_failure?(state, failure) do
      complete_degraded_startup(state, failure.store_failure)
    else
      stop_failed_startup(state, failure.reason)
    end
  end

  defp degradable_startup_failure?(
         %State{config: %{store_failure: :degrade}, open_failure: nil},
         %{store_failure: {operation, _reason}}
       ) do
    operation in [:save_session, :append_event]
  end

  defp degradable_startup_failure?(_state, _failure), do: false

  defp complete_degraded_startup(state, {operation, reason}) do
    pending_attrs = state.open_session_attrs
    state = update_provider_session_id(state, pending_attrs)
    state = handle_store_failure(state, operation, reason)
    state = %{state | status: :idle, open_session_attrs: %{}}

    state =
      state
      |> publish_event(startup_ready_event(state))
      |> start_session_telemetry()
      |> clear_startup_tracking()

    state =
      if map_size(pending_attrs) > 0 do
        emit(state, nil, :session_updated, pending_attrs)
      else
        state
      end

    notify_starter(state, {:ok, self()})
    {:noreply, state}
  end

  defp stop_failed_startup(state, reason) do
    state = close_failed_startup_provider(state)
    notify_starter(state, {:error, reason})
    {:stop, :normal, state |> clear_startup_tracking() |> clear_starter()}
  end

  defp close_failed_startup_provider(%State{provider_handle: nil} = state), do: state

  defp close_failed_startup_provider(state) do
    safe_close_provider(
      state.provider,
      state.provider_handle,
      startup_cleanup_timeout(state)
    )

    clear_provider(state)
  end

  defp startup_cleanup_timeout(state) do
    configured = Application.get_env(:agent_harness, :provider_close_timeout, 5_000)
    min(configured, state.config.startup_finalization_timeout)
  end

  defp start_session_telemetry(state) do
    started_at =
      Telemetry.start([:session], %{
        session_id: state.session.id,
        provider: state.session.provider
      })

    %{state | session_started_monotonic: started_at}
  end

  defp clear_startup_tracking(state) do
    %{
      state
      | startup_replacement: nil,
        startup_replacement_marker: nil,
        startup_failure: nil
    }
  end

  defp open_provider(provider, config, sink) do
    case provider.open_session(config, sink) do
      {:ok, provider_handle, info} when is_map(info) ->
        {:ok, provider_handle, info}

      {:error, reason} ->
        {:error, {:provider_open_failed, reason}}

      other ->
        {:error, {:provider_open_failed, {:invalid_return, other}}}
    end
  rescue
    error ->
      {:error, {:provider_open_failed, {:exception, error.__struct__, Exception.message(error)}}}
  catch
    kind, reason -> {:error, {:provider_open_failed, {kind, reason}}}
  end

  defp normalize_provider_open_error({:provider_open_failed, _reason} = error), do: error
  defp normalize_provider_open_error(reason), do: reason

  defp reusable_session(false, _session_id, _reuse), do: {:ok, :none}

  defp reusable_session({module, owner}, session_id, reuse) do
    case module.fetch_session(owner, session_id) do
      :not_found ->
        {:ok, :none}

      {:ok, %{status: :closed}} when reuse in [:closed, :replace] ->
        {:ok, :replace}

      # A previous owner died after staging startup but before readiness. It is
      # not a reusable logical session and must not burn the caller's ID.
      {:ok, %{status: :finalizing}} ->
        {:ok, :replace}

      {:ok, _snapshot} when reuse == :replace ->
        {:ok, :replace}

      {:ok, _snapshot} ->
        {:error, :session_id_already_used}

      {:error, reason} ->
        {:error, {:store_read_failed, reason}}

      other ->
        {:error, {:invalid_store_response, other}}
    end
  rescue
    error ->
      {:error, {:store_read_failed, {:exception, error.__struct__, Exception.message(error)}}}
  catch
    kind, reason ->
      {:error, {:store_read_failed, {kind, reason}}}
  end

  defp safe_close_provider(provider, provider_handle) do
    timeout = Application.get_env(:agent_harness, :provider_close_timeout, 5_000)
    safe_close_provider(provider, provider_handle, timeout)
  end

  defp safe_close_provider(provider, provider_handle, timeout) do
    task =
      Task.Supervisor.async(AgentHarness.RunnerSupervisor, fn ->
        provider.close_session(provider_handle)
      end)

    case Task.yield(task, timeout) do
      {:ok, _result} ->
        :ok

      {:exit, _reason} ->
        :ok

      nil ->
        _ = Task.shutdown(task, :brutal_kill)
        if is_pid(provider_handle), do: Process.exit(provider_handle, :kill)
        :ok
    end
  rescue
    _error -> :ok
  catch
    _kind, _reason -> :ok
  end

  defp clear_provider(state) do
    if state.provider_monitor, do: Process.demonitor(state.provider_monitor, [:flush])
    %{state | provider_handle: nil, provider_monitor: nil}
  end

  defp subscribe_to_turn(state, pid, turn_id, from, stream?) do
    case lookup_turn_history(state, turn_id) do
      {:terminal, _event} when stream? and from == :latest ->
        {:reply, {:error, :replay_unavailable}, state}

      {:terminal, _event} ->
        subscribe(state, pid, turn_id, from, true)

      {:active, _turn} ->
        subscribe(state, pid, turn_id, from, false)

      :not_found ->
        {:reply, {:error, :turn_not_found}, state}

      unavailable when unavailable in [:history_unavailable, :terminal_unavailable] ->
        {:reply, {:error, :replay_unavailable}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  defp subscribe(state, pid, turn_id, from, completed?) do
    events = replay_events(state, turn_id, from)

    if replay_unavailable?(turn_id, from, events, completed?) do
      {:reply, {:error, :replay_unavailable}, state}
    else
      install_subscription(state, pid, turn_id, events)
    end
  end

  defp install_subscription(state, pid, turn_id, events) do
    subscription = %Subscription{
      ref: make_ref(),
      session_id: state.session.id,
      turn_id: turn_id,
      pid: pid,
      server: self()
    }

    monitor = Process.monitor(pid)
    subscriber = %{pid: pid, monitor: monitor, turn_id: turn_id}

    state = %{
      state
      | subscriptions: Map.put(state.subscriptions, subscription.ref, subscriber),
        monitors: Map.put(state.monitors, monitor, subscription.ref)
    }

    Enum.each(events, &deliver(subscription.ref, pid, &1))

    {:reply, {:ok, subscription}, state}
  end

  defp replay_unavailable?(nil, _from, _events, _completed?), do: false
  defp replay_unavailable?(_turn_id, :latest, _events, _completed?), do: false

  defp replay_unavailable?(_turn_id, _from, events, completed?) do
    completed? and not Enum.any?(events, &terminal_event?/1)
  end

  defp lookup_turn_history(state, turn_id) do
    case local_terminal_event(state, turn_id) do
      %Event{} = event ->
        {:terminal, event}

      nil ->
        case Map.fetch(state.turns, turn_id) do
          {:ok, %Turn{} = turn} -> lookup_hot_turn_history(state, turn)
          :error -> lookup_cold_turn_history(state, turn_id)
        end
    end
  end

  defp lookup_hot_turn_history(state, %Turn{} = turn) do
    if terminal_turn?(turn) do
      case stored_terminal_event(state, turn.id) do
        {:ok, event} -> {:terminal, event}
        _unavailable -> :terminal_unavailable
      end
    else
      {:active, turn}
    end
  end

  defp lookup_cold_turn_history(
         %State{store: false, completed_turn_cache: %CompletedTurnCache{pruned?: true}},
         _turn_id
       ),
       do: :history_unavailable

  defp lookup_cold_turn_history(%State{store: false}, _turn_id), do: :not_found

  defp lookup_cold_turn_history(%State{store: {module, owner}} = state, turn_id) do
    case safe_store_fetch_turn(module, owner, state.session.id, turn_id) do
      {:ok, %Turn{} = turn} ->
        lookup_stored_turn_history(state, turn)

      :not_found ->
        :not_found

      {:error, reason} ->
        {:error, {:store_read_failed, reason}}
    end
  end

  defp lookup_stored_turn_history(state, %Turn{} = turn) do
    if terminal_turn?(turn) do
      case stored_terminal_event(state, turn.id) do
        {:ok, event} -> {:terminal, event}
        _unavailable -> :terminal_unavailable
      end
    else
      :terminal_unavailable
    end
  end

  defp local_terminal_event(state, turn_id) do
    Map.get(state.completed_turn_cache.events, turn_id) ||
      Enum.find(EventBuffer.to_list(state.event_buffer), fn event ->
        event.turn_id == turn_id and terminal_event?(event)
      end)
  end

  defp stored_terminal_event(%State{store: false}, _turn_id), do: :unavailable

  defp stored_terminal_event(%State{store: {module, owner}} = state, turn_id) do
    options = replay_options(turn_id, [])

    case safe_store_events(module, owner, state.session.id, options) do
      {:ok, events} ->
        case Enum.find(events, &terminal_event?/1) do
          %Event{} = event -> {:ok, event}
          nil -> :unavailable
        end

      {:error, _reason} ->
        :unavailable
    end
  end

  defp turn_id_availability(state, turn_id, explicit_id?) do
    cond do
      Map.has_key?(state.turns, turn_id) ->
        :used

      local_terminal_event(state, turn_id) ->
        :used

      state.store == false and explicit_id? and state.completed_turn_cache.pruned? ->
        :history_unavailable

      state.store == false ->
        :available

      true ->
        stored_turn_id_availability(state, turn_id)
    end
  end

  defp stored_turn_id_availability(%State{store: {module, owner}} = state, turn_id) do
    case safe_store_fetch_turn(module, owner, state.session.id, turn_id) do
      {:ok, %Turn{}} -> :used
      :not_found -> :available
      {:error, reason} -> {:error, {:store_read_failed, reason}}
    end
  end

  defp lookup_request(state, request_id) do
    case Map.fetch(state.requests, request_id) do
      {:ok, %Request{} = request} ->
        {:hot, request}

      :error ->
        lookup_cold_request(state, request_id)
    end
  end

  defp lookup_cold_request(%State{store: false}, _request_id), do: :not_found

  defp lookup_cold_request(%State{store: {module, owner}} = state, request_id) do
    case safe_store_fetch_request(module, owner, state.session.id, request_id) do
      {:ok, %Request{} = request} -> {:cold, request}
      :not_found -> :not_found
      {:error, reason} -> {:error, {:store_read_failed, reason}}
    end
  end

  defp safe_store_fetch_turn(module, owner, session_id, turn_id) do
    case module.fetch_turn(owner, session_id, turn_id) do
      {:ok, %Turn{} = turn} -> {:ok, turn}
      :not_found -> :not_found
      {:error, reason} -> {:error, reason}
      other -> {:error, {:invalid_return, other}}
    end
  rescue
    error -> {:error, {:exception, error.__struct__, Exception.message(error)}}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp safe_store_fetch_request(module, owner, session_id, request_id) do
    case module.fetch_request(owner, session_id, request_id) do
      {:ok, %Request{} = request} -> {:ok, request}
      :not_found -> :not_found
      {:error, reason} -> {:error, reason}
      other -> {:error, {:invalid_return, other}}
    end
  rescue
    error -> {:error, {:exception, error.__struct__, Exception.message(error)}}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp start_provider_turn(state, turn, input, provider_opts) do
    turn = %{turn | status: :starting}

    state = %{
      state
      | status: :starting,
        current_turn: turn,
        turns: Map.put(state.turns, turn.id, turn)
    }

    state = persist_turn(state, turn)
    state = persist_session(state)

    turn_started_monotonic =
      Telemetry.start([:turn], %{
        session_id: state.session.id,
        provider: state.session.provider,
        turn_id: turn.id
      })

    state = %{state | turn_started_monotonic: turn_started_monotonic}

    case start_turn_task(state, turn, input, provider_opts) do
      {:ok, task} ->
        timer =
          Process.send_after(
            self(),
            {:provider_turn_start_timeout, task.ref},
            state.config.turn_start_timeout
          )

        state = %{state | turn_start_task: task, turn_start_timer: timer}
        {:reply, {:ok, turn}, state}

      {:error, reason} ->
        state = fail_turn_start(state, {:provider_turn_task_failed, reason})
        {:reply, {:error, {:provider_turn_task_failed, reason}}, state}
    end
  end

  defp start_turn_task(state, turn, input, provider_opts) do
    {:ok,
     Task.Supervisor.async(AgentHarness.RunnerSupervisor, fn ->
       state.provider.start_turn(state.provider_handle, turn, input, provider_opts)
     end)}
  catch
    :exit, reason -> {:error, reason}
  end

  defp finish_turn_start(state, {:ok, provider_turn_ref}) do
    now = DateTime.utc_now()
    cancelling? = state.status == :cancelling
    status = if cancelling?, do: :cancelling, else: :running
    turn = %{state.current_turn | status: status, started_at: now}

    state = %{
      state
      | status: status,
        current_turn: turn,
        provider_turn_ref: provider_turn_ref,
        turns: Map.put(state.turns, turn.id, turn)
    }

    state = persist_turn(state, turn)
    state = persist_session(state)

    state =
      emit(state, turn.id, :turn_started, %{
        turn: turn,
        provider_turn_ref: provider_turn_ref
      })

    state = if cancelling?, do: request_provider_cancel(state), else: state
    drain_deferred_provider_messages(state)
  end

  defp finish_turn_start(state, {:error, {:turn_start_uncertain, reason}}) do
    retire_uncertain_turn_start(
      state,
      reason,
      {:provider_turn_start_uncertain, reason}
    )
  end

  defp finish_turn_start(state, {:error, reason}) do
    state |> fail_turn_start(reason) |> drain_deferred_provider_messages()
  end

  defp finish_turn_start(state, other) do
    state
    |> fail_turn_start({:invalid_provider_start_return, other})
    |> drain_deferred_provider_messages()
  end

  defp fail_turn_start(%State{current_turn: nil} = state, _reason), do: state

  defp fail_turn_start(state, reason) do
    turn_id = state.current_turn.id
    complete_turn(state, turn_id, :failed, %{reason: reason}, nil)
  end

  defp retire_uncertain_turn_start(state, failure, stop_reason) do
    state = fail_turn_start(state, failure)
    state = state |> Map.put(:status, :unavailable) |> persist_session()
    safe_close_provider(state.provider, state.provider_handle)
    {:stop, stop_reason, clear_provider(state)}
  end

  defp turn_starting?(%State{turn_start_task: %Task{}}), do: true
  defp turn_starting?(_state), do: false

  defp defer_provider_message(state, message) do
    %{
      state
      | deferred_provider_messages: :queue.in(message, state.deferred_provider_messages)
    }
  end

  defp drain_deferred_provider_messages(state) do
    messages = :queue.to_list(state.deferred_provider_messages)
    state = %{state | deferred_provider_messages: :queue.new()}

    Enum.reduce(messages, state, &apply_deferred_provider_message(&2, &1))
  end

  defp apply_deferred_provider_message(state, {:event, turn_id, type, data, raw}) do
    if current_turn?(state, turn_id), do: emit(state, turn_id, type, data, raw), else: state
  end

  defp apply_deferred_provider_message(
         state,
         {:request, turn_id, provider_ref, attrs, raw}
       ) do
    handle_provider_request(state, turn_id, provider_ref, attrs, raw)
  end

  defp apply_deferred_provider_message(
         state,
         {:expire_request, turn_id, provider_ref, reason, raw}
       ) do
    if current_turn?(state, turn_id) do
      expire_provider_request(state, turn_id, provider_ref, reason, raw)
    else
      state
    end
  end

  defp apply_deferred_provider_message(state, {:finish, turn_id, status, result, raw}) do
    if current_turn?(state, turn_id) do
      state
      |> expire_pending_requests(turn_id)
      |> complete_turn(turn_id, status, result, raw)
    else
      state
    end
  end

  defp apply_deferred_provider_message(state, {:transport_down, reason}) do
    transport_down(state, reason)
  end

  defp handle_provider_event(state, turn_id, type, data, raw) do
    if current_turn?(state, turn_id) do
      {:noreply, emit(state, turn_id, type, data, raw)}
    else
      {:noreply, state}
    end
  end

  defp handle_provider_finish(state, turn_id, status, result, raw) do
    if current_turn?(state, turn_id) do
      state = expire_pending_requests(state, turn_id)
      {:noreply, complete_turn(state, turn_id, status, result, raw)}
    else
      {:noreply, state}
    end
  end

  defp handle_provider_request(state, turn_id, provider_ref, attrs, raw) do
    if current_turn?(state, turn_id) and state.status != :cancelling do
      request_opts =
        attrs
        |> Keyword.delete(:id)
        |> Keyword.put(:session_id, state.session.id)
        |> Keyword.put(:turn_id, turn_id)
        |> Keyword.put(:provider_ref, provider_ref)

      request = Request.new(request_opts)
      turn = %{state.current_turn | status: :awaiting_input}

      state = %{
        state
        | status: :awaiting_input,
          current_turn: turn,
          turns: Map.put(state.turns, turn.id, turn),
          requests: Map.put(state.requests, request.id, request)
      }

      state = persist_turn(state, turn)
      state = persist_request(state, request)
      state = persist_session(state)
      request_telemetry(:created, state, request)
      emit(state, turn_id, :request_created, request, raw)
    else
      state
    end
  end

  defp via(session) do
    {:via, Registry, {AgentHarness.SessionRegistry, session.id, session}}
  end

  defp respond_to_request(state, request, %Response{} = response) do
    case state.provider.respond(state.provider_handle, request.provider_ref, response) do
      :ok ->
        request = %{request | status: :resolved, response: response}
        requests = Map.put(state.requests, request.id, request)

        pending? =
          Enum.any?(requests, fn
            {_id, %Request{turn_id: turn_id, status: :pending}} ->
              turn_id == state.current_turn.id

            _entry ->
              false
          end)

        status = if pending?, do: :awaiting_input, else: :running
        turn = %{state.current_turn | status: status}

        state = %{
          state
          | requests: requests,
            status: status,
            current_turn: turn,
            turns: Map.put(state.turns, turn.id, turn)
        }

        state = persist_turn(state, turn)
        state = persist_request(state, request)
        state = persist_session(state)

        data = %{request: request, response: response}
        request_telemetry(:resolved, state, request)
        {:reply, :ok, emit(state, request.turn_id, :request_resolved, data)}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  defp cancel_current_turn(%State{status: :cancelling} = state) do
    {:reply, :ok, state}
  end

  defp cancel_current_turn(%State{status: :starting} = state) do
    turn = %{state.current_turn | status: :cancelling}

    state = %{
      state
      | status: :cancelling,
        current_turn: turn,
        turns: Map.put(state.turns, turn.id, turn)
    }

    state = persist_turn(state, turn)
    state = persist_session(state)

    state = emit(state, turn.id, :cancel_requested)
    {:reply, :ok, expire_pending_requests(state, turn.id)}
  end

  defp cancel_current_turn(state) do
    case state.provider.cancel(state.provider_handle, state.provider_turn_ref) do
      :ok ->
        turn = %{state.current_turn | status: :cancelling}

        state = %{
          state
          | status: :cancelling,
            current_turn: turn,
            turns: Map.put(state.turns, turn.id, turn)
        }

        state = persist_turn(state, turn)
        state = persist_session(state)

        state = emit(state, state.current_turn.id, :cancel_requested)
        {:reply, :ok, expire_pending_requests(state, state.current_turn.id)}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  defp request_provider_cancel(state) do
    case state.provider.cancel(state.provider_handle, state.provider_turn_ref) do
      :ok ->
        state

      {:error, reason} ->
        transport_down(state, {:cancel_failed, reason})

      other ->
        transport_down(state, {:cancel_failed, {:invalid_return, other}})
    end
  end

  defp complete_turn(state, turn_id, status, result, raw) do
    turn_started_monotonic = state.turn_started_monotonic

    event_type =
      case status do
        :completed -> :turn_completed
        :failed -> :turn_failed
        :cancelled -> :turn_cancelled
        :interrupted -> :turn_interrupted
      end

    turn = %{
      state.current_turn
      | status: status,
        result: result,
        finished_at: DateTime.utc_now()
    }

    state = %{
      state
      | status: :idle,
        current_turn: nil,
        provider_turn_ref: nil,
        turn_started_monotonic: nil,
        turns: Map.put(state.turns, turn_id, turn)
    }

    state = persist_turn(state, turn)
    state = persist_session(state)

    Telemetry.stop(
      [:turn],
      turn_started_monotonic || System.monotonic_time(),
      %{
        session_id: state.session.id,
        provider: state.session.provider,
        turn_id: turn.id,
        status: turn.status
      }
    )

    emit(state, turn_id, event_type, %{status: status, result: result}, raw)
  end

  defp expire_pending_requests(state, turn_id) do
    state.requests
    |> Map.values()
    |> Enum.filter(&(&1.turn_id == turn_id and &1.status == :pending))
    |> Enum.reduce(state, fn request, acc ->
      request = %{request | status: :expired}
      acc = %{acc | requests: Map.put(acc.requests, request.id, request)}
      acc = persist_request(acc, request)
      request_telemetry(:expired, acc, request)
      emit(acc, turn_id, :request_expired, request)
    end)
  end

  defp expire_provider_request(state, turn_id, provider_ref, reason, raw) do
    request =
      Enum.find_value(state.requests, fn
        {_id,
         %Request{
           turn_id: ^turn_id,
           provider_ref: ^provider_ref,
           status: :pending
         } = request} ->
          request

        _entry ->
          nil
      end)

    case request do
      nil ->
        state

      request ->
        request = %{
          request
          | status: :expired,
            metadata: Map.put(request.metadata, :expiration_reason, reason)
        }

        requests = Map.put(state.requests, request.id, request)
        pending? = pending_requests_for_turn?(requests, turn_id)
        status = status_after_request(state.status, pending?)
        turn = %{state.current_turn | status: status}

        state = %{
          state
          | requests: requests,
            status: status,
            current_turn: turn,
            turns: Map.put(state.turns, turn.id, turn)
        }

        state = persist_turn(state, turn)
        state = persist_request(state, request)
        state = persist_session(state)
        request_telemetry(:expired, state, request)
        emit(state, turn_id, :request_expired, request, raw)
    end
  end

  defp pending_requests_for_turn?(requests, turn_id) do
    Enum.any?(requests, fn
      {_id, %Request{turn_id: ^turn_id, status: :pending}} -> true
      _entry -> false
    end)
  end

  defp status_after_request(:cancelling, _pending?), do: :cancelling
  defp status_after_request(_status, true), do: :awaiting_input
  defp status_after_request(_status, false), do: :running

  defp emit(state, turn_id, type, data \\ %{}, raw \\ nil) do
    event = build_event(state, turn_id, type, data, raw)

    {state, event} =
      case persist_event(state, event) do
        :ok ->
          {state, event}

        {:error, reason} ->
          state = handle_store_failure(state, :append_event, reason)
          {state, build_event(state, turn_id, type, data, raw)}
      end

    publish_event(state, event)
  end

  defp build_event(state, turn_id, type, data, raw) do
    Event.new(
      seq: state.seq + 1,
      session_id: state.session.id,
      turn_id: turn_id,
      provider: state.session.provider,
      type: type,
      data: data,
      raw: raw
    )
  end

  defp publish_event(state, event) do
    Enum.each(state.subscriptions, fn {subscription_ref, subscriber} ->
      if matches_turn?(event, subscriber.turn_id) do
        deliver(subscription_ref, subscriber.pid, event)
      end
    end)

    state = %{
      state
      | seq: event.seq,
        event_buffer: EventBuffer.push(state.event_buffer, event)
    }

    if terminal_event?(event) do
      cache_completed_turn(state, event)
    else
      state
    end
  end

  defp cache_completed_turn(state, %Event{} = event) do
    cache = state.completed_turn_cache
    already_cached? = Map.has_key?(cache.events, event.turn_id)
    cache = %{cache | events: Map.put(cache.events, event.turn_id, event)}
    state = %{state | completed_turn_cache: cache}

    if already_cached? do
      state
    else
      cache = %{
        cache
        | order: :queue.in(event.turn_id, cache.order),
          count: cache.count + 1
      }

      prune_completed_turns(%{state | completed_turn_cache: cache})
    end
  end

  defp prune_completed_turns(%State{config: %{completed_turn_cache_size: :infinity}} = state),
    do: state

  defp prune_completed_turns(state)
       when state.completed_turn_cache.count <= state.config.completed_turn_cache_size,
       do: state

  defp prune_completed_turns(state) do
    cache = state.completed_turn_cache
    {{:value, turn_id}, order} = :queue.out(cache.order)

    requests =
      Map.reject(state.requests, fn {_request_id, request} -> request.turn_id == turn_id end)

    cache = %{
      cache
      | events: Map.delete(cache.events, turn_id),
        order: order,
        count: cache.count - 1,
        pruned?: true
    }

    state = %{
      state
      | turns: Map.delete(state.turns, turn_id),
        requests: requests,
        completed_turn_cache: cache
    }

    prune_completed_turns(state)
  end

  defp terminal_event?(%Event{type: type}) do
    type in [:turn_completed, :turn_failed, :turn_cancelled, :turn_interrupted]
  end

  defp terminal_turn?(%Turn{status: status}) do
    status in [:completed, :failed, :cancelled, :interrupted]
  end

  defp replay_events(_state, _turn_id, :latest), do: []

  defp replay_events(%State{store: false, event_buffer: buffer}, turn_id, :start) do
    buffer |> EventBuffer.from(:start) |> Enum.filter(&matches_turn?(&1, turn_id))
  end

  defp replay_events(%State{store: false, event_buffer: buffer}, turn_id, {:after, seq})
       when is_integer(seq) do
    buffer |> EventBuffer.from(seq + 1) |> Enum.filter(&matches_turn?(&1, turn_id))
  end

  defp replay_events(%State{store: {module, owner}} = state, turn_id, :start) do
    case safe_store_events(module, owner, state.session.id, replay_options(turn_id, [])) do
      {:ok, events} ->
        events

      {:error, _reason} ->
        state.event_buffer
        |> EventBuffer.from(:start)
        |> Enum.filter(&matches_turn?(&1, turn_id))
    end
  end

  defp replay_events(%State{store: {module, owner}} = state, turn_id, {:after, seq})
       when is_integer(seq) do
    options = replay_options(turn_id, after: seq)

    case safe_store_events(module, owner, state.session.id, options) do
      {:ok, events} ->
        events

      {:error, _reason} ->
        state.event_buffer
        |> EventBuffer.from(seq + 1)
        |> Enum.filter(&matches_turn?(&1, turn_id))
    end
  end

  defp replay_options(nil, options), do: options
  defp replay_options(turn_id, options), do: Keyword.put(options, :turn_id, turn_id)

  defp safe_store_events(module, owner, session_id, options) do
    module.events(owner, session_id, options)
  rescue
    error -> {:error, {:exception, error.__struct__, Exception.message(error)}}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp matches_turn?(_event, nil), do: true
  defp matches_turn?(%Event{turn_id: turn_id}, turn_id), do: true
  defp matches_turn?(_event, _turn_id), do: false

  defp valid_replay_cursor?(:latest), do: true
  defp valid_replay_cursor?(:start), do: true

  defp valid_replay_cursor?({:after, sequence})
       when is_integer(sequence) and sequence >= 0,
       do: true

  defp valid_replay_cursor?(_cursor), do: false

  defp deliver(subscription_ref, pid, event) do
    send(pid, {:agent_harness, subscription_ref, event})
  end

  defp remove_subscription(state, subscription_ref) do
    case Map.pop(state.subscriptions, subscription_ref) do
      {nil, _subscriptions} ->
        state

      {%{monitor: monitor}, subscriptions} ->
        Process.demonitor(monitor, [:flush])

        %{
          state
          | subscriptions: subscriptions,
            monitors: Map.delete(state.monitors, monitor)
        }
    end
  end

  defp current_turn?(%State{current_turn: %Turn{id: turn_id}}, turn_id), do: true
  defp current_turn?(_state, _turn_id), do: false

  defp current_turn_id(%State{current_turn: %Turn{id: turn_id}}), do: turn_id
  defp current_turn_id(_state), do: nil

  defp monitor_provider(_provider, provider_handle, info) do
    case Map.get(info, :monitor, provider_handle) do
      pid when is_pid(pid) and pid != self() -> Process.monitor(pid)
      _other -> nil
    end
  end

  defp startup_in_progress?(%State{status: status}) do
    status in [:opening, :finalizing, :rolling_back]
  end

  defp apply_session_update(state, attrs) do
    state
    |> update_provider_session_id(attrs)
    |> persist_session()
    |> emit(nil, :session_updated, attrs)
  end

  defp update_provider_session_id(state, attrs) do
    case Map.fetch(attrs, :provider_session_id) do
      {:ok, provider_session_id} -> %{state | provider_session_id: provider_session_id}
      :error -> state
    end
  end

  defp transport_down(%State{status: :unavailable} = state, _reason), do: state

  defp transport_down(state, reason) do
    state = emit(state, current_turn_id(state), :transport_error, %{reason: reason})

    state =
      case state.current_turn do
        nil ->
          state

        turn ->
          state
          |> expire_pending_requests(turn.id)
          |> complete_turn(turn.id, :failed, %{reason: reason}, nil)
      end

    state = %{state | status: :unavailable}
    persist_session(state)
  end

  defp close_state(%State{current_turn: %Turn{id: turn_id}} = state) do
    state = expire_pending_requests(state, turn_id)
    state = complete_turn(state, turn_id, :interrupted, %{reason: :session_shutdown}, nil)
    close_state(state)
  end

  defp close_state(state) do
    state = %{state | status: :closed}
    state = persist_session(state)
    emit(state, nil, :session_closed)
  end

  defp persist_session(%State{store: false} = state), do: state

  defp persist_session(%State{store: {module, owner}} = state) do
    persist_store_write(state, :save_session, fn ->
      module.save_session(owner, state.session.id, session_snapshot(state))
    end)
  end

  defp persist_turn(%State{store: false} = state, _turn), do: state

  defp persist_turn(%State{store: {module, owner}} = state, turn) do
    persist_store_write(state, :save_turn, fn -> module.save_turn(owner, turn) end)
  end

  defp persist_request(%State{store: false} = state, _request), do: state

  defp persist_request(%State{store: {module, owner}} = state, request) do
    persist_store_write(state, :save_request, fn -> module.save_request(owner, request) end)
  end

  defp persist_event(%State{store: false}, _event), do: :ok

  defp persist_event(%State{store: {module, owner}} = state, event) do
    telemetry_store_write(state, :append_event, fn -> module.append_event(owner, event) end)
  end

  defp persist_store_write(state, operation, write) do
    case telemetry_store_write(state, operation, write) do
      :ok -> state
      {:error, reason} -> handle_store_failure(state, operation, reason)
    end
  end

  defp telemetry_store_write(state, operation, write) do
    Telemetry.span(
      [:store, :write],
      %{
        session_id: state.session.id,
        provider: state.session.provider,
        operation: operation
      },
      fn -> safe_store_write(write) end
    )
  end

  defp safe_store_write(write) do
    case write.() do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
      other -> {:error, {:invalid_return, other}}
    end
  rescue
    error -> {:error, {:exception, error.__struct__, Exception.message(error)}}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp handle_store_failure(state, operation, reason) do
    failure = %{operation: operation, reason: reason, at: DateTime.utc_now()}

    Logger.error(
      "AgentHarness Store write failed " <>
        "session_id=#{state.session.id} provider=#{state.session.provider} " <>
        "operation=#{operation} reason=#{inspect(reason)}"
    )

    case state.config.store_failure do
      :stop ->
        exit({:store_write_failed, operation, reason})

      :degrade ->
        state = %{state | store: false, durability: {:degraded, failure}}
        event = build_event(state, current_turn_id(state), :store_failed, failure, nil)
        publish_event(state, event)
    end
  end

  defp request_telemetry(action, state, request) do
    Telemetry.event(
      [:request, action],
      %{},
      %{
        session_id: state.session.id,
        provider: state.session.provider,
        turn_id: request.turn_id,
        request_id: request.id,
        kind: request.kind
      }
    )
  end

  defp session_snapshot(state) do
    %{
      id: state.session.id,
      provider: state.session.provider,
      status: state.status,
      provider_session_id: state.provider_session_id,
      current_turn_id: current_turn_id(state),
      cwd: state.config.cwd,
      model: state.config.model,
      metadata: state.config.metadata,
      config_fingerprint: state.config_fingerprint,
      created_at: state.created_at,
      updated_at: DateTime.utc_now()
    }
  end

  defp notify_starter(%State{starter: {pid, ref}}, result) when is_pid(pid) do
    send(pid, {__MODULE__, ref, result})
    :ok
  end

  defp notify_starter(_state, _result), do: :ok

  defp clear_starter(%State{starter_monitor: nil} = state), do: %{state | starter: nil}

  defp clear_starter(%State{starter_monitor: monitor} = state) do
    Process.demonitor(monitor, [:flush])
    %{state | starter: nil, starter_monitor: nil}
  end

  defp record_open_failure(%State{open_failure: nil} = state, reason) do
    %{state | open_failure: reason}
  end

  defp record_open_failure(state, _reason), do: state

  defp cancel_timer(nil), do: :ok
  defp cancel_timer(timer), do: Process.cancel_timer(timer, async: true, info: false)

  defp shutdown_task(nil), do: :ok
  defp shutdown_task(%Task{} = task), do: Task.shutdown(task, :brutal_kill)

  defp maybe_persist_shutdown(reason, %State{status: status} = state)
       when reason in [:normal, :shutdown] and
              status not in [:opening, :finalizing, :rolling_back, :closed] do
    close_state(state)
  rescue
    _error -> state
  catch
    _kind, _reason -> state
  end

  defp maybe_persist_shutdown(_reason, state), do: state

  defp stop_session_telemetry(
         %State{session_started_monotonic: started_at} = state,
         reason
       )
       when is_integer(started_at) do
    Telemetry.stop([:session], started_at, %{
      session_id: state.session.id,
      provider: state.session.provider,
      status: state.status,
      reason: reason
    })
  end

  defp stop_session_telemetry(_state, _reason), do: :ok

  defp config_fingerprint(config) do
    config
    |> Map.from_struct()
    |> Map.delete(:store)
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end
end
