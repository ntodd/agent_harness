defmodule AgentHarness.SessionServer do
  @moduledoc false

  use GenServer

  alias AgentHarness.{
    Event,
    EventBuffer,
    Request,
    Response,
    Subscription,
    Turn
  }

  alias AgentHarness.Provider.Sink

  defmodule State do
    @moduledoc false

    @enforce_keys [
      :session,
      :config,
      :provider,
      :provider_handle,
      :sink,
      :event_buffer
    ]
    defstruct [
      :session,
      :config,
      :provider,
      :provider_handle,
      :provider_session_id,
      :sink,
      :current_turn,
      :provider_turn_ref,
      status: :idle,
      turns: %{},
      requests: %{},
      subscriptions: %{},
      monitors: %{},
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
      type: :worker
    }
  end

  def start_link(opts) do
    session = Keyword.fetch!(opts, :session)
    GenServer.start_link(__MODULE__, opts, name: via(session.id))
  end

  @impl true
  def init(opts) do
    session = Keyword.fetch!(opts, :session)
    config = Keyword.fetch!(opts, :config)
    provider = Keyword.fetch!(opts, :provider)
    sink = Sink.new(self())

    case provider.open_session(config, sink) do
      {:ok, provider_handle, info} ->
        state = %State{
          session: session,
          config: config,
          provider: provider,
          provider_handle: provider_handle,
          provider_session_id: Map.get(info, :provider_session_id),
          sink: sink,
          event_buffer: EventBuffer.new(config.event_buffer_size)
        }

        state =
          emit(state, nil, :session_ready, %{
            provider_session_id: state.provider_session_id
          })

        {:ok, state}

      {:error, reason} ->
        {:stop, {:provider_open_failed, reason}}
    end
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
      provider_session_id: state.provider_session_id,
      current_turn: state.current_turn,
      pending_requests: pending_requests
    }

    {:reply, snapshot, state}
  end

  def handle_call(:capabilities, _from, state) do
    {:reply, state.provider.capabilities(state.provider_handle), state}
  end

  def handle_call({:start_turn, input, opts}, _from, %State{status: :idle} = state) do
    metadata = Keyword.get(opts, :metadata, %{})
    turn_opts = [metadata: metadata]

    turn_opts =
      if id = Keyword.get(opts, :id), do: Keyword.put(turn_opts, :id, id), else: turn_opts

    turn = Turn.new(state.session.id, input, turn_opts)
    provider_opts = Keyword.drop(opts, [:id, :metadata])

    case state.provider.start_turn(state.provider_handle, turn, input, provider_opts) do
      {:ok, provider_turn_ref} ->
        now = DateTime.utc_now()
        turn = %{turn | status: :running, started_at: now}

        state = %{
          state
          | status: :running,
            current_turn: turn,
            provider_turn_ref: provider_turn_ref,
            turns: Map.put(state.turns, turn.id, turn)
        }

        state =
          emit(state, turn.id, :turn_started, %{
            turn: turn,
            provider_turn_ref: provider_turn_ref
          })

        {:reply, {:ok, turn}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:start_turn, _input, _opts}, _from, state) do
    {:reply, {:error, {:turn_in_progress, state.current_turn}}, state}
  end

  def handle_call({:subscribe, pid, turn_id, from}, _from, state) when is_pid(pid) do
    subscription = %Subscription{
      ref: make_ref(),
      session_id: state.session.id,
      turn_id: turn_id,
      pid: pid
    }

    monitor = Process.monitor(pid)
    subscriber = %{pid: pid, monitor: monitor, turn_id: turn_id}

    state = %{
      state
      | subscriptions: Map.put(state.subscriptions, subscription.ref, subscriber),
        monitors: Map.put(state.monitors, monitor, subscription.ref)
    }

    state.event_buffer
    |> events_from(from)
    |> Enum.filter(&matches_turn?(&1, turn_id))
    |> Enum.each(&deliver(subscription.ref, pid, &1))

    {:reply, {:ok, subscription}, state}
  end

  def handle_call({:unsubscribe, subscription_ref}, _from, state) do
    {:reply, :ok, remove_subscription(state, subscription_ref)}
  end

  def handle_call({:respond, request_id, response}, _from, state) do
    case Map.get(state.requests, request_id) do
      nil ->
        {:reply, {:error, :request_not_found}, state}

      %Request{status: :resolved} ->
        {:reply, {:error, :already_resolved}, state}

      %Request{status: :expired} ->
        {:reply, {:error, :request_expired}, state}

      %Request{status: :pending} = request ->
        respond_to_request(state, request, response)
    end
  end

  def handle_call({:cancel, turn_id}, _from, %State{current_turn: %Turn{id: turn_id}} = state) do
    cancel_current_turn(state)
  end

  def handle_call({:cancel, _turn_id}, _from, state) do
    {:reply, {:error, :turn_not_active}, state}
  end

  def handle_call({:stop, false}, _from, %State{status: :idle} = state) do
    {:stop, :normal, :ok, state}
  end

  def handle_call({:stop, false}, _from, state) do
    {:reply, {:error, :turn_in_progress}, state}
  end

  def handle_call({:stop, true}, _from, %State{current_turn: nil} = state) do
    {:stop, :normal, :ok, state}
  end

  def handle_call({:stop, true}, _from, state) do
    case state.provider.cancel(state.provider_handle, state.provider_turn_ref) do
      :ok ->
        state = emit(state, state.current_turn.id, :cancel_requested)
        state = expire_pending_requests(state, state.current_turn.id)
        state = complete_turn(state, state.current_turn.id, :cancelled, %{forced: true}, nil)
        {:stop, :normal, :ok, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_info(
        {:agent_harness_provider, sink_ref, {:event, turn_id, type, data, raw}},
        %State{sink: %Sink{ref: sink_ref}} = state
      ) do
    if current_turn?(state, turn_id) do
      {:noreply, emit(state, turn_id, type, data, raw)}
    else
      {:noreply, state}
    end
  end

  def handle_info(
        {:agent_harness_provider, sink_ref, {:request, turn_id, provider_ref, attrs, raw}},
        %State{sink: %Sink{ref: sink_ref}} = state
      ) do
    if current_turn?(state, turn_id) do
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

      {:noreply, emit(state, turn_id, :request_created, request, raw)}
    else
      {:noreply, state}
    end
  end

  def handle_info(
        {:agent_harness_provider, sink_ref, {:finish, turn_id, status, result, raw}},
        %State{sink: %Sink{ref: sink_ref}} = state
      ) do
    if current_turn?(state, turn_id) do
      state = expire_pending_requests(state, turn_id)
      {:noreply, complete_turn(state, turn_id, status, result, raw)}
    else
      {:noreply, state}
    end
  end

  def handle_info(
        {:agent_harness_provider, sink_ref, {:session_updated, attrs}},
        %State{sink: %Sink{ref: sink_ref}} = state
      ) do
    state =
      case Map.fetch(attrs, :provider_session_id) do
        {:ok, provider_session_id} -> %{state | provider_session_id: provider_session_id}
        :error -> state
      end

    {:noreply, emit(state, nil, :session_updated, attrs)}
  end

  def handle_info(
        {:agent_harness_provider, sink_ref, {:transport_down, reason}},
        %State{sink: %Sink{ref: sink_ref}} = state
      ) do
    state = emit(state, current_turn_id(state), :transport_error, %{reason: reason})

    state =
      case state.current_turn do
        nil -> %{state | status: :unavailable}
        turn -> complete_turn(state, turn.id, :failed, %{reason: reason}, nil)
      end

    {:noreply, %{state | status: :unavailable}}
  end

  def handle_info({:agent_harness_provider, _stale_ref, _message}, state) do
    {:noreply, state}
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

  @impl true
  def terminate(_reason, state) do
    state.provider.close_session(state.provider_handle)
  end

  defp via(session_id) do
    {:via, Registry, {AgentHarness.SessionRegistry, session_id}}
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

        data = %{request: request, response: response}
        {:reply, :ok, emit(state, request.turn_id, :request_resolved, data)}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  defp cancel_current_turn(%State{status: :cancelling} = state) do
    {:reply, :ok, state}
  end

  defp cancel_current_turn(state) do
    case state.provider.cancel(state.provider_handle, state.provider_turn_ref) do
      :ok ->
        state = %{state | status: :cancelling}
        {:reply, :ok, emit(state, state.current_turn.id, :cancel_requested)}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  defp complete_turn(state, turn_id, status, result, raw) do
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
        turns: Map.put(state.turns, turn_id, turn)
    }

    emit(state, turn_id, event_type, %{status: status, result: result}, raw)
  end

  defp expire_pending_requests(state, turn_id) do
    state.requests
    |> Map.values()
    |> Enum.filter(&(&1.turn_id == turn_id and &1.status == :pending))
    |> Enum.reduce(state, fn request, acc ->
      request = %{request | status: :expired}
      acc = %{acc | requests: Map.put(acc.requests, request.id, request)}
      emit(acc, turn_id, :request_expired, request)
    end)
  end

  defp emit(state, turn_id, type, data \\ %{}, raw \\ nil) do
    seq = state.seq + 1

    event =
      Event.new(
        seq: seq,
        session_id: state.session.id,
        turn_id: turn_id,
        provider: state.session.provider,
        type: type,
        data: data,
        raw: raw
      )

    Enum.each(state.subscriptions, fn {subscription_ref, subscriber} ->
      if matches_turn?(event, subscriber.turn_id) do
        deliver(subscription_ref, subscriber.pid, event)
      end
    end)

    %{
      state
      | seq: seq,
        event_buffer: EventBuffer.push(state.event_buffer, event)
    }
  end

  defp events_from(buffer, :start), do: EventBuffer.from(buffer, :start)
  defp events_from(buffer, :latest), do: EventBuffer.from(buffer, :latest)

  defp events_from(buffer, {:after, seq}) when is_integer(seq),
    do: EventBuffer.from(buffer, seq + 1)

  defp matches_turn?(_event, nil), do: true
  defp matches_turn?(%Event{turn_id: turn_id}, turn_id), do: true
  defp matches_turn?(_event, _turn_id), do: false

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
end
