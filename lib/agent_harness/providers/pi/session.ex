defmodule AgentHarness.Providers.Pi.Session do
  @moduledoc false

  use GenServer

  alias AgentHarness.Provider.{OpenGuardian, Sink}
  alias AgentHarness.Providers.Pi.{Config, Normalizer, Protocol}
  alias AgentHarness.{Response, SessionConfig, Turn}

  defmodule State do
    @moduledoc false

    @enforce_keys [:owner, :owner_monitor, :config, :prepared, :client, :transport, :sink]
    defstruct [
      :owner,
      :owner_monitor,
      :config,
      :prepared,
      :client,
      :transport,
      :sink,
      :provider_session_id,
      :active,
      ready?: false,
      waiting: [],
      commands: %{},
      requests: %{},
      counter: 0
    ]
  end

  @spec start(SessionConfig.t(), Sink.t(), pid()) :: {:ok, pid(), map()} | {:error, term()}
  def start(%SessionConfig{} = config, %Sink{} = sink, owner) when is_pid(owner) do
    # Configuration and credentials are settled before a process exists, so a
    # rejected session reports its own reason instead of a supervisor exit.
    with {:ok, prepared} <- Config.prepare(config),
         :ok <- verify_auth(prepared, config.startup_timeout) do
      start_child(config, prepared, sink, owner)
    end
  end

  defp start_child(config, prepared, sink, owner) do
    child = {__MODULE__, config: config, prepared: prepared, sink: sink, owner: owner}

    case DynamicSupervisor.start_child(AgentHarness.ProviderSupervisor, child) do
      {:ok, pid} ->
        case safe_call(pid, :info, config.startup_timeout) do
          %{provider_session_id: _id} = info ->
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

  @spec start_turn(pid(), Turn.t(), term(), keyword()) ::
          {:ok, reference()} | {:error, term()}
  def start_turn(pid, %Turn{} = turn, input, opts) do
    safe_call(pid, {:start_turn, turn, input, opts}, command_timeout(opts))
  end

  @spec respond(pid(), term(), Response.t()) :: :ok | {:error, term()}
  def respond(pid, provider_request_ref, %Response{} = response) do
    safe_call(pid, {:respond, provider_request_ref, response})
  end

  @spec cancel(pid(), reference()) :: :ok | {:error, term()}
  def cancel(pid, provider_turn_ref) do
    safe_call(pid, {:cancel, provider_turn_ref})
  end

  @spec close(pid()) :: :ok
  def close(pid) do
    GenServer.stop(pid, :normal)
  catch
    :exit, _reason -> :ok
  end

  @impl true
  def init(opts) do
    config = Keyword.fetch!(opts, :config)
    prepared = Keyword.fetch!(opts, :prepared)
    sink = Keyword.fetch!(opts, :sink)
    owner = Keyword.fetch!(opts, :owner)
    guardian = OpenGuardian.start(self(), owner)

    {:ok, {:opening, config, prepared, sink, owner, guardian}, {:continue, :open}}
  end

  @impl true
  def handle_continue(:open, {:opening, config, prepared, sink, owner, guardian}) do
    case prepared.client.open(prepared, self()) do
      {:ok, transport} ->
        open_ready(config, prepared, sink, owner, guardian, transport)

      {:error, reason} ->
        # Stay alive just long enough to hand the caller the real reason.
        # Stopping here instead would surface as an opaque supervisor exit.
        OpenGuardian.disarm(guardian)
        {:noreply, {:failed, reason}}
    end
  end

  def handle_continue(:arm_startup_timeout, state) do
    Process.send_after(self(), :startup_timeout, state.config.startup_timeout)
    {:noreply, state}
  end

  @impl true
  def handle_call(:info, _from, {:failed, reason} = state) do
    {:stop, :normal, {:error, reason}, state}
  end

  def handle_call(_message, _from, {:failed, reason} = state) do
    {:reply, {:error, reason}, state}
  end

  def handle_call(:info, from, %State{ready?: false} = state) do
    {:noreply, %{state | waiting: [from | state.waiting]}}
  end

  def handle_call(:info, _from, state) do
    {:reply, %{provider_session_id: state.provider_session_id}, state}
  end

  def handle_call({:start_turn, _turn, _input, _opts}, _from, %State{transport: nil} = state) do
    {:reply, {:error, :provider_not_found}, state}
  end

  def handle_call({:start_turn, %Turn{} = turn, input, opts}, from, %State{active: nil} = state) do
    case message_text(input) do
      {:ok, message} -> send_prompt(state, turn, message, opts, from)
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:start_turn, _turn, _input, _opts}, _from, state) do
    {:reply, {:error, :turn_already_active}, state}
  end

  def handle_call({:respond, ref, response}, _from, state) do
    case Map.fetch(state.requests, ref) do
      {:ok, metadata} ->
        reply_to_dialog(state, ref, metadata, response)

      :error ->
        {:reply, {:error, :unknown_request}, state}
    end
  end

  def handle_call({:cancel, ref}, _from, %State{active: %{ref: ref}} = state) do
    {id, state} = next_id(state)

    case state.client.send_frame(state.transport, Protocol.command(id, :abort)) do
      :ok -> {:reply, :ok, state}
      {:error, reason} -> {:reply, {:error, {:provider_command_uncertain, reason}}, state}
    end
  end

  def handle_call({:cancel, _ref}, _from, state) do
    {:reply, {:error, :unknown_turn}, state}
  end

  @impl true
  def handle_info({:pi_frame, transport, frame}, %State{transport: transport} = state) do
    {:noreply, handle_frame(state, Normalizer.normalize(frame), frame)}
  end

  def handle_info({:pi_down, transport, reason}, %State{transport: transport} = state) do
    state = fail_pending(state, reason)
    Sink.transport_down(state.sink, {:pi_transport_down, reason})
    {:stop, :normal, %{state | transport: nil}}
  end

  def handle_info(:startup_timeout, %State{ready?: false} = state) do
    {:stop, {:pi_startup_timeout, state.config.startup_timeout}, state}
  end

  def handle_info(:startup_timeout, state), do: {:noreply, state}

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %State{owner_monitor: ref} = state) do
    {:stop, :normal, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %State{transport: transport, client: client})
      when not is_nil(transport) do
    client.close(transport)
    :ok
  rescue
    # Shutdown must not turn into a second failure; the transport is torn down
    # by its own owner monitor regardless.
    _error -> :ok
  end

  def terminate(_reason, _state), do: :ok

  defp open_ready(config, prepared, sink, owner, guardian, transport) do
    state = %State{
      owner: owner,
      owner_monitor: Process.monitor(owner),
      config: config,
      prepared: prepared,
      client: prepared.client,
      transport: transport,
      sink: sink,
      provider_session_id: prepared.provider_session_id
    }

    OpenGuardian.disarm(guardian)

    # Pi emits nothing until it is spoken to, so readiness is established by a
    # `get_state` round trip rather than a startup frame.
    {:noreply, probe_readiness(state), {:continue, :arm_startup_timeout}}
  end

  defp send_prompt(state, turn, message, opts, from) do
    {id, state} = next_id(state)
    payload = prompt_payload(message, opts)

    case state.client.send_frame(state.transport, Protocol.command(id, :prompt, payload)) do
      :ok ->
        provider_turn_ref = make_ref()

        state = %{
          state
          | active: %{
              turn_id: turn.id,
              ref: provider_turn_ref,
              status: :completed,
              message: "",
              usage: nil
            },
            commands: Map.put(state.commands, id, {:start_turn, from, provider_turn_ref})
        }

        {:noreply, state}

      {:error, reason} ->
        {:reply, {:error, {:turn_start_uncertain, reason}}, state}
    end
  end

  ## Frame handling

  defp handle_frame(state, {:response, id, data}, _raw) do
    case Map.pop(state.commands, id) do
      {nil, _commands} ->
        state

      {{:readiness, _id}, commands} ->
        become_ready(%{state | commands: commands}, data)

      {{:start_turn, from, provider_turn_ref}, commands} ->
        state = %{state | commands: commands}
        settle_turn_start(state, from, provider_turn_ref, data)
    end
  end

  defp handle_frame(state, {:event, type, data}, raw) do
    emit(state, type, data, raw)
    accumulate(state, type, data)
  end

  defp handle_frame(state, {:request, ref, attrs}, raw) do
    case state.active do
      %{turn_id: turn_id} ->
        Sink.request(state.sink, turn_id, ref, attrs, raw)
        %{state | requests: Map.put(state.requests, ref, attrs[:metadata] || %{})}

      nil ->
        # Nothing owns this dialog, and an unanswered dialog blocks pi forever.
        dismiss_dialog(state, ref)
    end
  end

  defp handle_frame(state, {:stopped, status, data}, raw) do
    emit(state, :provider_turn_completed, data, raw)
    put_active_status(state, status)
  end

  defp handle_frame(state, {:settle, _data}, raw) do
    case state.active do
      %{turn_id: turn_id, status: status} = active ->
        Sink.finish(state.sink, turn_id, status, turn_result(state, active), raw)
        %{state | active: nil, requests: %{}}

      nil ->
        state
    end
  end

  defp handle_frame(state, :ignore, _raw), do: state

  defp turn_result(state, active) do
    %{provider_session_id: state.provider_session_id}
    |> maybe_put(:text, nonempty(active.message))
    |> maybe_put(:usage, active.usage)
  end

  # Deltas build the message as it streams; a non-empty `message_end` text is
  # authoritative and replaces what was accumulated.
  defp accumulate(%State{active: nil} = state, _type, _data), do: state

  defp accumulate(%State{active: active} = state, :message_delta, %{text: text})
       when is_binary(text) do
    %{state | active: %{active | message: active.message <> text}}
  end

  defp accumulate(%State{active: active} = state, :message_completed, data) do
    active =
      case data do
        %{text: text} when is_binary(text) and text != "" -> %{active | message: text}
        _other -> active
      end

    %{state | active: %{active | usage: Map.get(data, :usage) || active.usage}}
  end

  defp accumulate(state, _type, _data), do: state

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp nonempty(""), do: nil
  defp nonempty(value), do: value

  # A pi run that ends in an error stops as failed even though the stop reason
  # itself may look ordinary, so error status wins over a later completion.
  defp put_active_status(%State{active: nil} = state, _status), do: state

  defp put_active_status(%State{active: %{status: :failed}} = state, _status), do: state

  defp put_active_status(%State{active: active} = state, status) do
    %{state | active: %{active | status: status}}
  end

  defp settle_turn_start(state, from, provider_turn_ref, %{success: true}) do
    GenServer.reply(from, {:ok, provider_turn_ref})
    state
  end

  defp settle_turn_start(state, from, _provider_turn_ref, %{success: false} = data) do
    # Pi rejects a prompt before any work starts, so this is a definite
    # failure rather than an uncertain start.
    GenServer.reply(from, {:error, {:pi_prompt_rejected, Map.get(data, :error)}})
    %{state | active: nil}
  end

  defp become_ready(state, %{success: true, data: data}) when is_map(data) do
    provider_session_id = Map.get(data, "sessionId") || state.provider_session_id
    state = %{state | ready?: true, provider_session_id: provider_session_id}

    info = %{provider_session_id: provider_session_id}
    Enum.each(state.waiting, &GenServer.reply(&1, info))

    %{state | waiting: []}
  end

  defp become_ready(state, data) do
    Enum.each(state.waiting, &GenServer.reply(&1, {:error, {:pi_readiness_failed, data}}))
    %{state | waiting: [], ready?: true}
  end

  defp probe_readiness(state) do
    {id, state} = next_id(state)

    case state.client.send_frame(state.transport, Protocol.command(id, :get_state)) do
      :ok -> %{state | commands: Map.put(state.commands, id, {:readiness, id})}
      {:error, _reason} -> state
    end
  end

  defp reply_to_dialog(state, ref, metadata, response) do
    case Protocol.encode(ref, metadata, response) do
      {:ok, frame} ->
        case state.client.send_frame(state.transport, frame) do
          :ok ->
            {:reply, :ok, %{state | requests: Map.delete(state.requests, ref)}}

          {:error, reason} ->
            {:reply, {:error, {:provider_command_uncertain, reason}}, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  defp dismiss_dialog(state, ref) do
    frame = %{"type" => "extension_ui_response", "id" => ref, "cancelled" => true}
    _ = state.client.send_frame(state.transport, frame)
    state
  end

  defp fail_pending(state, reason) do
    Enum.each(state.commands, fn
      {_id, {:start_turn, from, _ref}} ->
        GenServer.reply(from, {:error, {:turn_start_uncertain, reason}})

      {_id, {:readiness, _id2}} ->
        :ok
    end)

    Enum.each(state.waiting, &GenServer.reply(&1, {:error, {:pi_transport_down, reason}}))

    %{state | commands: %{}, waiting: []}
  end

  defp emit(%State{active: %{turn_id: turn_id}} = state, type, data, raw) do
    Sink.emit(state.sink, turn_id, type, data, raw)
  end

  defp emit(_state, _type, _data, _raw), do: :ok

  defp prompt_payload(message, opts) do
    case Keyword.get(opts, :streaming_behavior) do
      nil -> %{message: message}
      behavior -> %{message: message, streaming_behavior: to_string(behavior)}
    end
  end

  defp message_text(input) when is_binary(input), do: {:ok, input}

  defp message_text(input) when is_list(input) do
    if Enum.all?(input, &is_binary/1) do
      {:ok, Enum.join(input, "\n")}
    else
      {:error, {:unsupported_input, input}}
    end
  end

  defp message_text(input), do: {:error, {:unsupported_input, input}}

  defp verify_auth(%{auth: :inherit}, _timeout), do: :ok

  defp verify_auth(%{auth: :subscription} = prepared, timeout) do
    prepared.client.verify_subscription_auth(prepared, timeout)
  end

  defp next_id(%State{counter: counter} = state) do
    {"ah-#{counter}", %{state | counter: counter + 1}}
  end

  defp command_timeout(opts) do
    Keyword.get(opts, :timeout, 30_000)
  end

  defp safe_call(pid, message, timeout \\ 30_000) do
    GenServer.call(pid, message, timeout)
  catch
    :exit, reason -> {:error, {:provider_call_failed, reason}}
  end
end
