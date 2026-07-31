defmodule AgentHarness.Providers.Codex.ConnectionProxy do
  @moduledoc false

  use GenServer

  alias Codex.AppServer.Connection

  defstruct [
    :connection,
    :connection_monitor,
    :owner,
    :owner_monitor,
    :disconnect,
    subscribers: %{},
    subscriber_monitors: %{},
    subscriber_refs: %{}
  ]

  @type t :: pid()

  @spec start_link(pid(), keyword()) :: GenServer.on_start()
  def start_link(connection, options \\ []) when is_pid(connection) and is_list(options) do
    owner = Keyword.get(options, :owner, self())
    GenServer.start_link(__MODULE__, {connection, owner, options})
  end

  @spec start(pid(), keyword()) :: GenServer.on_start()
  def start(connection, options \\ []) when is_pid(connection) and is_list(options) do
    owner = Keyword.get(options, :owner, self())
    GenServer.start(__MODULE__, {connection, owner, options})
  end

  @spec disconnect(t()) :: :ok
  def disconnect(proxy) when is_pid(proxy) do
    GenServer.call(proxy, :disconnect)
  catch
    :exit, _reason -> :ok
  end

  @impl true
  def init({connection, owner, options}) do
    disconnect = Keyword.get(options, :disconnect, &Codex.AppServer.disconnect/1)

    case Connection.subscribe(connection) do
      :ok ->
        {:ok,
         %__MODULE__{
           connection: connection,
           connection_monitor: Process.monitor(connection),
           owner: owner,
           owner_monitor: Process.monitor(owner),
           disconnect: disconnect
         }}

      {:error, reason} ->
        {:stop, {:proxy_subscribe_failed, reason}}
    end
  end

  @impl true
  def handle_call(:await_ready, _from, state), do: {:reply, :ok, state}

  def handle_call({:subscribe, pid, options}, _from, state) do
    state = put_subscriber(state, pid, normalize_filters(options))
    {:reply, :ok, state}
  end

  def handle_call({:unsubscribe, pid}, _from, state) do
    {:reply, :ok, drop_subscriber(state, pid)}
  end

  def handle_call({:request, method, params, timeout_ms}, from, state) do
    forward_call(from, fn ->
      Connection.request(state.connection, method, params, timeout_ms: timeout_ms)
    end)

    {:noreply, state}
  end

  def handle_call({:respond, id, result}, from, state) do
    forward_call(from, fn -> Connection.respond(state.connection, id, result) end)
    {:noreply, state}
  end

  def handle_call({:respond_error, id, code, message, data}, from, state) do
    forward_call(from, fn ->
      Connection.respond_error(state.connection, id, code, message, data)
    end)

    {:noreply, state}
  end

  def handle_call(:disconnect, _from, state), do: {:stop, :normal, :ok, state}

  @impl true
  def handle_info({:codex_notification, method, params}, state) do
    params = repair_notification(method, params)
    broadcast(state, {:codex_notification, method, params}, method, params)
    {:noreply, state}
  end

  def handle_info({:codex_request, id, method, params}, state) do
    if unresolvable_request?(method) do
      send(state.owner, {:codex_unresolvable_request, id, method, params})
    else
      broadcast(state, {:codex_request, id, method, params}, method, params)
    end

    {:noreply, state}
  end

  def handle_info(
        {:DOWN, monitor, :process, _pid, reason},
        %{connection_monitor: monitor} = state
      ) do
    {:stop, {:wrapped_connection_down, reason}, %{state | connection: nil}}
  end

  def handle_info(
        {:DOWN, monitor, :process, _pid, _reason},
        %{owner_monitor: monitor} = state
      ) do
    {:stop, :normal, state}
  end

  def handle_info({:DOWN, monitor, :process, pid, _reason}, state) do
    case Map.get(state.subscriber_refs, monitor) do
      ^pid -> {:noreply, drop_subscriber(state, pid)}
      _ -> {:noreply, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    if is_pid(state.connection) do
      _ = Connection.unsubscribe(state.connection)
      safe_disconnect(state.disconnect, state.connection)
    end

    :ok
  end

  defp forward_call(from, fun) do
    case Task.Supervisor.start_child(AgentHarness.RunnerSupervisor, fn ->
           GenServer.reply(from, safe_forward(fun))
         end) do
      {:ok, _pid} ->
        :ok

      {:error, reason} ->
        GenServer.reply(from, {:error, {:forward_start_failed, reason}})
    end
  end

  defp safe_forward(fun) do
    fun.()
  rescue
    error -> {:error, {:forward_failed, Exception.message(error)}}
  catch
    :exit, reason -> {:error, {:forward_failed, reason}}
    kind, reason -> {:error, {:forward_failed, {kind, reason}}}
  end

  defp safe_disconnect(disconnect, connection) do
    disconnect.(connection)
  rescue
    _error -> :ok
  catch
    _kind, _reason -> :ok
  end

  defp put_subscriber(state, pid, filters) do
    state = drop_subscriber(state, pid)
    monitor = Process.monitor(pid)

    %{
      state
      | subscribers: Map.put(state.subscribers, pid, filters),
        subscriber_monitors: Map.put(state.subscriber_monitors, pid, monitor),
        subscriber_refs: Map.put(state.subscriber_refs, monitor, pid)
    }
  end

  defp drop_subscriber(state, pid) do
    case Map.pop(state.subscriber_monitors, pid) do
      {nil, monitors} ->
        %{state | subscribers: Map.delete(state.subscribers, pid), subscriber_monitors: monitors}

      {monitor, monitors} ->
        Process.demonitor(monitor, [:flush])

        %{
          state
          | subscribers: Map.delete(state.subscribers, pid),
            subscriber_monitors: monitors,
            subscriber_refs: Map.delete(state.subscriber_refs, monitor)
        }
    end
  end

  defp broadcast(state, message, method, params) do
    Enum.reduce(state.subscribers, false, fn {pid, filters}, delivered? ->
      if subscriber_match?(filters, method, params) do
        send(pid, message)
        true
      else
        delivered?
      end
    end)
  end

  defp subscriber_match?(%{methods: nil, thread_id: nil}, _method, _params), do: true

  defp subscriber_match?(filters, method, params) do
    method_match?(filters.methods, method) and thread_match?(filters.thread_id, params)
  end

  defp method_match?(nil, _method), do: true
  defp method_match?(methods, method) when is_list(methods), do: method in methods
  defp method_match?(_methods, _method), do: false

  defp thread_match?(nil, _params), do: true

  defp thread_match?(thread_id, params) when is_binary(thread_id) and is_map(params) do
    case Map.get(params, "threadId") || Map.get(params, "thread_id") ||
           Map.get(params, :thread_id) do
      nil -> true
      value -> value == thread_id
    end
  end

  defp thread_match?(thread_id, _params) when is_binary(thread_id), do: true
  defp thread_match?(_thread_id, _params), do: false

  defp normalize_filters(options) do
    %{
      methods: normalize_methods(Keyword.get(options, :methods)),
      thread_id: normalize_thread_id(Keyword.get(options, :thread_id))
    }
  end

  defp normalize_methods(nil), do: nil

  defp normalize_methods(methods) when is_list(methods) do
    Enum.flat_map(methods, fn method ->
      case String.Chars.impl_for(method) do
        nil -> []
        _impl -> [to_string(method)]
      end
    end)
  end

  defp normalize_methods(_invalid), do: :invalid

  defp normalize_thread_id(nil), do: nil
  defp normalize_thread_id(thread_id) when is_binary(thread_id), do: thread_id
  defp normalize_thread_id(_invalid), do: :invalid

  defp repair_notification(
         "thread/status/changed",
         %{"status" => %{"type" => "active"} = status} = params
       ) do
    case Map.get(status, "activeFlags", []) do
      flags when is_list(flags) -> params
      _invalid -> put_in(params, ["status", "activeFlags"], [])
    end
  end

  defp repair_notification(_method, params), do: params

  defp unresolvable_request?("account/chatgptAuthTokens/refresh"), do: true
  defp unresolvable_request?("attestation/generate"), do: true
  defp unresolvable_request?(_method), do: false
end
