defmodule AgentHarness.Providers.Codex.ExecConnection do
  @moduledoc """
  App-server connection that runs `codex app-server` through an
  `AgentHarness.Exec` implementation instead of the SDK's subprocess
  transport.

  The codex app-server speaks newline-delimited JSON-RPC on stdio. This
  GenServer keeps the whole protocol on the orchestrator — the initialize
  handshake, request/response correlation, notification fan-out, and
  server-initiated requests (approvals) — while the CLI process itself runs
  wherever the configured exec module puts it.

  The process answers the same call messages as the SDK's own connections
  (Codex.AppServer.Connection and Codex.AppServer.RemoteConnection, both
  private), so everything downstream in the SDK — `Codex.Thread`,
  `Codex.AppServer.respond/3`, the harness `ConnectionProxy` — works against
  this pid unchanged. It intentionally mirrors the SDK's remote connection
  and reuses its protocol helpers; treat codex_sdk upgrades as a review
  point for this file.

  ## Options

    * `:exec` — `{module, opts}` implementing `AgentHarness.Exec`.
      Defaults to `{AgentHarness.Exec.Local, []}`.
    * `:owner` — pid whose death tears the connection down. Optional.
    * `:client_name`, `:client_version`, `:client_title`,
      `:experimental_api` — initialize handshake identity.
    * `:init_timeout_ms` — handshake deadline.
  """

  use GenServer

  require Logger

  alias Codex.AppServer.Protocol

  @default_exec {AgentHarness.Exec.Local, []}
  @default_init_timeout_ms 30_000
  @default_client_name "agent_harness"

  defmodule State do
    @moduledoc false

    defstruct [
      :exec_module,
      :handle,
      :owner_monitor,
      :phase,
      :buffer,
      :next_id,
      :pending,
      :ready_waiters,
      :subscribers,
      :subscriber_refs,
      :buffered_events
    ]
  end

  @type spec :: AgentHarness.Exec.spec()

  @spec start(spec(), keyword()) :: {:ok, pid()} | {:error, term()}
  def start(spec, opts \\ []) when is_map(spec) and is_list(opts) do
    case GenServer.start(__MODULE__, {spec, opts}) do
      {:ok, pid} -> {:ok, pid}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec stop(pid()) :: :ok
  def stop(conn) when is_pid(conn) do
    GenServer.stop(conn, :normal)
  catch
    :exit, _reason -> :ok
  end

  @impl true
  def init({spec, opts}) do
    {exec_module, exec_opts} = Keyword.get(opts, :exec) || @default_exec
    init_timeout_ms = Keyword.get(opts, :init_timeout_ms, @default_init_timeout_ms)

    init_params =
      initialize_params(
        Keyword.get(opts, :client_name, @default_client_name),
        Keyword.get(opts, :client_version, default_client_version()),
        Keyword.get(opts, :client_title),
        Keyword.get(opts, :experimental_api, false)
      )

    with {:ok, handle} <- exec_module.start(spec, self(), exec_opts),
         state = %State{
           exec_module: exec_module,
           handle: handle,
           owner_monitor: monitor_owner(Keyword.get(opts, :owner)),
           phase: :initializing,
           buffer: "",
           next_id: 1,
           pending: %{},
           ready_waiters: [],
           subscribers: %{},
           subscriber_refs: %{},
           buffered_events: []
         },
         {:ok, state} <- send_initialize(state, init_params, init_timeout_ms) do
      {:ok, state}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  defp monitor_owner(nil), do: nil
  defp monitor_owner(owner) when is_pid(owner), do: Process.monitor(owner)

  defp send_initialize(%State{} = state, init_params, init_timeout_ms) do
    case write(state, Protocol.encode_request(0, "initialize", init_params)) do
      :ok ->
        timer_ref = Process.send_after(self(), {:request_timeout, 0}, init_timeout_ms)

        pending =
          Map.put(state.pending, 0, %{
            from: :init,
            method: "initialize",
            timeout_ms: init_timeout_ms,
            timer_ref: timer_ref
          })

        {:ok, %State{state | pending: pending}}

      {:error, reason} ->
        safe_kill(state)
        {:error, {:init_write_failed, reason}}
    end
  end

  # ============================================================================
  # Connection call contract (mirrors Codex.AppServer.Connection)
  # ============================================================================

  @impl true
  def handle_call(:await_ready, _from, %State{phase: :ready} = state) do
    {:reply, :ok, state}
  end

  def handle_call(:await_ready, from, %State{} = state) do
    {:noreply, %State{state | ready_waiters: [from | state.ready_waiters]}}
  end

  def handle_call({:subscribe, pid, opts}, _from, %State{} = state) do
    ref = Process.monitor(pid)
    filters = normalize_subscriber_filters(opts)

    state = %State{
      state
      | subscribers: Map.put(state.subscribers, pid, filters),
        subscriber_refs: Map.put(state.subscriber_refs, ref, pid)
    }

    state =
      case state.buffered_events do
        [] ->
          state

        events ->
          send_buffered_events(pid, events)
          %State{state | buffered_events: []}
      end

    {:reply, :ok, state}
  end

  def handle_call({:unsubscribe, pid}, _from, %State{} = state) do
    {refs_to_drop, subscriber_refs} =
      Enum.reduce(state.subscriber_refs, {[], %{}}, fn {ref, sub_pid}, {refs, acc} ->
        if sub_pid == pid, do: {[ref | refs], acc}, else: {refs, Map.put(acc, ref, sub_pid)}
      end)

    Enum.each(refs_to_drop, &Process.demonitor(&1, [:flush]))

    {:reply, :ok,
     %State{
       state
       | subscribers: Map.delete(state.subscribers, pid),
         subscriber_refs: subscriber_refs
     }}
  end

  def handle_call({:request, _method, _params, _timeout_ms}, _from, %State{phase: phase} = state)
      when phase != :ready do
    {:reply, {:error, :not_ready}, state}
  end

  def handle_call({:request, method, params, timeout_ms}, from, %State{} = state) do
    id = state.next_id
    timer_ref = Process.send_after(self(), {:request_timeout, id}, timeout_ms)

    case write(state, Protocol.encode_request(id, method, params)) do
      :ok ->
        pending =
          Map.put(state.pending, id, %{
            from: from,
            method: method,
            timeout_ms: timeout_ms,
            timer_ref: timer_ref
          })

        {:noreply, %State{state | next_id: id + 1, pending: pending}}

      {:error, reason} ->
        _ = Process.cancel_timer(timer_ref)
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:respond, id, result}, _from, %State{} = state) do
    {:reply, write(state, Protocol.encode_response(id, result)), state}
  end

  def handle_call({:respond_error, id, code, message, data}, _from, %State{} = state) do
    {:reply, write(state, Protocol.encode_error(id, code, message, data)), state}
  end

  # ============================================================================
  # Exec stream
  # ============================================================================

  @impl true
  def handle_info({:exec_data, handle, chunk}, %State{handle: handle} = state) do
    {messages, buffer, _non_json} = Protocol.decode_lines(state.buffer, chunk)
    state = %State{state | buffer: buffer}

    Enum.reduce_while(messages, {:noreply, state}, fn message, {:noreply, acc_state} ->
      case handle_incoming_message(acc_state, message) do
        {:ok, next_state} -> {:cont, {:noreply, next_state}}
        {:stop, reason, next_state} -> {:halt, {:stop, reason, next_state}}
      end
    end)
  end

  def handle_info({:exec_exit, handle, reason}, %State{handle: handle} = state) do
    failure = {:app_server_down, %{reason: reason}}
    state = fail_transport_waiters(%State{state | handle: nil}, failure)
    {:stop, {:shutdown, failure}, state}
  end

  def handle_info({:request_timeout, id}, %State{} = state) do
    case Map.pop(state.pending, id) do
      {nil, _pending} ->
        {:noreply, state}

      {%{from: :init, timeout_ms: timeout_ms}, pending} ->
        state = %State{state | pending: pending}
        failure = {:init_timeout, timeout_ms}
        reply_ready_waiters(state.ready_waiters, {:error, failure})
        {:stop, :normal, %State{state | ready_waiters: []}}

      {%{from: from, method: method, timeout_ms: timeout_ms}, pending} ->
        GenServer.reply(from, {:error, {:timeout, method, timeout_ms}})
        {:noreply, %State{state | pending: pending}}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %State{owner_monitor: ref} = state) do
    {:stop, :normal, state}
  end

  def handle_info({:DOWN, ref, :process, pid, _reason}, %State{} = state) do
    state =
      case Map.pop(state.subscriber_refs, ref) do
        {nil, _refs} ->
          state

        {^pid, refs} ->
          %State{state | subscriber_refs: refs, subscribers: Map.delete(state.subscribers, pid)}
      end

    {:noreply, state}
  end

  def handle_info(_message, %State{} = state) do
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, %State{} = state) do
    if state.handle, do: safe_kill(state)
    :ok
  end

  # ============================================================================
  # Incoming JSON-RPC messages (mirrors Codex.AppServer.RemoteConnection)
  # ============================================================================

  defp handle_incoming_message(%State{} = state, message) when is_map(message) do
    case Protocol.message_type(message) do
      :notification ->
        method = Map.get(message, "method")
        params = Map.get(message, "params") || %{}
        {:ok, buffer_or_broadcast(state, {:notification, method, params})}

      :request ->
        id = Map.get(message, "id")
        method = Map.get(message, "method")
        params = Map.get(message, "params") || %{}
        {:ok, buffer_or_broadcast(state, {:request, id, method, params})}

      :response ->
        handle_response(state, Map.get(message, "id"), {:ok, Map.get(message, "result")})

      :error ->
        handle_response(state, Map.get(message, "id"), {:error, Map.get(message, "error")})

      :unknown ->
        Logger.debug("Ignoring unknown app-server JSON-RPC message: #{inspect(message)}")
        {:ok, state}
    end
  end

  defp buffer_or_broadcast(%State{phase: :initializing} = state, event) do
    %State{state | buffered_events: state.buffered_events ++ [event]}
  end

  defp buffer_or_broadcast(%State{} = state, {:notification, method, params}) do
    broadcast(state, {:codex_notification, method, params}, method, params)
  end

  defp buffer_or_broadcast(%State{} = state, {:request, id, method, params}) do
    broadcast(state, {:codex_request, id, method, params}, method, params)
  end

  defp handle_response(%State{} = state, id, reply) do
    case Map.pop(state.pending, id) do
      {nil, _pending} ->
        Logger.debug("Ignoring response for unknown app-server request id: #{inspect(id)}")
        {:ok, state}

      {%{from: :init, timer_ref: timer_ref}, pending} ->
        _ = Process.cancel_timer(timer_ref)
        handle_init_reply(%State{state | pending: pending}, reply)

      {%{from: from, timer_ref: timer_ref}, pending} ->
        _ = Process.cancel_timer(timer_ref)
        GenServer.reply(from, reply)
        {:ok, %State{state | pending: pending}}
    end
  end

  defp handle_init_reply(%State{} = state, {:ok, _result}) do
    case write(state, Protocol.encode_notification("initialized")) do
      :ok ->
        reply_ready_waiters(state.ready_waiters, :ok)
        {:ok, %State{state | phase: :ready, ready_waiters: []}}

      {:error, reason} ->
        fail_init(state, reason)
    end
  end

  defp handle_init_reply(%State{} = state, {:error, reason}) do
    fail_init(state, reason)
  end

  defp fail_init(%State{} = state, reason) do
    failure = {:init_failed, reason}
    reply_ready_waiters(state.ready_waiters, {:error, failure})
    {:stop, :normal, %State{state | ready_waiters: []}}
  end

  defp reply_ready_waiters(waiters, reply) do
    Enum.each(waiters, fn from -> GenServer.reply(from, reply) end)
  end

  defp fail_transport_waiters(%State{} = state, failure) do
    reply_ready_waiters(state.ready_waiters, {:error, failure})

    Enum.each(state.pending, fn
      {_id, %{from: :init, timer_ref: timer_ref}} ->
        _ = Process.cancel_timer(timer_ref)

      {_id, %{from: from, timer_ref: timer_ref}} ->
        _ = Process.cancel_timer(timer_ref)
        GenServer.reply(from, {:error, failure})
    end)

    %State{state | pending: %{}, ready_waiters: []}
  end

  # ============================================================================
  # Broadcast
  # ============================================================================

  defp broadcast(%State{} = state, message, method, params) do
    Enum.each(state.subscribers, fn {pid, filters} ->
      if subscriber_match?(filters, method, params) do
        send(pid, message)
      end
    end)

    state
  end

  defp send_buffered_events(pid, events) when is_pid(pid) do
    Enum.each(events, fn
      {:notification, method, params} ->
        send(pid, {:codex_notification, method, params})

      {:request, id, method, params} ->
        send(pid, {:codex_request, id, method, params})
    end)
  end

  defp subscriber_match?(%{methods: nil, thread_id: nil}, _method, _params), do: true

  defp subscriber_match?(filters, method, params) do
    method_matches?(filters.methods, method) and thread_matches?(filters.thread_id, params)
  end

  defp method_matches?(nil, _method), do: true
  defp method_matches?(methods, method) when is_list(methods), do: method in methods
  defp method_matches?(_methods, _method), do: false

  defp thread_matches?(nil, _params), do: true

  defp thread_matches?(thread_id, params) when is_binary(thread_id) do
    case Map.get(params, "threadId") || Map.get(params, "thread_id") ||
           Map.get(params, :thread_id) do
      nil -> true
      params_thread_id -> thread_id == params_thread_id
    end
  end

  defp thread_matches?(_thread_id, _params), do: false

  defp normalize_subscriber_filters(opts) do
    methods =
      case Keyword.get(opts, :methods) do
        nil -> nil
        list when is_list(list) -> Enum.map(list, &to_string/1)
        _invalid -> :invalid
      end

    thread_id =
      case Keyword.get(opts, :thread_id) do
        nil -> nil
        id when is_binary(id) -> id
        _invalid -> :invalid
      end

    %{methods: methods, thread_id: thread_id}
  end

  # ============================================================================
  # Exec plumbing
  # ============================================================================

  defp write(%State{handle: nil}, _payload), do: {:error, :not_connected}

  defp write(%State{} = state, payload) do
    state.exec_module.write(state.handle, payload)
  end

  defp safe_kill(%State{handle: nil}), do: :ok

  defp safe_kill(%State{} = state) do
    state.exec_module.kill(state.handle)
  rescue
    _error -> :ok
  catch
    _kind, _reason -> :ok
  end

  defp initialize_params(client_name, client_version, client_title, experimental_api) do
    %{
      "clientInfo" =>
        %{"name" => client_name, "version" => client_version}
        |> put_optional("title", client_title)
    }
    |> put_optional(
      "capabilities",
      if(experimental_api, do: %{"experimentalApi" => true}, else: nil)
    )
  end

  defp default_client_version do
    case Application.spec(:agent_harness, :vsn) do
      nil -> "0.0.0"
      version -> to_string(version)
    end
  end

  defp put_optional(map, _key, nil), do: map
  defp put_optional(map, key, value), do: Map.put(map, key, value)
end
