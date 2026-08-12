defmodule AgentHarness.Providers.Claude.Adapter.Exec do
  @moduledoc """
  `ClaudeCode.Adapter` that runs the Claude Code CLI through an
  `AgentHarness.Exec` implementation instead of a local port.

  This keeps the whole `claude_code` protocol stack — stream-json framing,
  the control-protocol handshake, hook and MCP routing, questions and
  approvals — on the orchestrator, while the CLI process itself runs
  wherever the configured exec module puts it: locally through
  `AgentHarness.Exec.Local`, or in a remote sandbox through an exec
  implementation provided by the application.

  Select it per session with `auth: :inherit` (the fail-closed
  `:subscription` mode pins the local port adapter by design):

      provider_options: %{
        auth: :inherit,
        adapter: {AgentHarness.Providers.Claude.Adapter.Exec,
                  exec: {MyApp.SandboxExec, sandbox: sandbox}}
      }

  ## Adapter options

    * `:exec` — `{module, opts}` implementing `AgentHarness.Exec`.
      Defaults to `{AgentHarness.Exec.Local, []}`.
    * `:cli_path` — executable name or path, resolved in the execution
      environment. Defaults to `"claude"`.

  All other options are the ordinary `claude_code` session options
  (`:model`, `:cwd`, `:env`, `:api_key`, `:can_use_tool`, `:resume`, ...).

  ## Differences from `ClaudeCode.Adapter.Port`

    * The spawn spec is remote-safe: `cwd` and the executable resolve in
      the execution environment, and only explicit entries are forwarded:
      the SDK vars, the `:env` option, and `:api_key`. The Port adapter
      forwards the orchestrator's whole `System.get_env/0`; this adapter
      never does. The execution environment's own environment remains the
      base, so under `Exec.Local` the command still inherits this VM's
      environment because the command runs here.
    * `cwd` has no orchestrator-side default. The Port adapter falls back
      to `File.cwd!()`; here a missing `:cwd` means the execution
      environment's default working directory, and the `can_use_tool`
      callback context omits `:cwd` until one is configured.
    * No reconnect-on-query. When the exec reports exit, the adapter stays
      disconnected — AgentHarness treats provider transport loss as
      session-fatal, and a dead sandbox process usually means a dead
      sandbox.
    * `execute/4` applies on the orchestrator node. SDK features that
      expect filesystem access next to the CLI (History, Plugin/skills
      materialization) do not reach the execution environment.

  This module intentionally mirrors `ClaudeCode.Adapter.Port` and reuses
  the SDK's protocol helpers. It is coupled to the pinned `claude_code`
  version; treat SDK upgrades as a review point for this file.
  """

  @behaviour ClaudeCode.Adapter

  use GenServer

  alias ClaudeCode.Adapter
  alias ClaudeCode.Adapter.ControlHandler
  alias ClaudeCode.Adapter.Port, as: PortAdapter
  alias ClaudeCode.CLI.Command
  alias ClaudeCode.CLI.Control
  alias ClaudeCode.CLI.Input
  alias ClaudeCode.CLI.Parser
  alias ClaudeCode.Hook.Registry, as: HookRegistry
  alias ClaudeCode.MCP.Status, as: MCPStatus
  alias ClaudeCode.Model
  alias ClaudeCode.Session.AccountInfo
  alias ClaudeCode.Session.AgentInfo
  alias ClaudeCode.Session.SlashCommand

  require Logger

  @default_control_timeout 60_000
  @default_cli_path "claude"

  # Keys consumed by this adapter that must never reach CLI command building.
  @adapter_internal_keys [
    :exec,
    :cli_path,
    :callback_proxy,
    :control_timeout,
    :hook_registry,
    :sdk_mcp_servers,
    :max_buffer_size
  ]

  defstruct [
    :session,
    :session_options,
    :exec_module,
    :exec_opts,
    :exec_handle,
    :exec_monitor,
    :cli_path,
    :buffer,
    :current_request,
    :api_key,
    :server_info,
    :hook_registry,
    :hooks_wire,
    :session_id,
    :cwd,
    status: :provisioning,
    control_counter: 0,
    control_timeout: @default_control_timeout,
    pending_control_requests: %{},
    deferred_exec_messages: [],
    max_buffer_size: 1_048_576,
    sdk_mcp_servers: %{}
  ]

  # ============================================================================
  # Client API (Adapter behaviour)
  # ============================================================================

  @impl ClaudeCode.Adapter
  def start_link(session, opts) do
    GenServer.start_link(__MODULE__, {session, opts})
  end

  @impl ClaudeCode.Adapter
  def send_query(adapter, request_id, prompt, opts) do
    GenServer.call(adapter, {:query, request_id, prompt, opts}, :infinity)
  end

  @impl ClaudeCode.Adapter
  def health(adapter) do
    GenServer.call(adapter, :health)
  end

  @impl ClaudeCode.Adapter
  def stop(adapter) do
    GenServer.stop(adapter, :normal)
  end

  @impl ClaudeCode.Adapter
  def send_control_request(adapter, subtype, params) do
    GenServer.call(adapter, {:control_request, subtype, params}, :infinity)
  end

  @impl ClaudeCode.Adapter
  def get_server_info(adapter) do
    GenServer.call(adapter, :get_server_info)
  end

  @impl ClaudeCode.Adapter
  def interrupt(adapter) do
    GenServer.call(adapter, :interrupt)
  end

  @impl ClaudeCode.Adapter
  def execute(adapter, m, f, a) do
    GenServer.call(adapter, {:execute, m, f, a})
  end

  # ============================================================================
  # GenServer callbacks
  # ============================================================================

  @impl GenServer
  def init({session, opts}) do
    hooks_map = Keyword.get(opts, :hooks)
    can_use_tool = Keyword.get(opts, :can_use_tool)
    {built_registry, hooks_wire} = HookRegistry.new(hooks_map, can_use_tool)

    hook_registry =
      case Keyword.get(opts, :hook_registry) do
        %HookRegistry{} = registry -> registry
        nil -> built_registry
      end

    {exec_module, exec_opts} = Keyword.get(opts, :exec, {AgentHarness.Exec.Local, []})

    sdk_mcp_servers =
      case Keyword.get(opts, :sdk_mcp_servers) do
        pre when is_map(pre) and map_size(pre) > 0 -> pre
        _absent -> PortAdapter.extract_sdk_mcp_servers(opts)
      end

    state = %__MODULE__{
      session: session,
      session_options: Keyword.drop(opts, @adapter_internal_keys),
      exec_module: exec_module,
      exec_opts: exec_opts,
      cli_path: normalize_cli_path(Keyword.get(opts, :cli_path, @default_cli_path)),
      buffer: "",
      api_key: Keyword.get(opts, :api_key),
      max_buffer_size: Keyword.get(opts, :max_buffer_size, 1_048_576),
      control_timeout: Keyword.get(opts, :control_timeout, @default_control_timeout),
      hook_registry: hook_registry,
      hooks_wire: hooks_wire,
      sdk_mcp_servers: sdk_mcp_servers,
      cwd: Keyword.get(opts, :cwd)
    }

    Process.link(session)
    Adapter.notify_status(session, :provisioning)

    {:ok, state, {:continue, :connect}}
  end

  @impl GenServer
  def handle_continue(:connect, state) do
    # Exec start can involve network round trips for remote environments;
    # run it in a task so the adapter stays responsive, mirroring the Port
    # adapter's async binary resolution.
    adapter = self()
    spec = build_spec(state)
    exec_module = state.exec_module
    exec_opts = state.exec_opts

    Task.start_link(fn ->
      send(adapter, {:exec_started, safe_exec_start(exec_module, spec, adapter, exec_opts)})
    end)

    {:noreply, state}
  end

  @impl GenServer
  def handle_call({:query, request_id, prompt, opts}, _from, %{status: :ready} = state) do
    session_id = Keyword.get(opts, :session_id, "default")
    message = Input.user_message(prompt, session_id)

    case push(state, message) do
      :ok -> {:reply, :ok, %{state | current_request: request_id}}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:query, _request_id, _prompt, _opts}, _from, state) do
    reason =
      case state.status do
        :provisioning -> :provisioning
        :initializing -> :initializing
        _other -> :disconnected
      end

    {:reply, {:error, reason}, state}
  end

  def handle_call(:health, _from, state) do
    health =
      case state do
        %{status: :provisioning} -> {:unhealthy, :provisioning}
        %{exec_handle: handle} when not is_nil(handle) -> :healthy
        _disconnected -> {:unhealthy, :not_connected}
      end

    {:reply, health, state}
  end

  def handle_call({:control_request, _subtype, _params}, _from, %{exec_handle: nil} = state) do
    {:reply, {:error, :not_connected}, state}
  end

  def handle_call({:control_request, subtype, params}, from, state) do
    {request_id, new_counter} = next_request_id(state.control_counter)

    case build_control_json(subtype, request_id, params) do
      {:error, _reason} = error ->
        {:reply, error, state}

      json ->
        send_outbound_control(state, from, subtype, request_id, new_counter, json)
    end
  end

  def handle_call(:get_server_info, _from, state) do
    {:reply, {:ok, state.server_info}, state}
  end

  def handle_call(:interrupt, _from, %{exec_handle: nil} = state) do
    {:reply, {:error, :not_connected}, state}
  end

  def handle_call(:interrupt, _from, state) do
    {request_id, new_counter} = next_request_id(state.control_counter)
    result = push(state, Control.interrupt_request(request_id))
    {:reply, result, %{state | control_counter: new_counter}}
  end

  def handle_call({:execute, m, f, a}, _from, state) do
    {:reply, apply(m, f, a), state}
  end

  defp send_outbound_control(state, from, subtype, request_id, new_counter, json) do
    case push(state, json) do
      :ok ->
        pending = Map.put(state.pending_control_requests, request_id, {subtype, from})
        schedule_control_timeout(state.control_timeout, request_id)

        {:noreply, %{state | control_counter: new_counter, pending_control_requests: pending}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_info({:exec_started, {:ok, handle}}, state) do
    state = %{
      state
      | exec_handle: handle,
        exec_monitor: monitor_handle(handle),
        buffer: "",
        status: :initializing
    }

    {:noreply, state} = send_initialize_handshake(state)
    drain_deferred_exec_messages(state)
  end

  def handle_info({:exec_started, {:error, reason}}, state) do
    Adapter.notify_status(state.session, {:error, {:exec_start_failed, reason}})
    {:noreply, %{state | status: :disconnected, deferred_exec_messages: []}}
  end

  def handle_info({:exec_data, handle, data}, %{exec_handle: handle} = state) do
    {:noreply, process_exec_data(state, data)}
  end

  def handle_info({:exec_exit, handle, reason}, %{exec_handle: handle} = state) do
    {:noreply, handle_disconnect(state, {:exec_exit, reason})}
  end

  # Remote exec implementations can deliver output from a connection process
  # before the start call's return value reaches this adapter. Hold those
  # messages until {:exec_started, ...} assigns the handle, then replay them.
  def handle_info({tag, _handle, _payload} = message, %{status: :provisioning} = state)
      when tag in [:exec_data, :exec_exit] do
    {:noreply, %{state | deferred_exec_messages: [message | state.deferred_exec_messages]}}
  end

  def handle_info({:exec_data, _stale_handle, _data}, state), do: {:noreply, state}
  def handle_info({:exec_exit, _stale_handle, _reason}, state), do: {:noreply, state}

  def handle_info({:DOWN, monitor, :process, _pid, reason}, %{exec_monitor: monitor} = state) do
    {:noreply, handle_disconnect(%{state | exec_monitor: nil}, {:exec_down, reason})}
  end

  def handle_info({:control_timeout, request_id}, state) do
    case Map.pop(state.pending_control_requests, request_id) do
      {nil, _pending} ->
        {:noreply, state}

      {{:initialize, session}, remaining} ->
        Adapter.notify_status(session, {:error, :initialize_timeout})
        {:noreply, %{state | pending_control_requests: remaining, status: :disconnected}}

      {{_subtype, from}, remaining} ->
        GenServer.reply(from, {:error, :control_timeout})
        {:noreply, %{state | pending_control_requests: remaining}}
    end
  end

  def handle_info(message, state) do
    Logger.debug("Claude exec adapter unhandled message: #{inspect(message)}")
    {:noreply, state}
  end

  @impl GenServer
  def terminate(_reason, state) do
    if state.exec_handle do
      # Best-effort in-band interrupt so the CLI stops consuming tokens
      # before the process is torn down, mirroring the Port adapter.
      {request_id, _counter} = next_request_id(state.control_counter)
      _ = safe_push(state, Control.interrupt_request(request_id))
      safe_exec_kill(state)
    end

    :ok
  end

  defp safe_push(state, json) do
    push(state, json)
  rescue
    _error -> {:error, :push_failed}
  catch
    _kind, _reason -> {:error, :push_failed}
  end

  # ============================================================================
  # Spec building
  # ============================================================================

  defp build_spec(state) do
    streaming_opts = Keyword.put(state.session_options, :input_format, :stream_json)
    resume_session_id = Keyword.get(state.session_options, :resume)

    args =
      ""
      |> Command.build_args(streaming_opts, resume_session_id)
      |> List.delete_at(-1)

    %{
      cmd: [state.cli_path | args],
      env: build_env(state),
      cwd: state.cwd,
      stderr: :merged
    }
  end

  # Unlike the Port adapter, the orchestrator's System.get_env/0 is not
  # explicitly forwarded: only the entries below ride along, layered over
  # whatever environment the execution environment already has. For
  # Exec.Local that base is this VM's environment, because the command runs
  # here; remote implementations start from the sandbox's environment.
  defp build_env(state) do
    user_env =
      state.session_options
      |> Keyword.get(:env, %{})
      |> Map.new(fn {key, value} -> {to_string(key), value} end)

    PortAdapter.sdk_env_vars()
    # The Port adapter strips CLAUDECODE from the forwarded environment so
    # a CLI child never sees nested-session detection; unset it explicitly
    # here for the same reason.
    |> Map.put("CLAUDECODE", false)
    |> maybe_put_file_checkpointing(state.session_options)
    |> Map.merge(user_env)
    |> maybe_put_api_key(state.api_key)
  end

  # This option has no CLI flag; the env var is its only transport.
  defp maybe_put_file_checkpointing(env, session_options) do
    if Keyword.get(session_options, :enable_file_checkpointing, false) do
      Map.put(env, "CLAUDE_CODE_ENABLE_SDK_FILE_CHECKPOINTING", "true")
    else
      env
    end
  end

  defp maybe_put_api_key(env, api_key) when is_binary(api_key) do
    Map.put(env, "ANTHROPIC_API_KEY", api_key)
  end

  defp maybe_put_api_key(env, _absent), do: env

  # The Port adapter's Resolver accepts sentinel atoms such as :global for
  # local binary discovery. Executable resolution belongs to the execution
  # environment here, so anything but an explicit path falls back to the
  # default executable name.
  defp normalize_cli_path(path) when is_binary(path), do: path
  defp normalize_cli_path(_sentinel), do: @default_cli_path

  # This state carries api_key and the session env; scrub the raw term so
  # adapter crash reports and :sys.get_status/1 cannot leak credentials.
  @impl GenServer
  def format_status(%{state: %__MODULE__{} = state} = status) do
    %{status | state: redact_state(state)}
  end

  def format_status(status), do: status

  @doc false
  def redact_state(%__MODULE__{} = state) do
    %{
      state
      | api_key: redact_value(state.api_key),
        session_options: redact_session_options(state.session_options)
    }
  end

  defp redact_value(nil), do: nil
  defp redact_value(_value), do: "[REDACTED]"

  defp redact_session_options(options) when is_list(options) do
    Enum.map(options, fn
      {:api_key, value} -> {:api_key, redact_value(value)}
      {:env, env} when is_map(env) -> {:env, Map.new(env, fn {key, _} -> {key, "[REDACTED]"} end)}
      other -> other
    end)
  end

  defp redact_session_options(options), do: options

  defp safe_exec_start(exec_module, spec, owner, exec_opts) do
    exec_module.start(spec, owner, exec_opts)
  rescue
    error -> {:error, {:exception, error.__struct__, Exception.message(error)}}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  # ============================================================================
  # Protocol handling (mirrors ClaudeCode.Adapter.Port)
  # ============================================================================

  defp push(state, json) do
    state.exec_module.write(state.exec_handle, [json, "\n"])
  end

  defp send_initialize_handshake(state) do
    agents = Keyword.get(state.session_options, :agents)

    sdk_mcp_server_names =
      case Map.keys(state.sdk_mcp_servers) do
        [] -> nil
        names -> names
      end

    extra_opts =
      []
      |> maybe_add_opt(state.session_options, :prompt_suggestions)
      |> maybe_add_opt(state.session_options, :tool_config)

    {request_id, new_counter} = next_request_id(state.control_counter)

    json =
      Control.initialize_request(
        request_id,
        state.hooks_wire,
        agents,
        sdk_mcp_server_names,
        extra_opts
      )

    case push(state, json) do
      :ok ->
        pending =
          Map.put(state.pending_control_requests, request_id, {:initialize, state.session})

        schedule_control_timeout(state.control_timeout, request_id)

        {:noreply, %{state | control_counter: new_counter, pending_control_requests: pending}}

      {:error, reason} ->
        Adapter.notify_status(state.session, {:error, {:initialize_write_failed, reason}})
        {:noreply, %{state | status: :disconnected}}
    end
  end

  defp process_exec_data(state, data) do
    new_buffer = state.buffer <> data
    {lines, remaining_buffer} = PortAdapter.extract_lines(new_buffer)

    new_state =
      Enum.reduce(lines, %{state | buffer: remaining_buffer}, fn line, acc_state ->
        process_line(line, acc_state)
      end)

    if byte_size(new_state.buffer) > new_state.max_buffer_size do
      Logger.error(
        "Exec adapter buffer overflow: incomplete line is " <>
          "#{byte_size(new_state.buffer)} bytes, exceeds max #{new_state.max_buffer_size}"
      )

      handle_disconnect(new_state, {:buffer_overflow, byte_size(new_state.buffer)})
    else
      new_state
    end
  end

  defp drain_deferred_exec_messages(%{deferred_exec_messages: []} = state) do
    {:noreply, state}
  end

  defp drain_deferred_exec_messages(state) do
    messages = Enum.reverse(state.deferred_exec_messages)
    state = %{state | deferred_exec_messages: []}

    {:noreply, Enum.reduce(messages, state, &apply_deferred_exec_message(&2, &1))}
  end

  defp apply_deferred_exec_message(
         %{exec_handle: handle} = state,
         {:exec_data, handle, data}
       )
       when not is_nil(handle) do
    process_exec_data(state, data)
  end

  defp apply_deferred_exec_message(
         %{exec_handle: handle} = state,
         {:exec_exit, handle, reason}
       )
       when not is_nil(handle) do
    handle_disconnect(state, {:exec_exit, reason})
  end

  defp apply_deferred_exec_message(state, _stale_message), do: state

  defp monitor_handle(handle) when is_pid(handle), do: Process.monitor(handle)
  defp monitor_handle(_handle), do: nil

  defp handle_disconnect(state, error) do
    for {_request_id, pending} <- state.pending_control_requests do
      case pending do
        {:initialize, session} -> Adapter.notify_status(session, {:error, error})
        {_subtype, from} -> GenServer.reply(from, {:error, error})
      end
    end

    if state.current_request do
      Adapter.notify_error(state.session, state.current_request, error)
    end

    # For disconnects the exec did not report itself (buffer overflow, a
    # monitored handle dying), the command may still be running and
    # billing. kill/1 is idempotent, so an already-dead exec is fine.
    if state.exec_handle, do: safe_exec_kill(state)
    if state.exec_monitor, do: Process.demonitor(state.exec_monitor, [:flush])

    %{
      state
      | exec_handle: nil,
        exec_monitor: nil,
        current_request: nil,
        buffer: "",
        status: :disconnected,
        pending_control_requests: %{}
    }
  end

  defp safe_exec_kill(state) do
    state.exec_module.kill(state.exec_handle)
  rescue
    _error -> :ok
  catch
    _kind, _reason -> :ok
  end

  defp process_line("", state), do: state

  defp process_line(line, state) do
    case JSON.decode(line) do
      {:ok, json} when is_map(json) ->
        case Control.classify(json) do
          {:control_response, msg} -> handle_control_response(msg, state)
          {:control_request, msg} -> handle_inbound_control_request(msg, state)
          {:control_cancel, msg} -> handle_control_cancel(msg, state)
          {:message, json_msg} -> handle_sdk_message(json_msg, state)
        end

      {:ok, non_map} ->
        Logger.warning("Dropping non-map CLI output: #{inspect(non_map)}")
        state

      {:error, _reason} ->
        Logger.debug("Non-JSON CLI output: #{String.slice(line, 0, 500)}")
        state
    end
  end

  defp handle_sdk_message(json, %{current_request: nil} = state) do
    maybe_capture_session_id(json, state)
  end

  defp handle_sdk_message(json, state) do
    Adapter.notify_message(state.session, state.current_request, json)

    state = maybe_capture_session_id(json, state)

    if json["type"] == "result" do
      %{state | current_request: nil}
    else
      state
    end
  end

  defp maybe_capture_session_id(
         %{"type" => "system", "session_id" => sid},
         %{session_id: current} = state
       )
       when is_binary(sid) and sid != current do
    %{state | session_id: sid}
  end

  defp maybe_capture_session_id(_json, state), do: state

  defp handle_control_response(msg, state) do
    {request_id, result} =
      case Control.parse_control_response(msg) do
        {:ok, id, response} -> {id, {:ok, Parser.normalize_keys(response)}}
        {:error, id, error_msg} -> {id, {:error, error_msg}}
      end

    case Map.pop(state.pending_control_requests, request_id) do
      {nil, _pending} ->
        Logger.warning("Received control response for unknown request: #{request_id}")
        state

      {{:initialize, session}, remaining} ->
        complete_initialize(result, session, %{state | pending_control_requests: remaining})

      {{subtype, from}, remaining} ->
        reply = with {:ok, response} <- result, do: {:ok, parse_control_result(subtype, response)}
        GenServer.reply(from, reply)
        %{state | pending_control_requests: remaining}
    end
  end

  defp complete_initialize({:ok, response}, session, state) do
    Adapter.notify_status(session, :ready)
    %{state | server_info: parse_initialize_response(response), status: :ready}
  end

  defp complete_initialize({:error, error_msg}, session, state) do
    Adapter.notify_status(session, {:error, {:initialize_failed, error_msg}})
    %{state | status: :disconnected}
  end

  defp handle_control_cancel(%{"request_id" => cancel_id}, state) do
    case Map.pop(state.pending_control_requests, cancel_id) do
      {nil, _pending} ->
        state

      {{:initialize, session}, remaining} ->
        Adapter.notify_status(session, {:error, :cancelled})
        %{state | pending_control_requests: remaining}

      {{_subtype, from}, remaining} ->
        GenServer.reply(from, {:error, :cancelled})
        %{state | pending_control_requests: remaining}
    end
  end

  defp handle_inbound_control_request(msg, state) do
    request_id = get_in(msg, ["request_id"])
    request = get_in(msg, ["request"])
    subtype = get_in(request, ["subtype"])

    result = dispatch_control_request(subtype, request, msg, state)

    send_control_result(request_id, result, state)
  end

  defp dispatch_control_request("hook_callback", request, _msg, state) do
    ControlHandler.handle_hook_callback(request, state.hook_registry)
  end

  defp dispatch_control_request("mcp_message", request, _msg, state) do
    server_name = request["server_name"]
    jsonrpc = request["message"]
    {:ok, ControlHandler.handle_mcp_message(server_name, jsonrpc, state.sdk_mcp_servers)}
  end

  defp dispatch_control_request("can_use_tool", request, _msg, state) do
    {:ok,
     ControlHandler.handle_can_use_tool(request, state.hook_registry, session_context(state))}
  end

  defp dispatch_control_request(subtype, _request, _msg, _state) do
    Logger.warning("Received unhandled control request: #{subtype}")
    {:error, "Not implemented: #{subtype}"}
  end

  defp send_control_result(request_id, result, state) do
    response =
      case result do
        {:ok, data} -> Control.success_response(request_id, data)
        {:error, reason} -> Control.error_response(request_id, reason)
      end

    if state.exec_handle, do: _ = push(state, response)
    state
  end

  defp session_context(state) do
    %{cwd: state.cwd, session_id: state.session_id}
  end

  defp next_request_id(counter) do
    {Control.generate_request_id(counter), counter + 1}
  end

  defp maybe_add_opt(acc, opts, key) do
    case Keyword.get(opts, key) do
      nil -> acc
      value -> Keyword.put(acc, key, value)
    end
  end

  defp parse_control_result(:mcp_status, %{"mcp_servers" => servers}) when is_list(servers) do
    Enum.map(servers, &MCPStatus.new/1)
  end

  defp parse_control_result(:set_mcp_servers, response) when is_map(response) do
    %{
      added: response["added"] || [],
      removed: response["removed"] || [],
      errors: response["errors"] || %{}
    }
  end

  defp parse_control_result(:rewind_files, response) when is_map(response) do
    %{
      can_rewind: response["can_rewind"],
      error: response["error"],
      files_changed: response["files_changed"],
      insertions: response["insertions"],
      deletions: response["deletions"]
    }
  end

  defp parse_control_result(_subtype, response), do: response

  defp parse_initialize_response(response) when is_map(response) do
    %{
      commands: parse_list(response["commands"], &SlashCommand.new/1),
      agents: parse_list(response["agents"], &AgentInfo.new/1),
      models: parse_list(response["models"], &Model.Info.new/1),
      account: parse_optional(response["account"], &AccountInfo.new/1),
      output_style: response["output_style"],
      available_output_styles: response["available_output_styles"] || [],
      fast_mode_state: response["fast_mode_state"]
    }
  end

  defp parse_list(nil, _parser), do: []
  defp parse_list(list, parser) when is_list(list), do: Enum.map(list, parser)

  defp parse_optional(nil, _parser), do: nil
  defp parse_optional(map, parser) when is_map(map), do: parser.(map)

  defp build_control_json(:initialize, request_id, params) do
    hooks = Map.get(params, :hooks)
    agents = Map.get(params, :agents)
    sdk_mcp_servers = Map.get(params, :sdk_mcp_servers)
    extra_opts = Map.get(params, :extra_opts, [])
    Control.initialize_request(request_id, hooks, agents, sdk_mcp_servers, extra_opts)
  end

  defp build_control_json(:set_model, request_id, %{model: model}) do
    Control.set_model_request(request_id, model)
  end

  defp build_control_json(:set_permission_mode, request_id, %{mode: mode}) do
    Control.set_permission_mode_request(request_id, to_string(mode))
  end

  defp build_control_json(:rewind_files, request_id, %{user_message_id: id} = params) do
    opts = if params[:dry_run], do: [dry_run: true], else: []
    Control.rewind_files_request(request_id, id, opts)
  end

  defp build_control_json(:mcp_status, request_id, _params) do
    Control.mcp_status_request(request_id)
  end

  defp build_control_json(:mcp_reconnect, request_id, %{server_name: name}) do
    Control.mcp_reconnect_request(request_id, name)
  end

  defp build_control_json(:mcp_toggle, request_id, %{server_name: name, enabled: enabled}) do
    Control.mcp_toggle_request(request_id, name, enabled)
  end

  defp build_control_json(:set_mcp_servers, request_id, %{servers: servers}) do
    Control.mcp_set_servers_request(request_id, servers)
  end

  defp build_control_json(:stop_task, request_id, %{task_id: task_id}) do
    Control.stop_task_request(request_id, task_id)
  end

  defp build_control_json(subtype, _request_id, _params) do
    {:error, {:unknown_control_subtype, subtype}}
  end

  defp schedule_control_timeout(timeout, request_id) do
    Process.send_after(self(), {:control_timeout, request_id}, timeout)
  end
end

defimpl Inspect, for: AgentHarness.Providers.Claude.Adapter.Exec do
  # Inspect.Any is the derived-struct fallback; calling it directly renders
  # the struct without re-dispatching this protocol, so no recursion. It is
  # internal API whose implementation shifted in Elixir 1.19, so revisit on
  # Elixir upgrades.
  def inspect(state, opts) do
    state
    |> AgentHarness.Providers.Claude.Adapter.Exec.redact_state()
    |> Inspect.Any.inspect(opts)
  end
end
