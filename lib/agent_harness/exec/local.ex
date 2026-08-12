defmodule AgentHarness.Exec.Local do
  @moduledoc """
  `AgentHarness.Exec` implementation that runs the command as a local OS
  process through an Erlang port.

  This is the default execution environment and preserves the library's
  local-first behavior: the executable resolves against the local `PATH`,
  and `spec.cwd`/`spec.env` apply to the local process.
  """

  @behaviour AgentHarness.Exec

  use GenServer

  @impl AgentHarness.Exec
  def start(spec, owner, _opts) when is_map(spec) and is_pid(owner) do
    with {:ok, executable} <- resolve_executable(spec.cmd) do
      GenServer.start(__MODULE__, {spec, executable, owner})
    end
  end

  @impl AgentHarness.Exec
  def write(handle, data) when is_pid(handle) do
    GenServer.call(handle, {:write, data})
  catch
    :exit, _reason -> {:error, :closed}
  end

  @impl AgentHarness.Exec
  def kill(handle) when is_pid(handle) do
    GenServer.call(handle, :kill)
  catch
    :exit, {:timeout, _call} ->
      # The exec process is wedged; :kill is untrappable and the port dies
      # with it. The OS process then sees EOF, which is the best available
      # outcome once the GenServer cannot run its own cleanup.
      Process.exit(handle, :kill)
      :ok

    :exit, _reason ->
      :ok
  end

  @impl GenServer
  def init({spec, executable, owner}) do
    Process.flag(:trap_exit, true)

    port = Port.open({:spawn_executable, executable}, port_options(spec))

    {:ok,
     %{
       port: port,
       owner: owner,
       owner_monitor: Process.monitor(owner),
       exited?: false
     }}
  end

  @impl GenServer
  def handle_call({:write, data}, _from, %{port: port} = state) when not is_nil(port) do
    # :nosuspend keeps a busy port (the command stopped draining stdin) from
    # suspending this process, which would wedge every later write and kill.
    if Port.command(port, data, [:nosuspend]) do
      {:reply, :ok, state}
    else
      {:reply, {:error, :busy}, state}
    end
  rescue
    ArgumentError -> {:reply, {:error, :closed}, state}
  end

  def handle_call({:write, _data}, _from, state) do
    {:reply, {:error, :closed}, state}
  end

  def handle_call(:kill, _from, state) do
    state = close_port(state)
    state = deliver_exit(state, :killed)
    {:stop, :normal, :ok, state}
  end

  @impl GenServer
  def handle_info({port, {:data, chunk}}, %{port: port} = state) do
    send(state.owner, {:exec_data, self(), chunk})
    {:noreply, state}
  end

  def handle_info({port, {:exit_status, status}}, %{port: port} = state) do
    state = deliver_exit(state, {:exit_status, status})
    {:stop, :normal, %{state | port: nil}}
  end

  def handle_info({:EXIT, port, reason}, %{port: port} = state) do
    state = deliver_exit(state, {:port_exit, reason})
    {:stop, :normal, %{state | port: nil}}
  end

  def handle_info({:DOWN, monitor, :process, _pid, _reason}, %{owner_monitor: monitor} = state) do
    {:stop, :normal, close_port(state)}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl GenServer
  def terminate(_reason, state) do
    close_port(state)
    :ok
  end

  defp deliver_exit(%{exited?: true} = state, _reason), do: state

  defp deliver_exit(state, reason) do
    send(state.owner, {:exec_exit, self(), reason})
    %{state | exited?: true}
  end

  defp close_port(%{port: nil} = state), do: state

  # Port.close/1 only closes the pipes; a command that ignores stdin EOF
  # would keep running. Kill the OS process so close_port means stopped.
  defp close_port(%{port: port} = state) do
    os_pid =
      case Port.info(port, :os_pid) do
        {:os_pid, pid} -> pid
        _closed -> nil
      end

    try do
      Port.close(port)
    rescue
      ArgumentError -> :ok
    end

    kill_os_process(os_pid)
    %{state | port: nil}
  end

  defp kill_os_process(nil), do: :ok

  defp kill_os_process(os_pid) do
    case :os.type() do
      {:unix, _flavor} ->
        _ = System.cmd("kill", ["-KILL", Integer.to_string(os_pid)], stderr_to_stdout: true)
        :ok

      _other ->
        :ok
    end
  rescue
    # kill(1) missing from PATH is not worth crashing cleanup over.
    ErlangError -> :ok
  end

  defp port_options(spec) do
    [_executable | args] = spec.cmd

    [
      :binary,
      :exit_status,
      :use_stdio,
      :hide,
      {:args, args},
      {:env, env_entries(spec.env)}
    ]
    |> put_stderr(Map.get(spec, :stderr, :passthrough))
    |> put_cd(spec.cwd)
  end

  defp env_entries(env) do
    Enum.map(env, fn
      {key, false} -> {to_charlist_key(key), false}
      {key, value} -> {to_charlist_key(key), String.to_charlist(to_string(value))}
    end)
  end

  defp to_charlist_key(key), do: key |> to_string() |> String.to_charlist()

  defp put_stderr(options, :merged), do: [:stderr_to_stdout | options]
  defp put_stderr(options, :passthrough), do: options

  defp put_cd(options, nil), do: options
  defp put_cd(options, cwd), do: [{:cd, String.to_charlist(cwd)} | options]

  defp resolve_executable([executable | _args]) do
    cond do
      String.contains?(executable, "/") and File.exists?(executable) ->
        {:ok, executable}

      path = System.find_executable(executable) ->
        {:ok, path}

      true ->
        {:error, {:executable_not_found, executable}}
    end
  end
end
