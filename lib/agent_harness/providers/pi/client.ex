defmodule AgentHarness.Providers.Pi.Client do
  @moduledoc false

  @type handle :: pid()

  @doc """
  Starts a transport that owns one `pi --mode rpc` process.

  The transport sends `{:pi_frame, handle, decoded_frame}` for every JSONL
  record pi writes, and `{:pi_down, handle, reason}` once when the process ends
  or the transport fails. It monitors `owner` and shuts down with it.
  """
  @callback open(prepared :: map(), owner :: pid()) :: {:ok, handle()} | {:error, term()}

  @callback send_frame(handle(), map()) :: :ok | {:error, term()}

  @callback verify_subscription_auth(prepared :: map(), timeout()) :: :ok | {:error, term()}

  @callback close(handle()) :: :ok
end

defmodule AgentHarness.Providers.Pi.Client.Port do
  @moduledoc false

  use GenServer

  @behaviour AgentHarness.Providers.Pi.Client

  alias AgentHarness.Providers.Pi.Framing

  # Pi writes a bare error line to stderr and exits when startup fails (a bad
  # --session id, an unreadable extension). stderr is not merged into stdout,
  # which would corrupt the JSONL stream, so a startup failure surfaces as an
  # exit status rather than a frame.

  @impl AgentHarness.Providers.Pi.Client
  def open(prepared, owner) when is_pid(owner) do
    case GenServer.start(__MODULE__, {prepared, owner}) do
      {:ok, pid} -> {:ok, pid}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl AgentHarness.Providers.Pi.Client
  def send_frame(pid, frame) when is_map(frame) do
    GenServer.call(pid, {:send_frame, frame})
  catch
    :exit, reason -> {:error, {:transport_call_failed, reason}}
  end

  @impl AgentHarness.Providers.Pi.Client
  def close(pid) do
    GenServer.stop(pid, :normal)
  catch
    :exit, _reason -> :ok
  end

  @impl AgentHarness.Providers.Pi.Client
  def verify_subscription_auth(prepared, timeout) do
    with {:ok, executable} <- resolve_executable(prepared) do
      run_auth_check(executable, prepared, timeout)
    end
  end

  @impl GenServer
  def init({prepared, owner}) do
    Process.flag(:trap_exit, true)

    case resolve_executable(prepared) do
      {:ok, executable} ->
        port = Port.open({:spawn_executable, executable}, port_options(prepared))

        {:ok,
         %{
           port: port,
           owner: owner,
           owner_monitor: Process.monitor(owner),
           buffer: ""
         }}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl GenServer
  def handle_call({:send_frame, frame}, _from, %{port: port} = state) when not is_nil(port) do
    Port.command(port, [JSON.encode!(frame), "\n"])
    {:reply, :ok, state}
  rescue
    ArgumentError -> {:reply, {:error, :transport_closed}, state}
    error -> {:reply, {:error, {:invalid_frame, Exception.message(error)}}, state}
  end

  def handle_call({:send_frame, _frame}, _from, state) do
    {:reply, {:error, :transport_closed}, state}
  end

  @impl GenServer
  def handle_info({port, {:data, chunk}}, %{port: port} = state) do
    {frames, buffer} = Framing.decode(state.buffer, chunk)
    Enum.each(frames, &forward(state, &1))
    {:noreply, %{state | buffer: buffer}}
  end

  def handle_info({port, {:exit_status, status}}, %{port: port} = state) do
    send(state.owner, {:pi_down, self(), {:exit_status, status}})
    {:stop, :normal, %{state | port: nil}}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{owner_monitor: ref} = state) do
    {:stop, :normal, state}
  end

  def handle_info({:EXIT, port, reason}, %{port: port} = state) do
    send(state.owner, {:pi_down, self(), {:port_exit, reason}})
    {:stop, :normal, %{state | port: nil}}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl GenServer
  def terminate(_reason, %{port: port}) when not is_nil(port) do
    Port.close(port)
    :ok
  rescue
    ArgumentError -> :ok
  end

  def terminate(_reason, _state), do: :ok

  defp forward(state, json) do
    case JSON.decode(json) do
      {:ok, frame} when is_map(frame) ->
        send(state.owner, {:pi_frame, self(), frame})

      _other ->
        send(state.owner, {:pi_frame, self(), %{"type" => "__undecodable__", "raw" => json}})
    end
  end

  defp port_options(prepared) do
    [
      :binary,
      :exit_status,
      :use_stdio,
      :hide,
      {:args, prepared.args},
      {:env, prepared.env}
    ]
    |> put_cd(prepared.cwd)
  end

  defp put_cd(options, nil), do: options
  defp put_cd(options, cwd), do: [{:cd, cwd} | options]

  defp resolve_executable(%{executable: executable}) do
    cond do
      String.contains?(executable, "/") and File.exists?(executable) ->
        {:ok, executable}

      path = System.find_executable(executable) ->
        {:ok, path}

      true ->
        {:error, {:cli_not_found, executable}}
    end
  end

  # `pi auth print-bearer-token` fails unless the selected provider holds an
  # OAuth credential from `pi /login`. Its stdout is the bearer token itself,
  # so only the exit status and the leading error marker are inspected; the
  # output is never returned or logged.
  defp run_auth_check(executable, prepared, timeout) do
    args =
      ["auth", "print-bearer-token"] ++
        provider_flag(prepared) ++ model_flag(prepared)

    task =
      Task.Supervisor.async_nolink(AgentHarness.RunnerSupervisor, fn ->
        System.cmd(executable, args,
          stderr_to_stdout: true,
          env: Enum.map(prepared.env, fn {k, v} -> {List.to_string(k), List.to_string(v)} end)
        )
      end)

    case Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, {output, 0}} -> verify_auth_output(output)
      {:ok, {_output, status}} -> {:error, {:subscription_auth_required, {:exit_status, status}}}
      {:exit, reason} -> {:error, {:subscription_auth_check_failed, reason}}
      nil -> {:error, :subscription_auth_check_timeout}
    end
  catch
    :exit, reason -> {:error, {:subscription_auth_check_failed, reason}}
  end

  defp verify_auth_output(output) do
    if output |> String.trim_leading() |> String.starts_with?("Error:") do
      {:error, {:subscription_auth_required, :no_oauth_credential}}
    else
      :ok
    end
  end

  defp provider_flag(%{provider: provider}) when is_binary(provider), do: ["--provider", provider]
  defp provider_flag(_prepared), do: []

  defp model_flag(%{model: model}) when is_binary(model), do: ["--model", model]
  defp model_flag(_prepared), do: []
end
