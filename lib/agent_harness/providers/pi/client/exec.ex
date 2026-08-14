defmodule AgentHarness.Providers.Pi.Client.Exec do
  @moduledoc """
  `AgentHarness.Providers.Pi.Client` that runs the pi CLI through an
  `AgentHarness.Exec` implementation instead of a local port.

  The pi RPC protocol — JSONL framing, prompts, dialogs — stays on the
  orchestrator; the `pi --mode rpc` process itself runs wherever the
  configured exec module puts it: locally through `AgentHarness.Exec.Local`,
  or in a remote sandbox through an exec implementation provided by the
  application.

  Select it per session with `auth: :inherit` (the fail-closed
  `:subscription` mode depends on local `pi /login` state by design):

      provider_options: %{
        auth: :inherit,
        exec: {MyApp.SandboxExec, sandbox: sandbox}
      }

  ## Differences from `Client.Port`

    * The executable resolves in the execution environment, not against the
      orchestrator's `PATH`.
    * `cwd` and `env` are interpreted where the command runs; nothing in the
      spawn spec refers to the orchestrator's filesystem.
    * stderr stays out of the data stream (`:passthrough`), as merging it
      would corrupt pi's JSONL protocol. Where it surfaces is up to the exec
      implementation.
    * `verify_subscription_auth/2` is refused: subscription auth inspects
      local credential state that has no meaning for a remote process.
  """

  use GenServer

  @behaviour AgentHarness.Providers.Pi.Client

  alias AgentHarness.Providers.Pi.Framing

  @default_exec {AgentHarness.Exec.Local, []}

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
  def verify_subscription_auth(_prepared, _timeout) do
    {:error, {:subscription_auth_unsupported, :exec_client}}
  end

  @impl GenServer
  def init({prepared, owner}) do
    {exec_module, exec_opts} = Map.get(prepared, :exec) || @default_exec

    case exec_module.start(spec(prepared), self(), exec_opts) do
      {:ok, handle} ->
        {:ok,
         %{
           exec_module: exec_module,
           handle: handle,
           owner: owner,
           owner_monitor: Process.monitor(owner),
           buffer: ""
         }}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl GenServer
  def handle_call({:send_frame, _frame}, _from, %{handle: nil} = state) do
    {:reply, {:error, :transport_closed}, state}
  end

  def handle_call({:send_frame, frame}, _from, state) do
    reply =
      case state.exec_module.write(state.handle, [JSON.encode!(frame), "\n"]) do
        :ok -> :ok
        {:error, :closed} -> {:error, :transport_closed}
        {:error, reason} -> {:error, reason}
      end

    {:reply, reply, state}
  rescue
    error -> {:reply, {:error, {:invalid_frame, Exception.message(error)}}, state}
  end

  @impl GenServer
  def handle_info({:exec_data, handle, chunk}, %{handle: handle} = state) do
    {frames, buffer} = Framing.decode(state.buffer, chunk)
    Enum.each(frames, &forward(state, &1))
    {:noreply, %{state | buffer: buffer}}
  end

  def handle_info({:exec_exit, handle, reason}, %{handle: handle} = state) do
    send(state.owner, {:pi_down, self(), reason})
    {:stop, :normal, %{state | handle: nil}}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{owner_monitor: ref} = state) do
    {:stop, :normal, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl GenServer
  def terminate(_reason, %{handle: handle} = state) when not is_nil(handle) do
    # kill/1 is idempotent and force-stops the command wherever it runs; an
    # in-band shutdown is the session's job before it closes the transport.
    state.exec_module.kill(handle)
    :ok
  rescue
    _error -> :ok
  catch
    _kind, _reason -> :ok
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

  # The executable name passes through unresolved: resolution belongs to the
  # execution environment, whose PATH is not ours. stderr must stay out of
  # the stream — pi's protocol is bare JSONL on stdout.
  defp spec(prepared) do
    %{
      cmd: [prepared.executable | prepared.args],
      env: env_map(prepared.env),
      cwd: prepared.cwd,
      stderr: :passthrough
    }
  end

  # The prepared env is Port-shaped ({charlist, charlist} pairs); the Exec
  # contract wants string entries.
  defp env_map(env) do
    Map.new(env, fn {key, value} -> {List.to_string(key), List.to_string(value)} end)
  end
end
