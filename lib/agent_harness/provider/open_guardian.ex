defmodule AgentHarness.Provider.OpenGuardian do
  @moduledoc false

  @default_shutdown_grace 250

  @type t :: pid()

  @spec start(pid(), pid(), keyword()) :: t()
  def start(runtime, owner, opts \\ [])
      when is_pid(runtime) and is_pid(owner) and is_list(opts) do
    supervisor = Keyword.get(opts, :supervisor, AgentHarness.ProviderSupervisor)

    shutdown_grace =
      opts
      |> Keyword.get_lazy(:shutdown_grace, fn ->
        Application.get_env(
          :agent_harness,
          :provider_open_shutdown_grace,
          @default_shutdown_grace
        )
      end)
      |> validate_shutdown_grace!()

    spawn(fn -> guard(runtime, owner, supervisor, shutdown_grace) end)
  end

  @spec disarm(t()) :: :ok
  def disarm(guardian) when is_pid(guardian) do
    call_ref = make_ref()
    monitor = Process.monitor(guardian)
    send(guardian, {__MODULE__, :disarm, self(), call_ref})

    receive do
      {__MODULE__, :disarmed, ^call_ref} ->
        Process.demonitor(monitor, [:flush])
        :ok

      {:DOWN, ^monitor, :process, ^guardian, _reason} ->
        :ok
    end
  end

  defp guard(runtime, owner, supervisor, shutdown_grace) do
    owner_monitor = Process.monitor(owner)
    runtime_monitor = Process.monitor(runtime)

    receive do
      {__MODULE__, :disarm, caller, call_ref} ->
        Process.demonitor(owner_monitor, [:flush])
        Process.demonitor(runtime_monitor, [:flush])
        send(caller, {__MODULE__, :disarmed, call_ref})

      {:DOWN, ^owner_monitor, :process, ^owner, _reason} ->
        stop_runtime(runtime, runtime_monitor, supervisor, shutdown_grace)

      {:DOWN, ^runtime_monitor, :process, ^runtime, _reason} ->
        Process.demonitor(owner_monitor, [:flush])
    end
  end

  defp stop_runtime(runtime, runtime_monitor, supervisor, shutdown_grace) do
    guardian = self()
    termination_ref = make_ref()

    spawn(fn ->
      result = terminate_child(supervisor, runtime)
      send(guardian, {__MODULE__, :termination_result, termination_ref, result})
    end)

    await_runtime_stop(runtime, runtime_monitor, termination_ref, shutdown_grace)
  end

  defp await_runtime_stop(runtime, runtime_monitor, termination_ref, shutdown_grace) do
    receive do
      {:DOWN, ^runtime_monitor, :process, ^runtime, _reason} ->
        :ok

      {__MODULE__, :termination_result, ^termination_ref, _result} ->
        force_stop_if_alive(runtime, runtime_monitor)
    after
      shutdown_grace ->
        force_stop_if_alive(runtime, runtime_monitor)
    end
  end

  defp force_stop_if_alive(runtime, runtime_monitor) do
    if Process.alive?(runtime), do: Process.exit(runtime, :kill)

    receive do
      {:DOWN, ^runtime_monitor, :process, ^runtime, _reason} -> :ok
    after
      1_000 -> Process.demonitor(runtime_monitor, [:flush])
    end
  end

  defp terminate_child(supervisor, runtime) do
    DynamicSupervisor.terminate_child(supervisor, runtime)
  rescue
    error -> {:error, {:exception, error.__struct__, Exception.message(error)}}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp validate_shutdown_grace!(shutdown_grace)
       when is_integer(shutdown_grace) and shutdown_grace >= 0,
       do: shutdown_grace

  defp validate_shutdown_grace!(shutdown_grace) do
    raise ArgumentError,
          "expected :shutdown_grace to be a non-negative integer, got: #{inspect(shutdown_grace)}"
  end
end
