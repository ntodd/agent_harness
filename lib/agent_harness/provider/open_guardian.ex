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

  @spec protect_paths(t(), [String.t()]) :: :ok | {:error, :guardian_down}
  def protect_paths(guardian, paths) when is_pid(guardian) and is_list(paths) do
    if Enum.all?(paths, &is_binary/1) do
      call_ref = make_ref()
      monitor = Process.monitor(guardian)
      send(guardian, {__MODULE__, :protect_paths, self(), call_ref, paths})

      receive do
        {__MODULE__, :paths_protected, ^call_ref} ->
          Process.demonitor(monitor, [:flush])
          :ok

        {:DOWN, ^monitor, :process, ^guardian, _reason} ->
          {:error, :guardian_down}
      end
    else
      raise ArgumentError, "expected cleanup paths to be strings, got: #{inspect(paths)}"
    end
  end

  defp guard(runtime, owner, supervisor, shutdown_grace) do
    owner_monitor = Process.monitor(owner)
    runtime_monitor = Process.monitor(runtime)

    guard(
      runtime,
      owner,
      owner_monitor,
      runtime_monitor,
      supervisor,
      shutdown_grace,
      MapSet.new()
    )
  end

  defp guard(
         runtime,
         owner,
         owner_monitor,
         runtime_monitor,
         supervisor,
         shutdown_grace,
         cleanup_paths
       ) do
    receive do
      {__MODULE__, :disarm, caller, call_ref} ->
        Process.demonitor(owner_monitor, [:flush])
        Process.demonitor(runtime_monitor, [:flush])
        send(caller, {__MODULE__, :disarmed, call_ref})

      {__MODULE__, :protect_paths, caller, call_ref, paths} ->
        cleanup_paths = register_paths(cleanup_paths, paths)
        send(caller, {__MODULE__, :paths_protected, call_ref})

        guard(
          runtime,
          owner,
          owner_monitor,
          runtime_monitor,
          supervisor,
          shutdown_grace,
          cleanup_paths
        )

      {:DOWN, ^owner_monitor, :process, ^owner, _reason} ->
        stop_runtime(runtime, runtime_monitor, supervisor, shutdown_grace, cleanup_paths)

      {:DOWN, ^runtime_monitor, :process, ^runtime, _reason} ->
        Process.demonitor(owner_monitor, [:flush])
        cleanup_registered_paths(cleanup_paths)
    end
  end

  defp stop_runtime(runtime, runtime_monitor, supervisor, shutdown_grace, cleanup_paths) do
    guardian = self()
    termination_ref = make_ref()

    spawn(fn ->
      result = terminate_child(supervisor, runtime)
      send(guardian, {__MODULE__, :termination_result, termination_ref, result})
    end)

    await_runtime_stop(
      runtime,
      runtime_monitor,
      termination_ref,
      shutdown_grace,
      cleanup_paths
    )
  end

  defp await_runtime_stop(
         runtime,
         runtime_monitor,
         termination_ref,
         shutdown_grace,
         cleanup_paths
       ) do
    receive do
      {__MODULE__, :protect_paths, caller, call_ref, paths} ->
        cleanup_paths = register_paths(cleanup_paths, paths)
        send(caller, {__MODULE__, :paths_protected, call_ref})

        await_runtime_stop(
          runtime,
          runtime_monitor,
          termination_ref,
          shutdown_grace,
          cleanup_paths
        )

      {:DOWN, ^runtime_monitor, :process, ^runtime, _reason} ->
        cleanup_registered_paths(cleanup_paths)

      {__MODULE__, :termination_result, ^termination_ref, _result} ->
        force_stop_and_cleanup(runtime, runtime_monitor, cleanup_paths)
    after
      shutdown_grace ->
        force_stop_and_cleanup(runtime, runtime_monitor, cleanup_paths)
    end
  end

  defp force_stop_and_cleanup(runtime, runtime_monitor, cleanup_paths) do
    if Process.alive?(runtime), do: Process.exit(runtime, :kill)

    await_forced_stop(runtime, runtime_monitor, cleanup_paths)
  end

  defp await_forced_stop(runtime, runtime_monitor, cleanup_paths) do
    receive do
      {__MODULE__, :protect_paths, caller, call_ref, paths} ->
        cleanup_paths = register_paths(cleanup_paths, paths)
        send(caller, {__MODULE__, :paths_protected, call_ref})
        await_forced_stop(runtime, runtime_monitor, cleanup_paths)

      {:DOWN, ^runtime_monitor, :process, ^runtime, _reason} ->
        cleanup_registered_paths(cleanup_paths)
    after
      1_000 ->
        Process.demonitor(runtime_monitor, [:flush])
        cleanup_registered_paths(cleanup_paths)
    end
  end

  defp register_paths(cleanup_paths, paths) do
    Enum.reduce(paths, cleanup_paths, &MapSet.put(&2, &1))
  end

  defp cleanup_registered_paths(cleanup_paths) do
    Enum.each(cleanup_paths, fn path ->
      _result = File.rm_rf(path)
    end)
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
