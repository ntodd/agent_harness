defmodule AgentHarness.Internal.OwnedTask do
  @moduledoc false

  @owner_key {__MODULE__, :owner}

  @typedoc false
  @type guardian :: pid()

  @doc false
  @spec async_nolink(Task.Supervisor.supervisor(), (-> result), keyword()) :: Task.t()
        when result: term()
  def async_nolink(supervisor, fun, options \\ [])

  def async_nolink(supervisor, fun, options)
      when is_function(fun, 0) and is_list(options) do
    unless Keyword.keyword?(options) do
      raise ArgumentError, "expected task options to be a keyword list"
    end

    owner = self()

    Task.Supervisor.async_nolink(
      supervisor,
      fn ->
        _guardian = arm(owner)
        Process.put(@owner_key, owner)

        try do
          fun.()
        after
          Process.delete(@owner_key)
        end
      end,
      options
    )
  end

  def async_nolink(_supervisor, fun, options) when is_function(fun, 0) do
    raise ArgumentError, "expected task options to be a keyword list, got: #{inspect(options)}"
  end

  def async_nolink(_supervisor, fun, _options) do
    raise ArgumentError,
          "expected task callback to be a zero-arity function, got: #{inspect(fun)}"
  end

  @doc false
  @spec owner() :: pid() | nil
  def owner, do: Process.get(@owner_key)

  @doc false
  @spec arm(pid()) :: guardian()
  def arm(owner) when is_pid(owner) do
    task = self()
    guardian = spawn(fn -> guard(owner, task) end)
    guardian_monitor = Process.monitor(guardian)

    receive do
      {__MODULE__, :armed, ^guardian} ->
        Process.demonitor(guardian_monitor, [:flush])
        guardian

      {:DOWN, ^guardian_monitor, :process, ^guardian, reason} ->
        exit({:owned_task_guardian_failed, reason})
    end
  end

  def arm(owner) do
    raise ArgumentError, "expected task owner to be a pid, got: #{inspect(owner)}"
  end

  @doc false
  @spec disarm(guardian()) :: :ok
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

  def disarm(guardian) do
    raise ArgumentError, "expected guardian to be a pid, got: #{inspect(guardian)}"
  end

  defp guard(owner, task) do
    owner_monitor = Process.monitor(owner)
    task_monitor = Process.monitor(task)
    send(task, {__MODULE__, :armed, self()})

    receive do
      {:DOWN, ^owner_monitor, :process, ^owner, _reason} ->
        Process.exit(task, :kill)
        await_task_down(task, task_monitor)

      {:DOWN, ^task_monitor, :process, ^task, _reason} ->
        Process.demonitor(owner_monitor, [:flush])

      {__MODULE__, :disarm, caller, call_ref} ->
        Process.demonitor(owner_monitor, [:flush])
        Process.demonitor(task_monitor, [:flush])
        send(caller, {__MODULE__, :disarmed, call_ref})
    end
  end

  defp await_task_down(task, task_monitor) do
    receive do
      {:DOWN, ^task_monitor, :process, ^task, _reason} -> :ok
    end
  end
end
