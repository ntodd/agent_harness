defmodule AgentHarness.Internal.OwnedTaskTest do
  use ExUnit.Case, async: true

  alias AgentHarness.Internal.OwnedTask

  describe "async_nolink/3" do
    test "kills a non-trapping task when its owner dies" do
      {owner, task} = start_owned_task(trap_exit?: false)
      task_monitor = Process.monitor(task.pid)

      Process.exit(owner, :kill)

      assert_receive {:DOWN, ^task_monitor, :process, _, :killed}
    end

    test "brutally kills a trapping task when its owner dies" do
      {owner, task} = start_owned_task(trap_exit?: true)
      task_monitor = Process.monitor(task.pid)

      Process.exit(owner, :kill)

      assert_receive {:DOWN, ^task_monitor, :process, _, :killed}
    end

    test "preserves ordinary Task result delivery" do
      task = OwnedTask.async_nolink(AgentHarness.RunnerSupervisor, fn -> {:ok, 42} end)

      assert Task.await(task) == {:ok, 42}
    end

    test "validates the callback and options" do
      assert_raise ArgumentError, ~r/zero-arity function/, fn ->
        OwnedTask.async_nolink(AgentHarness.RunnerSupervisor, :not_a_function)
      end

      assert_raise ArgumentError, ~r/keyword list/, fn ->
        OwnedTask.async_nolink(AgentHarness.RunnerSupervisor, fn -> :ok end, [:invalid])
      end

      assert_raise ArgumentError, ~r/keyword list/, fn ->
        OwnedTask.async_nolink(AgentHarness.RunnerSupervisor, fn -> :ok end, :invalid)
      end
    end
  end

  describe "arm/1 and disarm/1" do
    test "guardian exits after a normally completed task" do
      owner = spawn_sleeper()
      test_pid = self()
      stop_on_exit(owner)

      task =
        Task.Supervisor.async_nolink(AgentHarness.RunnerSupervisor, fn ->
          guardian = OwnedTask.arm(owner)
          send(test_pid, {:guardian, guardian})
          :done
        end)

      assert_receive {:guardian, guardian}
      guardian_monitor = Process.monitor(guardian)

      assert Task.await(task) == :done
      assert_receive {:DOWN, ^guardian_monitor, :process, ^guardian, reason}
      assert reason in [:normal, :noproc]

      Process.exit(owner, :kill)
    end

    test "disarming lets a task outlive the former owner" do
      owner = spawn_sleeper()
      test_pid = self()
      stop_on_exit(owner)

      task =
        Task.Supervisor.async_nolink(AgentHarness.RunnerSupervisor, fn ->
          guardian = OwnedTask.arm(owner)
          send(test_pid, {:guardian, guardian})

          receive do
            :finish -> :done
          end
        end)

      assert_receive {:guardian, guardian}
      assert :ok = OwnedTask.disarm(guardian)

      Process.exit(owner, :kill)
      assert Process.alive?(task.pid)

      send(task.pid, :finish)
      assert Task.await(task) == :done
    end

    test "validates guardian arguments" do
      assert_raise ArgumentError, ~r/task owner to be a pid/, fn ->
        OwnedTask.arm(:not_a_pid)
      end

      assert_raise ArgumentError, ~r/guardian to be a pid/, fn ->
        OwnedTask.disarm(:not_a_pid)
      end
    end
  end

  defp start_owned_task(options) do
    test_pid = self()

    owner =
      spawn(fn ->
        task =
          OwnedTask.async_nolink(AgentHarness.RunnerSupervisor, fn ->
            Process.flag(:trap_exit, Keyword.fetch!(options, :trap_exit?))
            send(test_pid, {:owned_task_started, self()})
            Process.sleep(:infinity)
          end)

        send(test_pid, {:owned_task, self(), task})
        Process.sleep(:infinity)
      end)

    assert_receive {:owned_task, ^owner, task}
    assert_receive {:owned_task_started, task_pid}
    assert task.pid == task_pid
    stop_on_exit(owner)
    stop_on_exit(task.pid)
    {owner, task}
  end

  defp spawn_sleeper, do: spawn(fn -> Process.sleep(:infinity) end)

  defp stop_on_exit(pid) do
    on_exit(fn ->
      if Process.alive?(pid), do: Process.exit(pid, :kill)
    end)
  end
end
