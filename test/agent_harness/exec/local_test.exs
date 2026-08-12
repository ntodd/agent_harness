defmodule AgentHarness.Exec.LocalTest do
  use ExUnit.Case, async: true

  alias AgentHarness.Exec.Local

  defp collect_output(handle, acc \\ "") do
    receive do
      {:exec_data, ^handle, data} -> collect_output(handle, acc <> data)
      {:exec_exit, ^handle, reason} -> {acc, reason}
    after
      5_000 -> flunk("timed out waiting for exec output; got so far: #{inspect(acc)}")
    end
  end

  test "streams stdout and reports the exit status" do
    assert {:ok, handle} =
             Local.start(%{cmd: ["sh", "-c", "printf hello"], env: %{}, cwd: nil}, self(), [])

    assert {"hello", {:exit_status, 0}} = collect_output(handle)
  end

  test "reports nonzero exit statuses" do
    assert {:ok, handle} =
             Local.start(%{cmd: ["sh", "-c", "exit 3"], env: %{}, cwd: nil}, self(), [])

    assert {"", {:exit_status, 3}} = collect_output(handle)
  end

  test "write/2 delivers stdin to the process" do
    assert {:ok, handle} = Local.start(%{cmd: ["cat"], env: %{}, cwd: nil}, self(), [])

    assert :ok = Local.write(handle, "ping\n")
    assert_receive {:exec_data, ^handle, "ping\n"}, 5_000

    assert :ok = Local.kill(handle)
  end

  test "env entries reach the process" do
    spec = %{
      cmd: ["sh", "-c", "printf %s \"$AGENT_HARNESS_EXEC_TEST\""],
      env: %{"AGENT_HARNESS_EXEC_TEST" => "from-env"},
      cwd: nil
    }

    assert {:ok, handle} = Local.start(spec, self(), [])
    assert {"from-env", {:exit_status, 0}} = collect_output(handle)
  end

  test "cwd is applied" do
    dir = Path.join(System.tmp_dir!(), "agent-harness-exec-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    assert {:ok, handle} =
             Local.start(%{cmd: ["sh", "-c", "pwd"], env: %{}, cwd: dir}, self(), [])

    {output, {:exit_status, 0}} = collect_output(handle)
    assert output |> String.trim() |> Path.basename() == Path.basename(dir)
  end

  test "stderr is delivered when merged" do
    spec = %{cmd: ["sh", "-c", "echo oops 1>&2"], env: %{}, cwd: nil, stderr: :merged}

    assert {:ok, handle} = Local.start(spec, self(), [])
    assert {"oops\n", {:exit_status, 0}} = collect_output(handle)
  end

  test "stderr is not delivered by default" do
    spec = %{cmd: ["sh", "-c", "echo oops 1>&2"], env: %{}, cwd: nil}

    assert {:ok, handle} = Local.start(spec, self(), [])
    assert {"", {:exit_status, 0}} = collect_output(handle)
  end

  test "kill/1 stops a running process and emits exactly one exit" do
    assert {:ok, handle} = Local.start(%{cmd: ["cat"], env: %{}, cwd: nil}, self(), [])

    monitor = Process.monitor(handle)

    assert :ok = Local.kill(handle)
    assert_receive {:exec_exit, ^handle, :killed}, 5_000
    assert_receive {:DOWN, ^monitor, :process, ^handle, _reason}, 5_000

    refute_receive {:exec_exit, ^handle, _reason}, 100

    # Idempotent after the handle is gone.
    assert :ok = Local.kill(handle)
  end

  test "no duplicate exit after natural termination followed by kill" do
    assert {:ok, handle} =
             Local.start(%{cmd: ["sh", "-c", "exit 0"], env: %{}, cwd: nil}, self(), [])

    assert_receive {:exec_exit, ^handle, {:exit_status, 0}}, 5_000

    assert :ok = Local.kill(handle)
    refute_receive {:exec_exit, ^handle, _reason}, 100
  end

  test "write/2 after exit returns an error" do
    assert {:ok, handle} =
             Local.start(%{cmd: ["sh", "-c", "exit 0"], env: %{}, cwd: nil}, self(), [])

    assert_receive {:exec_exit, ^handle, {:exit_status, 0}}, 5_000

    assert {:error, :closed} = Local.write(handle, "late\n")
  end

  test "unknown executables fail to start" do
    spec = %{cmd: ["agent-harness-no-such-binary"], env: %{}, cwd: nil}

    assert {:error, {:executable_not_found, "agent-harness-no-such-binary"}} =
             Local.start(spec, self(), [])
  end

  test "kill/1 stops the underlying OS process, not just the pipes" do
    # sleep never reads stdin and never exits on EOF, so closing the port
    # alone would orphan it. The marker makes the process findable.
    marker = "86399.#{System.unique_integer([:positive])}"

    assert {:ok, handle} =
             Local.start(%{cmd: ["sleep", marker], env: %{}, cwd: nil}, self(), [])

    assert os_process_running?(marker)

    assert :ok = Local.kill(handle)
    assert_receive {:exec_exit, ^handle, :killed}, 5_000

    assert await_os_process_exit(marker)
  end

  test "a busy port fails writes with :busy instead of suspending the exec" do
    # sleep never drains stdin, so once the port driver queue is full a
    # suspending write would wedge the exec GenServer and break kill/1.
    assert {:ok, handle} =
             Local.start(%{cmd: ["sleep", "30"], env: %{}, cwd: nil}, self(), [])

    payload = :binary.copy("x", 1_048_576)

    results =
      for _attempt <- 1..8 do
        Local.write(handle, payload)
      end

    assert {:error, :busy} in results

    # The exec stays responsive: kill works promptly and the OS process dies.
    assert :ok = Local.kill(handle)
    assert_receive {:exec_exit, ^handle, :killed}, 5_000
  end

  test "atom env keys are accepted" do
    spec = %{
      cmd: ["sh", "-c", "printf %s \"$ATOM_KEY_TEST\""],
      env: %{ATOM_KEY_TEST: "atom-value"},
      cwd: nil
    }

    assert {:ok, handle} = Local.start(spec, self(), [])
    assert {"atom-value", {:exit_status, 0}} = collect_output(handle)
  end

  defp os_process_running?(marker) do
    {output, _status} = System.cmd("pgrep", ["-f", "sleep #{marker}"])
    String.trim(output) != ""
  end

  defp await_os_process_exit(marker, attempts \\ 40) do
    cond do
      not os_process_running?(marker) ->
        true

      attempts == 0 ->
        false

      true ->
        Process.sleep(50)
        await_os_process_exit(marker, attempts - 1)
    end
  end

  test "the exec process exits when its owner dies" do
    parent = self()

    owner =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    {:ok, handle} = Local.start(%{cmd: ["cat"], env: %{}, cwd: nil}, owner, [])
    send(parent, :ready)

    monitor = Process.monitor(handle)
    Process.exit(owner, :kill)

    assert_receive {:DOWN, ^monitor, :process, ^handle, _reason}, 5_000
  end
end
