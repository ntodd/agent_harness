defmodule AgentHarness.ProviderOpenGuardianTest do
  use ExUnit.Case, async: false

  alias AgentHarness.Provider.OpenGuardian

  defmodule GracefulRuntime do
    use GenServer

    def child_spec(test_pid) do
      %{
        id: {__MODULE__, make_ref()},
        start: {__MODULE__, :start_link, [test_pid]},
        restart: :temporary
      }
    end

    def start_link(test_pid), do: GenServer.start_link(__MODULE__, test_pid)

    @impl true
    def init(test_pid) do
      Process.flag(:trap_exit, true)
      {:ok, test_pid}
    end

    @impl true
    def terminate(reason, test_pid) do
      send(test_pid, {:graceful_runtime_stopped, self(), reason})
      :ok
    end
  end

  defmodule NeverReadyClaudeClient do
    @behaviour AgentHarness.Providers.Claude.Client

    @impl true
    def verify_subscription_auth(_options, _timeout), do: :ok

    @impl true
    def start_link(options) do
      test_pid = Keyword.fetch!(options, :guardian_test_pid)

      case Keyword.get(options, :plugins, []) do
        [generated_plugin] -> send(test_pid, {:generated_plugin, generated_plugin})
        _plugins -> :ok
      end

      Agent.start_link(fn -> test_pid end)
    end

    @impl true
    def await_ready(client_session, _timeout) do
      test_pid = Agent.get(client_session, & &1)
      send(test_pid, {:claude_handshake_blocked, self(), client_session})
      Process.sleep(:infinity)
    end

    @impl true
    def stream(_client_session, _prompt, _options), do: []

    @impl true
    def session_id(_client_session), do: nil

    @impl true
    def interrupt(_client_session), do: :ok

    @impl true
    def stop(client_session), do: Agent.stop(client_session)
  end

  defmodule NeverConnectingCodexClient do
    @behaviour AgentHarness.Providers.Codex.Client

    @impl true
    def options(options), do: {:ok, options}

    @impl true
    def connect(options, _connect_options) do
      send(options.guardian_test_pid, {:codex_handshake_blocked, self()})
      Process.sleep(:infinity)
    end

    @impl true
    def alive?(_connection), do: false

    @impl true
    def disconnect(_connection), do: :ok

    @impl true
    def start_thread(_options, _thread_options), do: {:error, :not_connected}

    @impl true
    def resume_thread(_thread_id, _options, _thread_options), do: {:error, :not_connected}

    @impl true
    def run_streamed(_thread, _input, _turn_options), do: {:error, :not_connected}

    @impl true
    def raw_events(_streaming), do: []

    @impl true
    def respond(_connection, _id, _payload), do: {:error, :not_connected}

    @impl true
    def turn_interrupt(_connection, _thread_id, _turn_id), do: {:error, :not_connected}

    @impl true
    def cancel_stream(_streaming, _mode), do: :ok
  end

  test "Claude runtime is terminated when readiness ignores the session startup timeout" do
    session_id = unique_id("claude-guardian")

    assert {:error, :session_start_timeout} =
             AgentHarness.start_session(:claude,
               id: session_id,
               startup_timeout: 50,
               provider_options: %{
                 auth: :inherit,
                 client: NeverReadyClaudeClient,
                 guardian_test_pid: self()
               }
             )

    assert_receive {:claude_handshake_blocked, runtime, client_session}
    cleanup_on_exit([runtime, client_session])

    assert_process_stops(runtime)
    assert_process_stops(client_session)
    refute runtime in provider_children()
  end

  test "Claude generated skill plugins are removed when the guarded startup is killed" do
    session_id = unique_id("claude-guardian-skill")
    skill = skill_fixture!()

    assert {:error, :session_start_timeout} =
             AgentHarness.start_session(:claude,
               id: session_id,
               startup_timeout: 50,
               skills: [skill],
               provider_options: %{
                 auth: :inherit,
                 client: NeverReadyClaudeClient,
                 guardian_test_pid: self()
               }
             )

    assert_receive {:generated_plugin, generated_plugin}
    assert_receive {:claude_handshake_blocked, runtime, client_session}
    cleanup_on_exit([runtime, client_session])

    assert_process_stops(runtime)
    refute_eventually_exists(generated_plugin)
  end

  test "guardian requests graceful provider termination before forcing it" do
    owner = spawn(fn -> Process.sleep(:infinity) end)

    assert {:ok, runtime} =
             DynamicSupervisor.start_child(
               AgentHarness.ProviderSupervisor,
               {GracefulRuntime, self()}
             )

    cleanup_on_exit([owner, runtime])
    _guardian = OpenGuardian.start(runtime, owner, shutdown_grace: 500)
    monitor = Process.monitor(runtime)

    Process.exit(owner, :kill)

    assert_receive {:graceful_runtime_stopped, ^runtime, :shutdown}
    assert_receive {:DOWN, ^monitor, :process, ^runtime, :shutdown}
  end

  test "guardian rejects an invalid shutdown grace" do
    assert_raise ArgumentError, ~r/non-negative integer/, fn ->
      OpenGuardian.start(self(), self(), shutdown_grace: :infinity)
    end
  end

  test "Codex runtime is terminated when connection ignores the session startup timeout" do
    session_id = unique_id("codex-guardian")

    assert {:error, :session_start_timeout} =
             AgentHarness.start_session(:codex,
               id: session_id,
               startup_timeout: 50,
               provider_options: %{
                 auth: :inherit,
                 client: NeverConnectingCodexClient,
                 codex_options: %{guardian_test_pid: self()}
               }
             )

    assert_receive {:codex_handshake_blocked, runtime}
    cleanup_on_exit([runtime])

    assert_process_stops(runtime)
    refute runtime in provider_children()
  end

  defp assert_process_stops(pid) do
    monitor = Process.monitor(pid)

    if Process.alive?(pid) do
      assert_receive {:DOWN, ^monitor, :process, ^pid, _reason}, 1_000
    else
      assert_receive {:DOWN, ^monitor, :process, ^pid, :noproc}
    end
  end

  defp cleanup_on_exit(pids) do
    on_exit(fn ->
      Enum.each(pids, &stop_if_alive/1)
    end)
  end

  defp stop_if_alive(pid) do
    if Process.alive?(pid), do: Process.exit(pid, :kill)
  end

  defp provider_children do
    AgentHarness.ProviderSupervisor
    |> DynamicSupervisor.which_children()
    |> Enum.map(&elem(&1, 1))
  end

  defp skill_fixture! do
    path =
      Path.join(
        System.tmp_dir!(),
        "agent-harness-guardian-skill-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(path)
    File.write!(Path.join(path, "SKILL.md"), "# Guardian cleanup test\n")
    on_exit(fn -> File.rm_rf!(path) end)
    path
  end

  defp refute_eventually_exists(path, attempts \\ 20)

  defp refute_eventually_exists(path, 0), do: refute(File.exists?(path))

  defp refute_eventually_exists(path, attempts) do
    if File.exists?(path) do
      Process.sleep(25)
      refute_eventually_exists(path, attempts - 1)
    else
      :ok
    end
  end

  defp unique_id(prefix), do: "#{prefix}-#{System.unique_integer([:positive])}"
end
