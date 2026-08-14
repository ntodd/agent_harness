defmodule AgentHarness.Providers.Codex.ExecConnectionTest do
  use ExUnit.Case, async: false

  import Mox

  alias AgentHarness.ExecMock
  alias AgentHarness.Providers.Codex.ExecConnection
  alias Codex.AppServer.Connection

  setup :set_mox_global
  setup :verify_on_exit!

  @handle :codex_exec_test_handle

  defp spec(overrides \\ %{}) do
    Map.merge(
      %{
        cmd: ["codex", "app-server"],
        env: %{"CODEX_HOME" => "/tmp/codex-home"},
        cwd: "/workspace",
        stderr: :passthrough
      },
      overrides
    )
  end

  defp stub_exec(test_pid) do
    stub(ExecMock, :start, fn spec, owner, exec_opts ->
      send(test_pid, {:exec_start, spec, owner, exec_opts})
      {:ok, @handle}
    end)

    stub(ExecMock, :write, fn @handle, data ->
      line = data |> IO.iodata_to_binary() |> String.trim_trailing("\n")
      send(test_pid, {:written, JSON.decode!(line)})
      :ok
    end)

    stub(ExecMock, :kill, fn @handle ->
      send(test_pid, :exec_killed)
      :ok
    end)
  end

  defp open(opts \\ []) do
    stub_exec(self())

    {:ok, conn} =
      ExecConnection.start(
        spec(),
        Keyword.merge(
          [
            exec: {ExecMock, sandbox: :test_sandbox},
            client_name: "agent_harness_test",
            client_version: "0.0.0",
            init_timeout_ms: 5_000
          ],
          opts
        )
      )

    assert_receive {:exec_start, started_spec, _owner, exec_opts}, 2_000
    {conn, started_spec, exec_opts}
  end

  defp feed(conn, message) when is_map(message) do
    send(conn, {:exec_data, @handle, JSON.encode!(message) <> "\n"})
  end

  # Stops the connection while the test process is still alive. A connection
  # that outlives its test fires exec-mock calls from terminate (or from a
  # late init timer) into a later test's global Mox ownership, which corrupts
  # that test's verification state.
  defp stop!(conn) do
    GenServer.stop(conn, :normal)
    assert_receive :exec_killed, 2_000
  end

  # Blocks until an await_ready caller is registered, so a test can feed the
  # init reply knowing the waiter will receive it rather than racing the stop.
  defp await_ready_waiter(conn, attempts \\ 50)

  defp await_ready_waiter(_conn, 0), do: flunk("await_ready waiter never registered")

  defp await_ready_waiter(conn, attempts) do
    case :sys.get_state(conn) do
      %{ready_waiters: [_ | _]} ->
        :ok

      _state ->
        Process.sleep(10)
        await_ready_waiter(conn, attempts - 1)
    end
  end

  defp connect_ready(opts \\ []) do
    {conn, started_spec, exec_opts} = open(opts)

    assert_receive {:written, %{"id" => 0, "method" => "initialize", "params" => params}}, 2_000

    feed(conn, %{"id" => 0, "result" => %{"userAgent" => "codex/0.0.0"}})

    assert_receive {:written, %{"method" => "initialized"}}, 2_000
    assert :ok = Connection.await_ready(conn, 2_000)

    {conn, started_spec, exec_opts, params}
  end

  describe "spawn" do
    test "starts the app-server through the configured exec with the given spec" do
      {conn, started_spec, exec_opts, _params} = connect_ready()

      assert started_spec.cmd == ["codex", "app-server"]
      assert started_spec.env == %{"CODEX_HOME" => "/tmp/codex-home"}
      assert started_spec.cwd == "/workspace"
      assert Map.get(started_spec, :stderr, :passthrough) == :passthrough
      assert exec_opts == [sandbox: :test_sandbox]

      stop!(conn)
    end

    test "surfaces exec start failures" do
      stub_exec(self())
      stub(ExecMock, :start, fn _spec, _owner, _opts -> {:error, :sandbox_unreachable} end)

      assert {:error, :sandbox_unreachable} =
               ExecConnection.start(spec(), exec: {ExecMock, []})
    end
  end

  describe "initialize handshake" do
    test "sends clientInfo and the experimental capability" do
      {conn, _spec, _exec_opts, params} =
        connect_ready(client_title: "AgentHarness", experimental_api: true)

      assert params["clientInfo"] == %{
               "name" => "agent_harness_test",
               "version" => "0.0.0",
               "title" => "AgentHarness"
             }

      assert params["capabilities"] == %{"experimentalApi" => true}

      stop!(conn)
    end

    test "await_ready blocks until the handshake completes" do
      {conn, _spec, _exec_opts} = open()

      task = Task.async(fn -> Connection.await_ready(conn, 5_000) end)

      assert_receive {:written, %{"id" => 0, "method" => "initialize"}}, 2_000
      feed(conn, %{"id" => 0, "result" => %{}})

      assert Task.await(task) == :ok

      stop!(conn)
    end

    test "an initialize error fails ready waiters and stops the connection" do
      {conn, _spec, _exec_opts} = open()
      monitor = Process.monitor(conn)

      task = Task.async(fn -> Connection.await_ready(conn, 5_000) end)
      await_ready_waiter(conn)

      assert_receive {:written, %{"id" => 0, "method" => "initialize"}}, 2_000
      feed(conn, %{"id" => 0, "error" => %{"code" => -32_600, "message" => "nope"}})

      assert {:error, {:init_failed, %{"message" => "nope"}}} = Task.await(task)
      assert_receive {:DOWN, ^monitor, :process, ^conn, _reason}, 2_000
    end

    test "requests are refused until the connection is ready" do
      {conn, _spec, _exec_opts} = open()

      assert {:error, :not_ready} = Connection.request(conn, "thread/start", %{})

      stop!(conn)
    end
  end

  describe "requests" do
    test "correlates responses to callers by id" do
      {conn, _spec, _exec_opts, _params} = connect_ready()

      task = Task.async(fn -> Connection.request(conn, "thread/start", %{"foo" => 1}) end)

      assert_receive {:written,
                      %{"id" => id, "method" => "thread/start", "params" => %{"foo" => 1}}},
                     2_000

      feed(conn, %{"id" => id, "result" => %{"threadId" => "t-1"}})

      assert {:ok, %{"threadId" => "t-1"}} = Task.await(task)

      stop!(conn)
    end

    test "returns JSON-RPC errors to the caller" do
      {conn, _spec, _exec_opts, _params} = connect_ready()

      task = Task.async(fn -> Connection.request(conn, "thread/start", %{}) end)

      assert_receive {:written, %{"id" => id, "method" => "thread/start"}}, 2_000
      feed(conn, %{"id" => id, "error" => %{"code" => 1, "message" => "bad"}})

      assert {:error, %{"code" => 1, "message" => "bad"}} = Task.await(task)

      stop!(conn)
    end

    test "times out slow requests without wedging the connection" do
      {conn, _spec, _exec_opts, _params} = connect_ready()

      task =
        Task.async(fn ->
          Connection.request(conn, "thread/start", %{}, timeout_ms: 100)
        end)

      assert {:error, {:timeout, "thread/start", 100}} = Task.await(task)

      # The connection still answers later requests.
      task = Task.async(fn -> Connection.request(conn, "turn/start", %{}) end)
      assert_receive {:written, %{"id" => id, "method" => "turn/start"}}, 2_000
      feed(conn, %{"id" => id, "result" => %{}})
      assert {:ok, %{}} = Task.await(task)

      stop!(conn)
    end

    test "a write failure surfaces as a request error" do
      {conn, _spec, _exec_opts, _params} = connect_ready()

      stub(ExecMock, :write, fn @handle, _data -> {:error, :closed} end)

      assert {:error, :closed} = Connection.request(conn, "thread/start", %{})

      stop!(conn)
    end
  end

  describe "notifications and server-initiated requests" do
    test "broadcasts notifications to subscribers" do
      {conn, _spec, _exec_opts, _params} = connect_ready()
      assert :ok = Connection.subscribe(conn)

      feed(conn, %{"method" => "thread/started", "params" => %{"threadId" => "t-1"}})

      assert_receive {:codex_notification, "thread/started", %{"threadId" => "t-1"}}, 2_000

      stop!(conn)
    end

    test "broadcasts server requests and routes responses back" do
      {conn, _spec, _exec_opts, _params} = connect_ready()
      assert :ok = Connection.subscribe(conn)

      feed(conn, %{
        "id" => "srv-1",
        "method" => "item/commandApproval",
        "params" => %{"threadId" => "t-1"}
      })

      assert_receive {:codex_request, "srv-1", "item/commandApproval", %{"threadId" => "t-1"}},
                     2_000

      assert :ok = Connection.respond(conn, "srv-1", %{"decision" => "accept"})

      assert_receive {:written, %{"id" => "srv-1", "result" => %{"decision" => "accept"}}},
                     2_000

      stop!(conn)
    end

    test "respond_error sends a JSON-RPC error frame" do
      {conn, _spec, _exec_opts, _params} = connect_ready()

      assert :ok = Connection.respond_error(conn, "srv-2", -32_600, "denied", nil)

      assert_receive {:written,
                      %{
                        "id" => "srv-2",
                        "error" => %{"code" => -32_600, "message" => "denied"}
                      }},
                     2_000

      stop!(conn)
    end

    test "buffers pre-ready events and flushes them to the first subscriber" do
      {conn, _spec, _exec_opts} = open()

      assert_receive {:written, %{"id" => 0, "method" => "initialize"}}, 2_000

      # Notification arrives before the handshake completes.
      feed(conn, %{"method" => "early/notice", "params" => %{"n" => 1}})
      feed(conn, %{"id" => 0, "result" => %{}})
      assert :ok = Connection.await_ready(conn, 2_000)

      refute_receive {:codex_notification, _method, _params}, 100

      assert :ok = Connection.subscribe(conn)
      assert_receive {:codex_notification, "early/notice", %{"n" => 1}}, 2_000

      stop!(conn)
    end

    test "honors subscriber method and thread filters" do
      {conn, _spec, _exec_opts, _params} = connect_ready()

      assert :ok = Connection.subscribe(conn, methods: ["turn/completed"], thread_id: "t-1")

      feed(conn, %{"method" => "turn/started", "params" => %{"threadId" => "t-1"}})
      feed(conn, %{"method" => "turn/completed", "params" => %{"threadId" => "t-2"}})
      feed(conn, %{"method" => "turn/completed", "params" => %{"threadId" => "t-1"}})

      assert_receive {:codex_notification, "turn/completed", %{"threadId" => "t-1"}}, 2_000
      refute_receive {:codex_notification, "turn/started", _params}, 100
      refute_receive {:codex_notification, _method, %{"threadId" => "t-2"}}, 100

      stop!(conn)
    end
  end

  describe "shutdown" do
    test "exec exit fails pending requests and stops the connection" do
      {conn, _spec, _exec_opts, _params} = connect_ready()
      monitor = Process.monitor(conn)

      task = Task.async(fn -> Connection.request(conn, "thread/start", %{}) end)
      assert_receive {:written, %{"method" => "thread/start"}}, 2_000

      send(conn, {:exec_exit, @handle, {:exit_status, 1}})

      assert {:error, {:app_server_down, %{reason: {:exit_status, 1}}}} = Task.await(task)

      assert_receive {:DOWN, ^monitor, :process, ^conn, {:shutdown, {:app_server_down, _}}},
                     2_000
    end

    test "stopping the connection kills the exec" do
      {conn, _spec, _exec_opts, _params} = connect_ready()

      GenServer.stop(conn, :normal)

      assert_receive :exec_killed, 2_000
    end

    test "owner death stops the connection and kills the exec" do
      stub_exec(self())

      owner =
        spawn(fn ->
          receive do
            :stop -> :ok
          end
        end)

      {:ok, conn} = ExecConnection.start(spec(), exec: {ExecMock, []}, owner: owner)
      monitor = Process.monitor(conn)
      assert_receive {:exec_start, _spec, _owner, _exec_opts}, 2_000

      send(owner, :stop)

      assert_receive :exec_killed, 2_000
      assert_receive {:DOWN, ^monitor, :process, ^conn, _reason}, 2_000
    end
  end
end
