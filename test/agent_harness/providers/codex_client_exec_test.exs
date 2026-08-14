defmodule AgentHarness.Providers.Codex.ClientExecTest do
  use ExUnit.Case, async: false

  import Mox

  alias AgentHarness.ExecMock
  alias AgentHarness.Providers.Codex.Client

  setup :set_mox_global
  setup :verify_on_exit!

  @handle :codex_client_exec_handle

  defp stub_exec(test_pid) do
    stub(ExecMock, :start, fn spec, owner, exec_opts ->
      send(test_pid, {:exec_start, spec, owner, exec_opts})
      {:ok, @handle}
    end)

    stub(ExecMock, :write, fn @handle, data ->
      line = data |> IO.iodata_to_binary() |> String.trim_trailing("\n")
      decoded = JSON.decode!(line)
      send(test_pid, {:written, decoded})
      answer_handshake(decoded, self())
      :ok
    end)

    stub(ExecMock, :kill, fn @handle ->
      send(test_pid, :exec_killed)
      :ok
    end)
  end

  # Answers the initialize request inline so connect can complete without a
  # cooperating test process. The writer is the connection itself.
  defp answer_handshake(%{"id" => 0, "method" => "initialize"}, conn) do
    spawn(fn ->
      send(conn, {:exec_data, @handle, ~s({"id":0,"result":{}}) <> "\n"})
    end)
  end

  defp answer_handshake(_decoded, _conn), do: :ok

  # Disconnects while the test process is still alive, so the exec-mock kill
  # from the connection's terminate lands inside this test's Mox ownership.
  defp disconnect!(proxy) do
    assert :ok = Client.Exec.disconnect(proxy)
    assert_receive :exec_killed, 2_000
  end

  defp options(attrs \\ %{}) do
    {:ok, options} = Codex.Options.new(attrs)
    options
  end

  defp connect_opts(overrides \\ []) do
    Keyword.merge(
      [
        exec: {ExecMock, sandbox: :sb},
        client_name: "agent_harness_test",
        client_version: "0.0.0",
        cwd: "/workspace",
        process_env: %{"CODEX_HOME" => "/tmp/codex-home"}
      ],
      overrides
    )
  end

  test "connect spawns codex app-server through the exec and returns a live proxy" do
    stub_exec(self())

    assert {:ok, proxy} = Client.Exec.connect(options(), connect_opts())
    assert is_pid(proxy)
    assert Client.Exec.alive?(proxy)

    assert_receive {:exec_start, spec, _owner, exec_opts}, 2_000
    assert spec.cmd == ["codex", "app-server"]
    assert spec.cwd == "/workspace"
    assert exec_opts == [sandbox: :sb]

    disconnect!(proxy)
  end

  test "the spawn env carries only explicit entries plus the API credential" do
    stub_exec(self())

    assert {:ok, proxy} =
             Client.Exec.connect(options(%{api_key: "sk-test"}), connect_opts())

    assert_receive {:exec_start, spec, _owner, _exec_opts}, 2_000

    assert spec.env["CODEX_HOME"] == "/tmp/codex-home"
    assert spec.env["OPENAI_API_KEY"] == "sk-test"
    assert spec.env["CODEX_API_KEY"] == "sk-test"

    disconnect!(proxy)
  end

  test "a codex_path override changes the spawned executable" do
    stub_exec(self())

    assert {:ok, proxy} =
             Client.Exec.connect(
               options(%{codex_path_override: "/sandbox/bin/codex"}),
               connect_opts()
             )

    assert_receive {:exec_start, spec, _owner, _exec_opts}, 2_000
    assert spec.cmd == ["/sandbox/bin/codex", "app-server"]

    disconnect!(proxy)
  end

  test "connect fails when the exec cannot start" do
    stub_exec(self())
    stub(ExecMock, :start, fn _spec, _owner, _opts -> {:error, :sandbox_unreachable} end)

    assert {:error, :sandbox_unreachable} = Client.Exec.connect(options(), connect_opts())
  end

  test "connect fails when the handshake times out" do
    stub_exec(self())
    stub(ExecMock, :write, fn @handle, _data -> :ok end)

    assert {:error, {:init_timeout, 100}} =
             Client.Exec.connect(options(), connect_opts(init_timeout_ms: 100))

    # The dead handshake must not leak a running app-server.
    assert_receive :exec_killed, 2_000
  end
end
