defmodule AgentHarness.Providers.Pi.ClientExecTest do
  use ExUnit.Case, async: false

  import Mox

  alias AgentHarness.ExecMock
  alias AgentHarness.Providers.Pi.Client

  setup :set_mox_global
  setup :verify_on_exit!

  @handle :pi_exec_test_handle

  defp prepared(overrides \\ %{}) do
    Map.merge(
      %{
        client: Client.Exec,
        exec: {ExecMock, sandbox: :test_sandbox},
        executable: "pi",
        args: ["--mode", "rpc", "--session-id", "sess-1"],
        env: [{~c"PI_CODING_AGENT_DIR", ~c"/tmp/agents"}],
        cwd: "/workspace",
        auth: :inherit,
        provider: nil,
        model: "test-model",
        provider_session_id: "sess-1",
        startup_timeout: 5_000
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
      send(test_pid, {:written, data |> IO.iodata_to_binary() |> String.trim_trailing("\n")})
      :ok
    end)

    stub(ExecMock, :kill, fn @handle ->
      send(test_pid, :exec_killed)
      :ok
    end)
  end

  defp open(overrides \\ %{}) do
    stub_exec(self())
    {:ok, transport} = Client.Exec.open(prepared(overrides), self())
    assert_receive {:exec_start, spec, _owner, exec_opts}, 2_000
    {transport, spec, exec_opts}
  end

  # Closes the transport while the test process is still alive. A transport
  # that outlives its test fires exec-mock calls from terminate into a later
  # test's global Mox ownership, which corrupts that test's verification.
  defp close!(transport) do
    assert :ok = Client.Exec.close(transport)
    assert_receive :exec_killed, 2_000
  end

  describe "open/2" do
    test "starts the command through the configured exec with a remote-safe spec" do
      {transport, spec, exec_opts} = open()

      assert spec.cmd == ["pi", "--mode", "rpc", "--session-id", "sess-1"]
      assert spec.cwd == "/workspace"
      assert exec_opts == [sandbox: :test_sandbox]

      close!(transport)
    end

    test "converts the prepared charlist env into string entries" do
      {transport, spec, _exec_opts} = open()

      assert spec.env == %{"PI_CODING_AGENT_DIR" => "/tmp/agents"}

      close!(transport)
    end

    test "keeps stderr out of the JSONL data stream" do
      {transport, spec, _exec_opts} = open()

      assert Map.get(spec, :stderr, :passthrough) == :passthrough

      close!(transport)
    end

    test "does not resolve the executable locally" do
      # The name must pass through untouched even though nothing named
      # this exists on the local PATH.
      {transport, spec, _exec_opts} =
        open(%{executable: "definitely-not-on-this-machine"})

      assert hd(spec.cmd) == "definitely-not-on-this-machine"

      close!(transport)
    end

    test "defaults to Exec.Local when no exec is configured" do
      # Local exec resolves the executable itself, so a bogus name fails
      # with the Local error shape rather than a remote pass-through.
      prepared = prepared(%{exec: nil, executable: "agent-harness-no-such-cli"})
      prepared = Map.delete(prepared, :exec)

      assert {:error, {:executable_not_found, "agent-harness-no-such-cli"}} =
               Client.Exec.open(prepared, self())
    end

    test "surfaces exec start failures as open errors" do
      stub(ExecMock, :start, fn _spec, _owner, _opts -> {:error, :sandbox_unreachable} end)

      assert {:error, :sandbox_unreachable} = Client.Exec.open(prepared(), self())
    end
  end

  describe "frames" do
    test "decodes exec output into pi frames for the owner" do
      {transport, _spec, _exec_opts} = open()

      send(transport, {:exec_data, @handle, ~s({"type":"response","id":"ah-0",)})
      send(transport, {:exec_data, @handle, ~s("success":true}\n{"type":"agent_start"}\n)})

      assert_receive {:pi_frame, ^transport, %{"type" => "response", "id" => "ah-0"}}, 2_000
      assert_receive {:pi_frame, ^transport, %{"type" => "agent_start"}}, 2_000

      close!(transport)
    end

    test "forwards undecodable lines with the raw payload" do
      {transport, _spec, _exec_opts} = open()

      send(transport, {:exec_data, @handle, "not json\n"})

      assert_receive {:pi_frame, ^transport, %{"type" => "__undecodable__", "raw" => "not json"}},
                     2_000

      close!(transport)
    end

    test "sends frames as JSONL through the exec" do
      {transport, _spec, _exec_opts} = open()

      assert :ok = Client.Exec.send_frame(transport, %{"type" => "prompt", "id" => "ah-1"})

      assert_receive {:written, line}, 2_000
      assert JSON.decode!(line) == %{"type" => "prompt", "id" => "ah-1"}

      close!(transport)
    end

    test "maps a closed exec to :transport_closed" do
      {transport, _spec, _exec_opts} = open()

      stub(ExecMock, :write, fn @handle, _data -> {:error, :closed} end)

      assert {:error, :transport_closed} =
               Client.Exec.send_frame(transport, %{"type" => "prompt"})

      close!(transport)
    end

    test "surfaces a busy exec without suspending" do
      {transport, _spec, _exec_opts} = open()

      stub(ExecMock, :write, fn @handle, _data -> {:error, :busy} end)

      assert {:error, :busy} = Client.Exec.send_frame(transport, %{"type" => "prompt"})

      close!(transport)
    end
  end

  describe "shutdown" do
    test "exec exit reaches the owner as pi_down exactly once" do
      {transport, _spec, _exec_opts} = open()
      monitor = Process.monitor(transport)

      send(transport, {:exec_exit, @handle, {:exit_status, 1}})

      assert_receive {:pi_down, ^transport, {:exit_status, 1}}, 2_000
      assert_receive {:DOWN, ^monitor, :process, ^transport, _reason}, 2_000
      refute_receive {:pi_down, ^transport, _reason}, 100
    end

    test "send_frame after exit reports a transport failure" do
      {transport, _spec, _exec_opts} = open()
      monitor = Process.monitor(transport)

      send(transport, {:exec_exit, @handle, {:exit_status, 0}})
      assert_receive {:pi_down, ^transport, _reason}, 2_000
      assert_receive {:DOWN, ^monitor, :process, ^transport, _reason}, 2_000

      # The transport stops with the exec, mirroring Client.Port, so a late
      # send lands on a dead process rather than a lingering closed state.
      assert {:error, {:transport_call_failed, _reason}} =
               Client.Exec.send_frame(transport, %{"type" => "prompt"})
    end

    test "close kills the underlying exec" do
      {transport, _spec, _exec_opts} = open()

      assert :ok = Client.Exec.close(transport)
      assert_receive :exec_killed, 2_000
    end

    test "owner death kills the exec" do
      stub_exec(self())

      owner =
        spawn(fn ->
          receive do
            :stop -> :ok
          end
        end)

      {:ok, transport} = Client.Exec.open(prepared(), owner)
      monitor = Process.monitor(transport)
      assert_receive {:exec_start, _spec, _owner, _exec_opts}, 2_000

      send(owner, :stop)

      assert_receive :exec_killed, 2_000
      assert_receive {:DOWN, ^monitor, :process, ^transport, _reason}, 2_000
    end
  end

  describe "subscription auth" do
    test "verify_subscription_auth is refused" do
      assert {:error, {:subscription_auth_unsupported, :exec_client}} =
               Client.Exec.verify_subscription_auth(prepared(), 5_000)
    end
  end
end
