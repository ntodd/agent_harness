defmodule AgentHarness.Providers.Claude.AdapterExecTest do
  use ExUnit.Case, async: false

  import Mox

  alias AgentHarness.ExecMock
  alias AgentHarness.Providers.Claude.Adapter.Exec, as: AdapterExec

  setup :set_mox_global
  setup :verify_on_exit!

  @handle :exec_test_handle

  defp default_opts(overrides) do
    Keyword.merge(
      [
        exec: {ExecMock, sandbox: :test_sandbox},
        api_key: "sk-test-key",
        env: %{"EXTRA_VAR" => "extra"},
        cwd: "/workspace",
        model: "claude-opus-5",
        control_timeout: 5_000
      ],
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

  defp start_adapter(opts) do
    stub_exec(self())
    {:ok, adapter} = AdapterExec.start_link(self(), default_opts(opts))
    assert_receive {:adapter_status, :provisioning}, 2_000
    adapter
  end

  defp connect_adapter(opts \\ []) do
    adapter = start_adapter(opts)

    assert_receive {:exec_start, spec, ^adapter, _exec_opts}, 2_000
    assert_receive {:written, init_request}, 2_000

    %{"type" => "control_request", "request_id" => request_id, "request" => request} =
      JSON.decode!(init_request)

    assert request["subtype"] == "initialize"

    feed(adapter, control_response(request_id, %{"commands" => []}))
    assert_receive {:adapter_status, :ready}, 2_000

    {adapter, spec}
  end

  defp feed(adapter, line) when is_binary(line) do
    send(adapter, {:exec_data, @handle, line <> "\n"})
  end

  defp control_response(request_id, response) do
    JSON.encode!(%{
      "type" => "control_response",
      "response" => %{
        "subtype" => "success",
        "request_id" => request_id,
        "response" => response
      }
    })
  end

  defp assistant_message do
    JSON.encode!(%{
      "type" => "assistant",
      "message" => %{"role" => "assistant", "content" => "hello"},
      "session_id" => "cli-session-1"
    })
  end

  defp result_message do
    JSON.encode!(%{
      "type" => "result",
      "subtype" => "success",
      "result" => "done",
      "session_id" => "cli-session-1"
    })
  end

  defp can_use_tool_request(request_id) do
    JSON.encode!(%{
      "type" => "control_request",
      "request_id" => request_id,
      "request" => %{
        "subtype" => "can_use_tool",
        "tool_name" => "Bash",
        "input" => %{"command" => "ls"},
        "tool_use_id" => "tool-use-1"
      }
    })
  end

  describe "provisioning" do
    test "spawns the CLI through the configured exec with a remote-safe spec" do
      {_adapter, spec} = connect_adapter()

      assert [cli | args] = spec.cmd
      assert cli == "claude"
      assert "--output-format" in args
      assert "stream-json" in args
      assert "--input-format" in args
      assert "--print" in args
      assert "--model" in args
      assert "claude-opus-5" in args

      # env is built explicitly: SDK vars + user env + api key. The
      # orchestrator's environment is not explicitly forwarded into the
      # spec (Exec.Local still inherits it as the process's base env).
      assert spec.env["ANTHROPIC_API_KEY"] == "sk-test-key"
      assert spec.env["EXTRA_VAR"] == "extra"
      assert spec.env["CLAUDE_CODE_ENTRYPOINT"] == "sdk-ex"
      assert spec.env["CLAUDECODE"] == false
      refute Map.has_key?(spec.env, "PATH")
      refute Map.has_key?(spec.env, "HOME")

      assert spec.cwd == "/workspace"
      assert spec.stderr == :merged
    end

    test "adapter-internal keys never reach CLI command building" do
      # Command.to_cli_args logs a warning for unknown option keys; a clean
      # log proves :exec and :cli_path were stripped before args were built.
      log =
        ExUnit.CaptureLog.capture_log(fn ->
          {_adapter, _spec} = connect_adapter()
        end)

      refute log =~ "unknown option"
    end

    test "enable_file_checkpointing rides the environment" do
      # The option has no CLI flag; the env var is its only transport.
      {_adapter, spec} = connect_adapter(enable_file_checkpointing: true)

      assert spec.env["CLAUDE_CODE_ENABLE_SDK_FILE_CHECKPOINTING"] == "true"
    end

    test "cli_path overrides the executable" do
      {_adapter, spec} = connect_adapter(cli_path: "/opt/claude/bin/claude")

      assert hd(spec.cmd) == "/opt/claude/bin/claude"
    end

    test "resolver sentinel cli_path values fall back to the default executable" do
      # AgentHarness's Claude provider injects cli_path: :global for the
      # Port adapter's Resolver. Executable resolution belongs to the
      # execution environment here, so sentinels must not reach the spec.
      {_adapter, spec} = connect_adapter(cli_path: :global)

      assert hd(spec.cmd) == "claude"
    end

    test "resume adds the --resume flag" do
      {_adapter, spec} = connect_adapter(resume: "prior-session-id")

      assert ["--resume", "prior-session-id"] =
               Enum.drop_while(spec.cmd, &(&1 != "--resume")) |> Enum.take(2)
    end

    test "exec start failure reports a provisioning error" do
      test_pid = self()

      stub(ExecMock, :start, fn _spec, _owner, _opts ->
        send(test_pid, :start_attempted)
        {:error, :sandbox_gone}
      end)

      {:ok, _adapter} = AdapterExec.start_link(self(), default_opts([]))

      assert_receive {:adapter_status, :provisioning}, 2_000
      assert_receive :start_attempted, 2_000
      assert_receive {:adapter_status, {:error, {:exec_start_failed, :sandbox_gone}}}, 2_000
    end

    test "exec output arriving before the start return is deferred, not dropped" do
      test_pid = self()

      stub(ExecMock, :start, fn _spec, owner, _opts ->
        # A remote exec can stream from a connection process before the
        # start call returns; the adapter learns the handle only afterward.
        send(owner, {:exec_data, @handle, "early CLI banner noise\n"})
        send(test_pid, :started)
        {:ok, @handle}
      end)

      stub(ExecMock, :write, fn @handle, data ->
        send(test_pid, {:written, data |> IO.iodata_to_binary() |> String.trim_trailing("\n")})
        :ok
      end)

      stub(ExecMock, :kill, fn @handle -> :ok end)

      {:ok, adapter} = AdapterExec.start_link(self(), default_opts([]))
      assert_receive {:adapter_status, :provisioning}, 2_000
      assert_receive :started, 2_000
      assert_receive {:written, init_request}, 2_000

      %{"request_id" => request_id} = JSON.decode!(init_request)
      feed(adapter, control_response(request_id, %{"commands" => []}))

      assert_receive {:adapter_status, :ready}, 2_000
    end

    test "an exec exit arriving before the start return fails provisioning" do
      test_pid = self()

      stub(ExecMock, :start, fn _spec, owner, _opts ->
        send(owner, {:exec_exit, @handle, {:exit_status, 1}})
        {:ok, @handle}
      end)

      stub(ExecMock, :write, fn @handle, _data ->
        send(test_pid, :written)
        :ok
      end)

      stub(ExecMock, :kill, fn @handle -> :ok end)

      {:ok, _adapter} = AdapterExec.start_link(self(), default_opts([]))

      assert_receive {:adapter_status, :provisioning}, 2_000
      assert_receive {:adapter_status, {:error, {:exec_exit, {:exit_status, 1}}}}, 2_000
    end

    test "an initialize write failure reports a provisioning error" do
      test_pid = self()

      stub(ExecMock, :start, fn _spec, _owner, _opts -> {:ok, @handle} end)

      stub(ExecMock, :write, fn @handle, _data ->
        send(test_pid, :write_attempted)
        {:error, :closed}
      end)

      stub(ExecMock, :kill, fn @handle -> :ok end)

      {:ok, _adapter} = AdapterExec.start_link(self(), default_opts([]))

      assert_receive :write_attempted, 2_000
      assert_receive {:adapter_status, {:error, {:initialize_write_failed, :closed}}}, 2_000
    end

    test "initialize timeout reports a provisioning error" do
      adapter = start_adapter(control_timeout: 50)

      assert_receive {:exec_start, _spec, ^adapter, _opts}, 2_000
      assert_receive {:written, _init_request}, 2_000

      assert_receive {:adapter_status, {:error, :initialize_timeout}}, 2_000
    end

    test "caches server info from the initialize response" do
      {adapter, _spec} = connect_adapter()

      assert {:ok, %{commands: []}} = AdapterExec.get_server_info(adapter)
    end
  end

  describe "queries" do
    test "send_query writes a stream-json user message and routes replies" do
      {adapter, _spec} = connect_adapter()
      request_id = make_ref()

      assert :ok = AdapterExec.send_query(adapter, request_id, "explain this PR", [])

      assert_receive {:written, query}, 2_000
      decoded = JSON.decode!(query)
      assert decoded["type"] == "user"
      assert decoded["message"]["content"] == "explain this PR"

      feed(adapter, assistant_message())
      assert_receive {:adapter_message, ^request_id, %{"type" => "assistant"}}, 2_000

      feed(adapter, result_message())
      assert_receive {:adapter_message, ^request_id, %{"type" => "result"}}, 2_000

      # The result cleared the in-flight request, so a new query is accepted.
      next = make_ref()
      assert :ok = AdapterExec.send_query(adapter, next, "next", [])
      assert_receive {:written, _next_query}, 2_000
    end

    test "reassembles JSON lines split across data chunks" do
      {adapter, _spec} = connect_adapter()
      request_id = make_ref()
      :ok = AdapterExec.send_query(adapter, request_id, "hi", [])
      assert_receive {:written, _query}, 2_000

      line = assistant_message() <> "\n"
      {first, second} = String.split_at(line, 25)
      send(adapter, {:exec_data, @handle, first})
      send(adapter, {:exec_data, @handle, second})

      assert_receive {:adapter_message, ^request_id, %{"type" => "assistant"}}, 2_000
    end

    test "ignores non-JSON output lines" do
      {adapter, _spec} = connect_adapter()
      request_id = make_ref()
      :ok = AdapterExec.send_query(adapter, request_id, "hi", [])
      assert_receive {:written, _query}, 2_000

      feed(adapter, "npm warn something unrelated on stderr")
      feed(adapter, assistant_message())

      assert_receive {:adapter_message, ^request_id, %{"type" => "assistant"}}, 2_000
    end

    test "messages without an active request are not forwarded" do
      {adapter, _spec} = connect_adapter()

      feed(adapter, assistant_message())

      refute_receive {:adapter_message, _ref, _msg}, 200
    end
  end

  describe "inbound control requests" do
    test "can_use_tool defaults to allow without a callback" do
      {adapter, _spec} = connect_adapter()

      feed(adapter, can_use_tool_request("ctl-1"))

      assert_receive {:written, response}, 2_000
      decoded = JSON.decode!(response)
      assert decoded["type"] == "control_response"
      assert decoded["response"]["request_id"] == "ctl-1"
      assert decoded["response"]["response"]["behavior"] == "allow"
    end

    test "can_use_tool invokes the configured callback for the approval round trip" do
      test_pid = self()

      callback = fn input, tool_use_id ->
        send(test_pid, {:decision_requested, input, tool_use_id})
        {:deny, message: "not allowed"}
      end

      {adapter, _spec} = connect_adapter(can_use_tool: callback)

      # A system message first, so the callback context carries the CLI
      # session id the way Adapter.Port does.
      feed(
        adapter,
        JSON.encode!(%{"type" => "system", "subtype" => "init", "session_id" => "cli-session-1"})
      )

      feed(adapter, can_use_tool_request("ctl-2"))

      assert_receive {:decision_requested, input, "tool-use-1"}, 2_000
      assert input[:tool_name] == "Bash"
      assert input[:session_id] == "cli-session-1"

      assert_receive {:written, response}, 2_000
      decoded = JSON.decode!(response)
      assert decoded["response"]["request_id"] == "ctl-2"
      assert decoded["response"]["response"]["behavior"] == "deny"
      assert decoded["response"]["response"]["message"] == "not allowed"
    end
  end

  describe "outbound control" do
    test "interrupt writes an interrupt control request" do
      {adapter, _spec} = connect_adapter()

      assert :ok = AdapterExec.interrupt(adapter)

      assert_receive {:written, request}, 2_000
      decoded = JSON.decode!(request)
      assert decoded["type"] == "control_request"
      assert decoded["request"]["subtype"] == "interrupt"
    end

    test "send_control_request correlates the response" do
      {adapter, _spec} = connect_adapter()

      task =
        Task.async(fn ->
          AdapterExec.send_control_request(adapter, :set_model, %{model: "claude-opus-5"})
        end)

      assert_receive {:written, request}, 2_000

      %{"request_id" => request_id, "request" => %{"subtype" => "set_model"}} =
        JSON.decode!(request)

      feed(adapter, control_response(request_id, %{"model" => "claude-opus-5"}))

      assert {:ok, %{"model" => "claude-opus-5"}} = Task.await(task, 2_000)
    end
  end

  describe "failure handling" do
    test "exec exit fails the in-flight request and disconnects" do
      {adapter, _spec} = connect_adapter()
      request_id = make_ref()
      :ok = AdapterExec.send_query(adapter, request_id, "hi", [])
      assert_receive {:written, _query}, 2_000

      send(adapter, {:exec_exit, @handle, {:exit_status, 137}})

      assert_receive {:adapter_error, ^request_id, {:exec_exit, {:exit_status, 137}}}, 2_000
      assert {:unhealthy, :not_connected} = AdapterExec.health(adapter)

      assert {:error, :disconnected} = AdapterExec.send_query(adapter, make_ref(), "again", [])
    end

    test "health reports healthy while connected" do
      {adapter, _spec} = connect_adapter()

      assert :healthy = AdapterExec.health(adapter)
    end

    test "a dying pid handle is detected through the monitor" do
      test_pid = self()
      handle = spawn(fn -> Process.sleep(:infinity) end)

      stub(ExecMock, :start, fn _spec, _owner, _opts ->
        send(test_pid, {:handle, handle})
        {:ok, handle}
      end)

      stub(ExecMock, :write, fn ^handle, data ->
        send(test_pid, {:written, data |> IO.iodata_to_binary() |> String.trim_trailing("\n")})
        :ok
      end)

      stub(ExecMock, :kill, fn ^handle ->
        send(test_pid, :exec_killed)
        :ok
      end)

      {:ok, adapter} = AdapterExec.start_link(self(), default_opts([]))
      assert_receive {:adapter_status, :provisioning}, 2_000
      assert_receive {:handle, ^handle}, 2_000
      assert_receive {:written, init_request}, 2_000

      %{"request_id" => request_id} = JSON.decode!(init_request)
      send(adapter, {:exec_data, handle, control_response(request_id, %{}) <> "\n"})
      assert_receive {:adapter_status, :ready}, 2_000

      request_id = make_ref()
      :ok = AdapterExec.send_query(adapter, request_id, "hi", [])
      assert_receive {:written, _query}, 2_000

      # The exec implementation dies without ever sending exec_exit.
      Process.exit(handle, :kill)

      assert_receive {:adapter_error, ^request_id, {:exec_down, :killed}}, 2_000
      assert {:unhealthy, :not_connected} = AdapterExec.health(adapter)
    end

    test "adapter state redacts credentials in inspect and format_status" do
      {adapter, _spec} = connect_adapter()

      status = :sys.get_status(adapter)
      rendered = inspect(status, limit: :infinity, printable_limit: :infinity)

      refute rendered =~ "sk-test-key"
    end
  end

  describe "lifecycle" do
    test "stop sends a best-effort interrupt and kills the exec" do
      {adapter, _spec} = connect_adapter()

      monitor = Process.monitor(adapter)
      assert :ok = AdapterExec.stop(adapter)

      assert_receive {:written, request}, 2_000
      assert JSON.decode!(request)["request"]["subtype"] == "interrupt"
      assert_receive :exec_killed, 2_000
      assert_receive {:DOWN, ^monitor, :process, ^adapter, :normal}, 2_000
    end

    test "execute applies locally on the orchestrator node" do
      {adapter, _spec} = connect_adapter()

      assert "ABC" = AdapterExec.execute(adapter, String, :upcase, ["abc"])
    end
  end
end
