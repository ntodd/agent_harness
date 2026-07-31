defmodule AgentHarness.Providers.ClaudeTest do
  use ExUnit.Case, async: false

  import Mox

  alias AgentHarness.Provider.Sink

  alias AgentHarness.{
    Capabilities,
    Response,
    SessionConfig,
    Turn
  }

  alias AgentHarness.Providers.Claude
  alias AgentHarness.Providers.Claude.ClientMock
  alias ClaudeCode.Message.ResultMessage
  alias ClaudeCode.Test.Factory

  setup :set_mox_global
  setup :verify_on_exit!

  setup do
    stub(ClientMock, :verify_subscription_auth, fn _options, _timeout -> :ok end)
    stub(ClientMock, :await_ready, fn _session, _timeout -> :ok end)

    session = spawn(fn -> receive do: (:stop -> :ok) end)

    on_exit(fn ->
      if Process.alive?(session), do: send(session, :stop)
    end)

    %{client_session: session}
  end

  test "opens the global CLI with isolated MCP and plugin-backed skills", %{
    client_session: client_session
  } do
    test_pid = self()
    plugin_path = plugin_fixture!()

    expect(ClientMock, :start_link, fn opts ->
      send(test_pid, {:client_options, opts})
      {:ok, client_session}
    end)

    expect(ClientMock, :session_id, fn ^client_session -> "resumed-claude-session" end)
    expect(ClientMock, :stop, fn ^client_session -> :ok end)

    config =
      config(
        cwd: "/work/project",
        model: "sonnet",
        system_prompt: "Work carefully",
        mcp_servers: %{"docs" => %{command: "docs-server"}},
        skills: [plugin_path]
      )

    sink = Sink.new(self())

    assert {:ok, handle, %{provider_session_id: "resumed-claude-session"}} =
             Claude.open_session(config, sink)

    assert_receive {:client_options, opts}
    assert opts[:cli_path] == :global
    assert opts[:cwd] == "/work/project"
    assert opts[:model] == "sonnet"
    assert opts[:system_prompt] == "Work carefully"
    assert opts[:mcp_servers] == %{"docs" => %{command: "docs-server"}}
    assert opts[:strict_mcp_config] == true
    assert opts[:include_partial_messages] == true
    assert opts[:plugins] == [plugin_path]
    assert "Skill" in opts[:allowed_tools]
    assert opts[:env]["ANTHROPIC_API_KEY"] == false
    assert is_function(opts[:can_use_tool], 2)

    assert :ok = Claude.close_session(handle)
  end

  test "does not open a logical session until the Claude transport is ready", %{
    client_session: client_session
  } do
    expect(ClientMock, :start_link, fn _opts -> {:ok, client_session} end)

    expect(ClientMock, :await_ready, fn ^client_session, timeout ->
      assert timeout > 0
      {:error, {:cli_not_ready, :not_connected}}
    end)

    expect(ClientMock, :stop, fn ^client_session -> :ok end)

    assert {:error, _reason} = Claude.open_session(config(), Sink.new(self()))
  end

  test "consumes Claude messages in a runner and finishes on ResultMessage", %{
    client_session: client_session
  } do
    partial =
      Factory.stream_event_text_delta("Hello",
        session_id: "claude-session-1"
      )

    result =
      ClaudeCode.Test.result("Hello there",
        session_id: "claude-session-1",
        stop_reason: :end_turn
      )

    expect(ClientMock, :start_link, fn _opts -> {:ok, client_session} end)
    expect(ClientMock, :session_id, fn ^client_session -> nil end)

    expect(ClientMock, :stream, fn ^client_session, "Say hello", [] ->
      [partial, result]
    end)

    expect(ClientMock, :stop, fn ^client_session -> :ok end)

    sink = Sink.new(self())
    sink_ref = sink.ref
    {:ok, handle, _info} = Claude.open_session(config(), sink)
    turn = Turn.new("session-1", "Say hello", id: "turn-1")

    assert {:ok, "turn-1"} = Claude.start_turn(handle, turn, "Say hello", [])

    assert_receive {:agent_harness_provider, ^sink_ref,
                    {:session_updated, %{provider_session_id: "claude-session-1"}}}

    assert_receive {:agent_harness_provider, ^sink_ref,
                    {:event, "turn-1", :message_delta, %{text: "Hello"}, ^partial}}

    assert_receive {:agent_harness_provider, ^sink_ref,
                    {:finish, "turn-1", :completed,
                     %{
                       text: "Hello there",
                       is_error: false,
                       session_id: "claude-session-1",
                       stop_reason: :end_turn
                     }, ^result}}

    assert :ok = Claude.close_session(handle)
  end

  test "surfaces AskUserQuestion and returns the normalized answer", %{
    client_session: client_session
  } do
    test_pid = self()
    result = ClaudeCode.Test.result("You chose red", session_id: "claude-session-2")

    expect(ClientMock, :start_link, fn opts ->
      send(test_pid, {:callback, opts[:can_use_tool]})
      {:ok, client_session}
    end)

    expect(ClientMock, :session_id, fn ^client_session -> nil end)

    expect(ClientMock, :stream, fn ^client_session, "Ask me", [] ->
      receive do
        :complete_stream -> [result]
      end
    end)

    expect(ClientMock, :stop, fn ^client_session -> :ok end)

    sink = Sink.new(self())
    sink_ref = sink.ref
    {:ok, handle, _info} = Claude.open_session(config(), sink)
    assert_receive {:callback, callback}

    turn = Turn.new("session-1", "Ask me", id: "turn-question")
    assert {:ok, "turn-question"} = Claude.start_turn(handle, turn, "Ask me", [])

    callback_task =
      Task.async(fn ->
        callback.(
          %{
            tool_name: "AskUserQuestion",
            input: %{
              "questions" => [
                %{
                  "header" => "Color",
                  "question" => "Red or blue?",
                  "options" => [
                    %{"label" => "Red", "description" => "Warm"},
                    %{"label" => "Blue", "description" => "Cool"}
                  ],
                  "multiSelect" => false
                }
              ]
            }
          },
          "tool-use-1"
        )
      end)

    assert_receive {:agent_harness_provider, ^sink_ref,
                    {:request, "turn-question", provider_ref, attrs, raw}}

    assert attrs[:kind] == :question
    assert attrs[:prompt] == "Red or blue?"
    assert [%{prompt: "Red or blue?", choices: choices}] = attrs[:questions]
    assert Enum.map(choices, & &1.label) == ["Red", "Blue"]
    assert raw.tool_name == "AskUserQuestion"

    assert {:error, {:invalid_response_action, :question, :approve}} =
             Claude.respond(handle, provider_ref, Response.approve())

    assert {:error, {:missing_answer, "Red or blue?"}} =
             Claude.respond(
               handle,
               provider_ref,
               Response.answer(%{{:invalid, 1} => "Red", {:invalid, 2} => "Blue"})
             )

    assert Process.alive?(handle)

    assert :ok =
             Claude.respond(
               handle,
               provider_ref,
               Response.answer(%{"Red or blue?" => "Red"})
             )

    assert {:allow, updated_input: updated_input} = Task.await(callback_task)
    assert updated_input["answers"] == %{"Red or blue?" => "Red"}

    send_runner(handle, :complete_stream)

    assert_receive {:agent_harness_provider, ^sink_ref,
                    {:finish, "turn-question", :completed, _data, ^result}}

    assert :ok = Claude.close_session(handle)
  end

  test "cancellation unblocks a pending approval before the interrupt call returns", %{
    client_session: client_session
  } do
    test_pid = self()

    expect(ClientMock, :start_link, fn opts ->
      send(test_pid, {:callback, opts[:can_use_tool]})
      {:ok, client_session}
    end)

    expect(ClientMock, :session_id, fn ^client_session -> nil end)

    expect(ClientMock, :stream, fn ^client_session, "Run command", [] ->
      receive do
        :never -> []
      end
    end)

    expect(ClientMock, :interrupt, fn ^client_session ->
      send(test_pid, {:interrupt_started, self()})

      receive do
        :finish_interrupt -> :ok
      end
    end)

    expect(ClientMock, :stop, fn ^client_session -> :ok end)

    sink = Sink.new(self())
    sink_ref = sink.ref
    {:ok, handle, _info} = Claude.open_session(config(), sink)
    assert_receive {:callback, callback}

    turn = Turn.new("session-1", "Run command", id: "turn-cancel")
    assert {:ok, "turn-cancel"} = Claude.start_turn(handle, turn, "Run command", [])

    callback_task =
      Task.async(fn ->
        callback.(
          %{tool_name: "Bash", input: %{"command" => "rm file"}},
          "tool-use-2"
        )
      end)

    assert_receive {:agent_harness_provider, ^sink_ref,
                    {:request, "turn-cancel", _provider_ref, attrs, _raw}}

    assert attrs[:kind] == :command_approval

    cancel_task = Task.async(fn -> Claude.cancel(handle, "turn-cancel") end)
    assert_receive {:interrupt_started, interrupt_worker}

    assert {:ok, {:deny, decision_opts}} = Task.yield(callback_task, 500)
    assert decision_opts[:interrupt] == true

    send(interrupt_worker, :finish_interrupt)
    assert :ok = Task.await(cancel_task)
    assert :ok = Claude.close_session(handle)
  end

  test "cancellation remains interrupted when Claude ends without a result", %{
    client_session: client_session
  } do
    expect(ClientMock, :start_link, fn _opts -> {:ok, client_session} end)
    expect(ClientMock, :session_id, fn ^client_session -> nil end)

    expect(ClientMock, :stream, fn ^client_session, "Cancel me", [] ->
      receive do
        :finish_stream -> []
      end
    end)

    expect(ClientMock, :interrupt, fn ^client_session -> :ok end)
    expect(ClientMock, :stop, fn ^client_session -> :ok end)

    sink = Sink.new(self())
    sink_ref = sink.ref
    {:ok, handle, _info} = Claude.open_session(config(), sink)

    turn = Turn.new("session-1", "Cancel me", id: "turn-cancel-eof")
    assert {:ok, "turn-cancel-eof"} = Claude.start_turn(handle, turn, "Cancel me", [])
    assert :ok = Claude.cancel(handle, "turn-cancel-eof")

    send_runner(handle, :finish_stream)

    assert_receive {:agent_harness_provider, ^sink_ref,
                    {:finish, "turn-cancel-eof", :interrupted, %{reason: :cancelled}, nil}}

    assert :ok = Claude.close_session(handle)
  end

  test "an interrupt failure rejects pending approval and tears down the provider", %{
    client_session: client_session
  } do
    test_pid = self()

    expect(ClientMock, :start_link, fn opts ->
      send(test_pid, {:callback, opts[:can_use_tool]})
      {:ok, client_session}
    end)

    expect(ClientMock, :session_id, fn ^client_session -> nil end)

    expect(ClientMock, :stream, fn ^client_session, "Keep running", [] ->
      receive do
        :never -> []
      end
    end)

    expect(ClientMock, :interrupt, fn ^client_session -> {:error, :not_connected} end)

    expect(ClientMock, :stop, fn ^client_session ->
      receive do
        :never -> :ok
      end
    end)

    sink = Sink.new(self())
    sink_ref = sink.ref
    {:ok, handle, _info} = Claude.open_session(config(), sink)
    monitor = Process.monitor(handle)
    assert_receive {:callback, callback}

    turn = Turn.new("session-1", "Keep running", id: "turn-interrupt-failed")

    assert {:ok, "turn-interrupt-failed"} =
             Claude.start_turn(handle, turn, "Keep running", [])

    callback_task =
      Task.async(fn ->
        callback.(
          %{tool_name: "Bash", input: %{"command" => "mix test"}},
          "tool-interrupt-failed"
        )
      end)

    assert_receive {:agent_harness_provider, ^sink_ref,
                    {:request, "turn-interrupt-failed", _provider_ref, _attrs, _raw}}

    started_at = System.monotonic_time(:millisecond)
    assert {:error, :not_connected} = Claude.cancel(handle, "turn-interrupt-failed")
    assert System.monotonic_time(:millisecond) - started_at < 500

    assert {:deny, decision_opts} = Task.await(callback_task)
    assert decision_opts[:interrupt] == true

    assert_receive {:agent_harness_provider, ^sink_ref,
                    {:transport_down, {:claude_interrupt_failed, :not_connected}}}

    assert_receive {:DOWN, ^monitor, :process, ^handle, :normal}, 1_500
  end

  test "a stream ending without a terminal result tears down the provider", %{
    client_session: client_session
  } do
    expect(ClientMock, :start_link, fn _opts -> {:ok, client_session} end)
    expect(ClientMock, :session_id, fn ^client_session -> nil end)
    expect(ClientMock, :stream, fn ^client_session, "Lose result", [] -> [] end)
    expect(ClientMock, :stop, fn ^client_session -> :ok end)

    sink = Sink.new(self())
    sink_ref = sink.ref
    {:ok, handle, _info} = Claude.open_session(config(), sink)
    monitor = Process.monitor(handle)
    turn = Turn.new("session-1", "Lose result", id: "turn-missing-result")

    assert {:ok, "turn-missing-result"} =
             Claude.start_turn(handle, turn, "Lose result", [])

    assert_receive {:agent_harness_provider, ^sink_ref,
                    {:finish, "turn-missing-result", :failed, %{reason: :missing_result}, nil}}

    assert_receive {:agent_harness_provider, ^sink_ref,
                    {:transport_down, {:claude_stream_failed, :missing_result}}}

    assert_receive {:DOWN, ^monitor, :process, ^handle, :normal}
  end

  test "question timeout expires the provider-neutral request", %{
    client_session: client_session
  } do
    test_pid = self()

    expect(ClientMock, :start_link, fn opts ->
      send(test_pid, {:callback, opts[:can_use_tool]})
      {:ok, client_session}
    end)

    expect(ClientMock, :session_id, fn ^client_session -> nil end)

    expect(ClientMock, :stream, fn ^client_session, "Wait for input", [] ->
      receive do
        :never -> []
      end
    end)

    expect(ClientMock, :stop, fn ^client_session -> :ok end)

    config =
      config(provider_options: %{client: ClientMock, question_timeout: 25})

    sink = Sink.new(self())
    sink_ref = sink.ref
    {:ok, handle, _info} = Claude.open_session(config, sink)
    assert_receive {:callback, callback}

    turn = Turn.new("session-1", "Wait for input", id: "turn-question-timeout")

    assert {:ok, "turn-question-timeout"} =
             Claude.start_turn(handle, turn, "Wait for input", [])

    input = %{tool_name: "Bash", input: %{"command" => "mix test"}}
    callback_task = Task.async(fn -> callback.(input, "tool-timeout") end)

    assert_receive {:agent_harness_provider, ^sink_ref,
                    {:request, "turn-question-timeout", provider_ref, _attrs, ^input}}

    assert_receive {:agent_harness_provider, ^sink_ref,
                    {:expire_request, "turn-question-timeout", ^provider_ref, :question_timeout,
                     ^input}}

    assert {:deny, decision_opts} = Task.await(callback_task)
    assert decision_opts[:interrupt] == true

    assert :ok = Claude.close_session(handle)
  end

  test "reports the supported Claude capabilities" do
    assert %Capabilities{
             token_streaming: :native,
             questions: :native,
             approvals: :native,
             cancel: :native,
             resume: :native,
             fork: :native,
             per_session_mcp: :native,
             skills: :emulated,
             steer: :unsupported
           } = Claude.capabilities(self())
  end

  test "maps an error result to a failed turn", %{client_session: client_session} do
    result =
      ClaudeCode.Test.result("Rate limited",
        session_id: "claude-session-3",
        is_error: true
      )

    assert %ResultMessage{is_error: true} = result

    expect(ClientMock, :start_link, fn _opts -> {:ok, client_session} end)
    expect(ClientMock, :session_id, fn ^client_session -> nil end)
    expect(ClientMock, :stream, fn ^client_session, "Fail", [] -> [result] end)
    expect(ClientMock, :stop, fn ^client_session -> :ok end)

    sink = Sink.new(self())
    sink_ref = sink.ref
    {:ok, handle, _info} = Claude.open_session(config(), sink)

    assert {:ok, "turn-fail"} =
             Claude.start_turn(
               handle,
               Turn.new("session-1", "Fail", id: "turn-fail"),
               "Fail",
               []
             )

    assert_receive {:agent_harness_provider, ^sink_ref,
                    {:finish, "turn-fail", :failed, %{is_error: true}, ^result}}

    assert :ok = Claude.close_session(handle)
  end

  test "applies stream filters locally without hiding the terminal result", %{
    client_session: client_session
  } do
    partial = Factory.stream_event_text_delta("hidden", session_id: "claude-filtered")
    result = ClaudeCode.Test.result("done", session_id: "claude-filtered")

    expect(ClientMock, :start_link, fn _opts -> {:ok, client_session} end)
    expect(ClientMock, :session_id, fn ^client_session -> nil end)

    expect(ClientMock, :stream, fn ^client_session, "Filtered", [] ->
      [partial, result]
    end)

    expect(ClientMock, :stop, fn ^client_session -> :ok end)

    sink = Sink.new(self())
    sink_ref = sink.ref
    {:ok, handle, _info} = Claude.open_session(config(), sink)
    turn = Turn.new("session-1", "Filtered", id: "turn-filtered")

    assert {:ok, "turn-filtered"} =
             Claude.start_turn(handle, turn, "Filtered", filter: :assistant)

    assert_receive {:agent_harness_provider, ^sink_ref, {:session_updated, _attrs}}

    assert_receive {:agent_harness_provider, ^sink_ref,
                    {:finish, "turn-filtered", :completed, %{text: "done"}, ^result}}

    refute_receive {:agent_harness_provider, ^sink_ref,
                    {:event, "turn-filtered", :message_delta, _data, ^partial}}

    assert :ok = Claude.close_session(handle)
  end

  test "validates approval responses and preserves the original tool input", %{
    client_session: client_session
  } do
    test_pid = self()
    result = ClaudeCode.Test.result("approved", session_id: "claude-approved")

    expect(ClientMock, :start_link, fn opts ->
      send(test_pid, {:callback, opts[:can_use_tool]})
      {:ok, client_session}
    end)

    expect(ClientMock, :session_id, fn ^client_session -> nil end)

    expect(ClientMock, :stream, fn ^client_session, "Approve command", [] ->
      receive do
        :complete_stream -> [result]
      end
    end)

    expect(ClientMock, :stop, fn ^client_session -> :ok end)

    sink = Sink.new(self())
    sink_ref = sink.ref
    {:ok, handle, _info} = Claude.open_session(config(), sink)
    assert_receive {:callback, callback}

    turn = Turn.new("session-1", "Approve command", id: "turn-approve")
    assert {:ok, "turn-approve"} = Claude.start_turn(handle, turn, "Approve command", [])

    tool_input = %{"command" => "mix test"}

    callback_task =
      Task.async(fn -> callback.(%{tool_name: "Bash", input: tool_input}, "tool-approve") end)

    assert_receive {:agent_harness_provider, ^sink_ref,
                    {:request, "turn-approve", provider_ref, _attrs, _raw}}

    assert {:error, {:invalid_response_action, :command_approval, :answer}} =
             Claude.respond(handle, provider_ref, Response.answer("yes"))

    assert Process.alive?(handle)

    assert {:error, {:unsupported_approval_scope, :session}} =
             Claude.respond(handle, provider_ref, Response.approve(scope: :session))

    assert :ok = Claude.respond(handle, provider_ref, Response.approve())
    assert {:allow, updated_input: ^tool_input} = Task.await(callback_task)

    send_runner(handle, :complete_stream)

    assert_receive {:agent_harness_provider, ^sink_ref,
                    {:finish, "turn-approve", :completed, _data, ^result}}

    assert :ok = Claude.close_session(handle)
  end

  test "runner failure rejects callbacks that would otherwise remain blocked", %{
    client_session: client_session
  } do
    test_pid = self()

    expect(ClientMock, :start_link, fn opts ->
      send(test_pid, {:callback, opts[:can_use_tool]})
      {:ok, client_session}
    end)

    expect(ClientMock, :session_id, fn ^client_session -> nil end)

    expect(ClientMock, :stream, fn ^client_session, "Wait for approval", [] ->
      receive do
        :never -> []
      end
    end)

    expect(ClientMock, :stop, fn ^client_session -> :ok end)

    sink = Sink.new(self())
    sink_ref = sink.ref
    {:ok, handle, _info} = Claude.open_session(config(), sink)
    monitor = Process.monitor(handle)
    assert_receive {:callback, callback}

    turn = Turn.new("session-1", "Wait for approval", id: "turn-runner-down")

    assert {:ok, "turn-runner-down"} =
             Claude.start_turn(handle, turn, "Wait for approval", [])

    callback_task =
      Task.async(fn ->
        callback.(%{tool_name: "Bash", input: %{"command" => "mix test"}}, "tool-runner-down")
      end)

    assert_receive {:agent_harness_provider, ^sink_ref,
                    {:request, "turn-runner-down", _provider_ref, _attrs, _raw}}

    runner = :sys.get_state(handle).runner
    Process.exit(runner, :kill)

    assert {:deny, decision_opts} = Task.await(callback_task)
    assert decision_opts[:interrupt] == true

    assert_receive {:agent_harness_provider, ^sink_ref,
                    {:finish, "turn-runner-down", :failed, %{reason: {:runner_down, :killed}},
                     nil}}

    assert_receive {:agent_harness_provider, ^sink_ref,
                    {:transport_down, {:claude_stream_failed, {:runner_down, :killed}}}}

    assert_receive {:DOWN, ^monitor, :process, ^handle, :normal}
  end

  test "cleans generated skill plugins when client startup raises" do
    test_pid = self()
    source = skill_fixture!()

    expect(ClientMock, :start_link, fn opts ->
      [generated_plugin] = opts[:plugins]
      send(test_pid, {:generated_plugin, generated_plugin})
      raise "invalid Claude client option"
    end)

    config =
      config(skills: [%{name: "test-skill", path: Path.join(source, "SKILL.md")}])

    assert {:error, _reason} = Claude.open_session(config, Sink.new(self()))
    assert_receive {:generated_plugin, generated_plugin}
    refute File.exists?(generated_plugin)
  end

  defp config(opts \\ []) do
    base = %SessionConfig{
      session_id: "session-1",
      provider: :claude,
      provider_options: %{client: ClientMock}
    }

    struct!(base, opts)
  end

  defp plugin_fixture! do
    path =
      Path.join(
        System.tmp_dir!(),
        "agent-harness-claude-plugin-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(Path.join(path, ".claude-plugin"))
    File.write!(Path.join([path, ".claude-plugin", "plugin.json"]), ~s({"name":"test"}))
    on_exit(fn -> File.rm_rf!(path) end)
    path
  end

  defp skill_fixture! do
    path =
      Path.join(
        System.tmp_dir!(),
        "agent-harness-claude-skill-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(path)
    File.write!(Path.join(path, "SKILL.md"), "# Test skill\n")
    on_exit(fn -> File.rm_rf!(path) end)
    path
  end

  defp send_runner(handle, message) do
    runner = :sys.get_state(handle).runner
    send(runner, message)
  end
end
