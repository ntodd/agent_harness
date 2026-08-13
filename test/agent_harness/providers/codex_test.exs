defmodule AgentHarness.Providers.CodexTest do
  use ExUnit.Case, async: false

  import Mox

  alias AgentHarness.{Capabilities, Response, SessionConfig, SessionRef, Turn}
  alias AgentHarness.Provider.Sink
  alias AgentHarness.Providers.Codex, as: CodexProvider
  alias AgentHarness.Providers.Codex.ClientMock
  alias Codex.Events
  alias Codex.Protocol.RequestUserInput.{Option, Question}

  setup :set_mox_global
  setup :verify_on_exit!

  setup do
    session = SessionRef.new(:codex, id: "session-1")
    sink = Sink.new(self())
    %{session: session, sink: sink}
  end

  test "marks provider call timeouts as uncertain", %{session: session} do
    previous_timeout = Application.get_env(:agent_harness, :codex_call_timeout, :not_configured)
    Application.put_env(:agent_harness, :codex_call_timeout, 10)
    runtime = spawn(fn -> Process.sleep(:infinity) end)

    on_exit(fn ->
      Process.exit(runtime, :kill)

      case previous_timeout do
        :not_configured -> Application.delete_env(:agent_harness, :codex_call_timeout)
        timeout -> Application.put_env(:agent_harness, :codex_call_timeout, timeout)
      end
    end)

    turn = Turn.new(session.id, "Work", id: "uncertain-turn")

    assert {:error, {:turn_start_uncertain, :provider_call_timeout}} =
             CodexProvider.start_turn(runtime, turn, "Work", [])

    assert {:error, {:provider_command_uncertain, :provider_call_timeout}} =
             CodexProvider.respond(runtime, :request, Response.answer("yes"))

    assert {:error, {:provider_command_uncertain, :provider_call_timeout}} =
             CodexProvider.cancel(runtime, :turn)
  end

  test "opens an isolated app-server and maps per-session configuration", %{
    session: session,
    sink: sink
  } do
    config =
      SessionConfig.new(session,
        cwd: "/work/repo",
        model: "gpt-5.1-codex",
        system_prompt: "Work carefully.",
        approval_policy: :on_request,
        sandbox: :workspace_write,
        env: %{"CODEX_HOME" => "/work/codex-home"},
        mcp_servers: %{
          "docs" => %{command: "docs-mcp", args: ["serve"]}
        },
        skills: [%{name: "release", path: "/work/skills/release/SKILL.md"}],
        provider_options: %{
          client: ClientMock,
          codex_options: %{codex_path_override: "/opt/bin/codex"},
          connect_options: [init_timeout_ms: 1_500],
          thread_options: %{web_search_enabled: true}
        }
      )

    expect(ClientMock, :options, fn %{codex_path_override: "/opt/bin/codex"} ->
      {:ok, :codex_options}
    end)

    expect(ClientMock, :connect, fn :codex_options, options ->
      assert options[:client_name] == "agent_harness"
      assert options[:client_title] == "AgentHarness"
      assert options[:experimental_api]
      assert options[:init_timeout_ms] == 1_500
      assert options[:cwd] == "/work/repo"

      assert options[:process_env] == %{
               "CODEX_HOME" => "/work/codex-home",
               "CODEX_API_KEY" => "",
               "CODEX_MODEL_PROVIDER" => "",
               "CODEX_OLLAMA_BASE_URL" => "",
               "CODEX_OSS_BASE_URL" => "",
               "CODEX_OSS_PROVIDER" => "",
               "CODEX_PROVIDER_BACKEND" => "",
               "OPENAI_API_KEY" => "",
               "OPENAI_BASE_URL" => ""
             }

      {:ok, :connection}
    end)

    expect(ClientMock, :disconnect, fn :connection -> :ok end)

    assert {:ok, runtime, %{provider_session_id: nil}} =
             CodexProvider.open_session(config, sink)

    assert %Capabilities{
             token_streaming: :native,
             questions: :native,
             approvals: :native,
             cancel: :native,
             resume: :native,
             per_session_mcp: :native,
             skills: :native
           } = CodexProvider.capabilities(runtime)

    turn = Turn.new(session.id, "Ship it", id: "turn-1")

    expect(ClientMock, :start_thread, fn :codex_options, options ->
      assert options.transport == {:app_server, :connection}
      assert options.working_directory == "/work/repo"
      assert options.model == "gpt-5.1-codex"
      assert options.developer_instructions == "Work carefully."
      assert options.ask_for_approval == :on_request
      assert options.sandbox == :workspace_write
      assert options.web_search_enabled
      assert options.skills_enabled
      assert options.model_provider == "openai"
      assert options.config["model_provider"] == "openai"

      assert options.config["mcp_servers"] == %{
               "docs" => %{"command" => "docs-mcp", "args" => ["serve"]}
             }

      {:ok, :thread}
    end)

    expect(ClientMock, :run_streamed, fn :thread, input, options ->
      assert input == [
               %{type: :skill, name: "release", path: "/work/skills/release/SKILL.md"},
               %{type: :text, text: "Ship it"}
             ]

      assert options.client_user_message_id == "turn-1"
      {:ok, :streaming}
    end)

    expect(ClientMock, :raw_events, fn :streaming ->
      [
        %Events.ThreadStarted{thread_id: "codex-thread-1"},
        %Events.TurnStarted{thread_id: "codex-thread-1", turn_id: "codex-turn-1"},
        %Events.ItemAgentMessageDelta{
          thread_id: "codex-thread-1",
          turn_id: "codex-turn-1",
          item: %{"text" => "Done"}
        },
        %Events.TurnCompleted{
          thread_id: "codex-thread-1",
          turn_id: "codex-turn-1",
          status: "completed",
          usage: %{"input_tokens" => 3}
        }
      ]
    end)

    assert {:ok, _provider_turn_ref} =
             CodexProvider.start_turn(runtime, turn, "Ship it", [])

    assert {:session_updated, %{provider_session_id: "codex-thread-1"}} =
             receive_provider(sink)

    assert {:event, "turn-1", :message_delta, %{text: "Done"}, _raw} =
             receive_provider(sink)

    assert {:finish, "turn-1", :completed,
            %{
              text: "Done",
              usage: %{"input_tokens" => 3},
              provider_session_id: "codex-thread-1",
              provider_turn_id: "codex-turn-1"
            }, _raw} = receive_provider(sink)

    assert :ok = CodexProvider.close_session(runtime)
  end

  test "resumes a known thread and routes all questions in one response", %{
    session: session,
    sink: sink
  } do
    config =
      SessionConfig.new(session,
        provider_options: %{
          client: ClientMock,
          provider_session_id: "existing-thread"
        }
      )

    expect_open()

    expect(ClientMock, :resume_thread, fn "existing-thread", :codex_options, options ->
      assert options.transport == {:app_server, :connection}
      {:ok, :thread}
    end)

    expect(ClientMock, :run_streamed, fn :thread, [%{type: :text, text: "Configure"}], _ ->
      {:ok, :streaming}
    end)

    question_event = %Events.RequestUserInput{
      id: 41,
      thread_id: "existing-thread",
      turn_id: "provider-turn",
      item_id: "question-item",
      questions: [
        %Question{
          id: "database",
          header: "Database",
          question: "Which database?",
          options: [
            %Option{label: "Postgres", description: "Use PostgreSQL"}
          ]
        },
        %Question{
          id: "region",
          header: "Region",
          question: "Which region?",
          is_other: true
        }
      ]
    }

    expect(ClientMock, :raw_events, fn :streaming ->
      delayed_end(
        [
          %Events.ThreadStarted{thread_id: "existing-thread"},
          %Events.TurnStarted{thread_id: "existing-thread", turn_id: "provider-turn"},
          question_event
        ],
        [
          %Events.TurnCompleted{
            thread_id: "existing-thread",
            turn_id: "provider-turn",
            status: "completed"
          }
        ]
      )
    end)

    expect(ClientMock, :respond, fn :connection, 41, payload ->
      assert payload == %{
               "answers" => %{
                 "database" => %{"answers" => ["Postgres"]},
                 "region" => %{"answers" => ["us-east-1"]}
               }
             }

      :ok
    end)

    expect(ClientMock, :disconnect, fn :connection -> :ok end)

    assert {:ok, runtime, %{provider_session_id: "existing-thread"}} =
             CodexProvider.open_session(config, sink)

    turn = Turn.new(session.id, "Configure", id: "turn-2")

    assert {:ok, _provider_turn_ref} =
             CodexProvider.start_turn(runtime, turn, "Configure", [])

    assert {:request, "turn-2", 41, attrs, ^question_event} = receive_provider(sink)

    assert attrs[:kind] == :question
    assert Enum.map(attrs[:questions], & &1.id) == ["database", "region"]

    assert :ok =
             CodexProvider.respond(
               runtime,
               41,
               Response.answer(%{
                 "database" => ["Postgres"],
                 "region" => "us-east-1"
               })
             )

    assert {:finish, "turn-2", :completed, _result, _raw} = receive_provider(sink)
    assert :ok = CodexProvider.close_session(runtime)
  end

  test "multiple pending question cancellations send only one turn interrupt", %{
    session: session,
    sink: sink
  } do
    test_pid = self()
    config = SessionConfig.new(session, provider_options: %{client: ClientMock})
    expect_open()

    first = %Events.RequestUserInput{
      id: 51,
      thread_id: "thread-questions",
      turn_id: "turn-questions",
      item_id: "first",
      questions: [%Question{id: "first", header: "First", question: "First?"}]
    }

    second = %Events.RequestUserInput{
      id: 52,
      thread_id: "thread-questions",
      turn_id: "turn-questions",
      item_id: "second",
      questions: [%Question{id: "second", header: "Second", question: "Second?"}]
    }

    expect(ClientMock, :start_thread, fn :codex_options, _ -> {:ok, :thread} end)
    expect(ClientMock, :run_streamed, fn :thread, _, _ -> {:ok, :streaming} end)

    expect(ClientMock, :raw_events, fn :streaming ->
      delayed_end(
        [
          %Events.ThreadStarted{thread_id: "thread-questions"},
          %Events.TurnStarted{
            thread_id: "thread-questions",
            turn_id: "turn-questions"
          },
          first,
          second
        ],
        [
          %Events.TurnCompleted{
            thread_id: "thread-questions",
            turn_id: "turn-questions",
            status: "interrupted"
          }
        ],
        300
      )
    end)

    expect(ClientMock, :turn_interrupt, 1, fn
      :connection, "thread-questions", "turn-questions" ->
        send(test_pid, :question_interrupt_started)
        Process.sleep(100)
        :ok
    end)

    expect(ClientMock, :disconnect, fn :connection -> :ok end)

    {:ok, runtime, _} = CodexProvider.open_session(config, sink)
    turn = Turn.new(session.id, "Ask", id: "local-questions")
    assert {:ok, _turn_ref} = CodexProvider.start_turn(runtime, turn, "Ask", [])

    assert {:session_updated, %{provider_session_id: "thread-questions"}} =
             receive_provider(sink)

    assert {:request, "local-questions", 51, _attrs, ^first} = receive_provider(sink)
    assert {:request, "local-questions", 52, _attrs, ^second} = receive_provider(sink)

    assert :ok = CodexProvider.respond(runtime, 51, Response.deny("stop"))
    assert :ok = CodexProvider.respond(runtime, 52, Response.cancel("stop"))
    assert_receive :question_interrupt_started

    assert {:finish, "local-questions", :interrupted, _result, _raw} =
             receive_provider(sink)

    assert :ok = CodexProvider.close_session(runtime)
  end

  test "normalizes approvals, interrupts cancellation, and requires terminal stream events", %{
    session: session,
    sink: sink
  } do
    test_pid = self()
    config = SessionConfig.new(session, provider_options: %{client: ClientMock})
    expect_open()

    approval = %Events.CommandApprovalRequested{
      id: "approval-1",
      thread_id: "thread-1",
      turn_id: "provider-turn-1",
      item_id: "command-1",
      reason: "Needs network",
      command: "mix deps.get",
      cwd: "/work"
    }

    expect(ClientMock, :start_thread, fn :codex_options, _ -> {:ok, :thread} end)
    expect(ClientMock, :run_streamed, fn :thread, _, _ -> {:ok, :streaming} end)

    expect(ClientMock, :raw_events, fn :streaming ->
      delayed_end(
        [
          %Events.ThreadStarted{thread_id: "thread-1"},
          %Events.TurnStarted{thread_id: "thread-1", turn_id: "provider-turn-1"},
          approval
        ],
        [],
        500
      )
    end)

    expect(ClientMock, :respond, fn :connection,
                                    "approval-1",
                                    %{"decision" => "acceptForSession"} ->
      :ok
    end)

    expect(ClientMock, :turn_interrupt, fn :connection, "thread-1", "provider-turn-1" ->
      send(test_pid, :interrupt_started)
      Process.sleep(150)
      :ok
    end)

    expect(ClientMock, :cancel_stream, fn :streaming, :immediate -> :ok end)
    expect(ClientMock, :disconnect, fn :connection -> :ok end)

    {:ok, runtime, _} = CodexProvider.open_session(config, sink)
    monitor = Process.monitor(runtime)
    turn = Turn.new(session.id, "Fetch", id: "turn-3")

    assert {:ok, provider_turn_ref} =
             CodexProvider.start_turn(runtime, turn, "Fetch", [])

    assert {:session_updated, %{provider_session_id: "thread-1"}} =
             receive_provider(sink)

    assert {:request, "turn-3", "approval-1", attrs, ^approval} = receive_provider(sink)

    assert attrs[:kind] == :command_approval
    assert attrs[:metadata].command == "mix deps.get"

    assert :ok =
             CodexProvider.respond(runtime, "approval-1", Response.approve(scope: :session))

    started_at = System.monotonic_time(:millisecond)
    assert :ok = CodexProvider.cancel(runtime, provider_turn_ref)
    assert System.monotonic_time(:millisecond) - started_at < 100
    assert_receive :interrupt_started

    assert {:finish, "turn-3", :failed, %{reason: :stream_ended_without_terminal_event}, nil} =
             receive_provider(sink)

    assert {:transport_down, {:stream_failure, :stream_ended_without_terminal_event}} =
             receive_provider(sink)

    assert_receive {:DOWN, ^monitor, :process, ^runtime, :normal}
    assert :ok = CodexProvider.close_session(runtime)
  end

  test "expires a pending request when Codex resolves it upstream", %{
    session: session,
    sink: sink
  } do
    config = SessionConfig.new(session, provider_options: %{client: ClientMock})
    expect_open()

    request = %Events.RequestUserInput{
      id: 71,
      thread_id: "thread-resolved",
      turn_id: "turn-resolved",
      item_id: "question",
      questions: [
        %Question{id: "choice", header: "Choice", question: "Continue?"}
      ]
    }

    resolved = %Events.ServerRequestResolved{
      thread_id: "thread-resolved",
      request_id: 71
    }

    expect(ClientMock, :start_thread, fn :codex_options, _ -> {:ok, :thread} end)
    expect(ClientMock, :run_streamed, fn :thread, _, _ -> {:ok, :streaming} end)

    expect(ClientMock, :raw_events, fn :streaming ->
      delayed_end(
        [
          %Events.ThreadStarted{thread_id: "thread-resolved"},
          %Events.TurnStarted{
            thread_id: "thread-resolved",
            turn_id: "turn-resolved"
          },
          request,
          resolved
        ],
        [
          %Events.TurnCompleted{
            thread_id: "thread-resolved",
            turn_id: "turn-resolved",
            status: "completed"
          }
        ],
        250
      )
    end)

    expect(ClientMock, :disconnect, fn :connection -> :ok end)

    {:ok, runtime, _} = CodexProvider.open_session(config, sink)
    turn = Turn.new(session.id, "Wait for resolution", id: "local-resolved")
    assert {:ok, _turn_ref} = CodexProvider.start_turn(runtime, turn, "Wait", [])

    assert {:session_updated, %{provider_session_id: "thread-resolved"}} =
             receive_provider(sink)

    assert {:request, "local-resolved", 71, _attrs, ^request} = receive_provider(sink)

    assert {:expire_request, "local-resolved", 71, :provider_resolved, ^resolved} =
             receive_provider(sink)

    assert {:event, "local-resolved", :provider_request_resolved, %{provider_request_ref: 71},
            ^resolved} = receive_provider(sink)

    assert {:error, :request_not_found} =
             CodexProvider.respond(runtime, 71, Response.answer("too late"))

    assert {:finish, "local-resolved", :completed, _result, _raw} =
             receive_provider(sink)

    assert :ok = CodexProvider.close_session(runtime)
  end

  test "fails closed and tears down unresolvable auth and attestation requests", %{
    session: session,
    sink: sink
  } do
    events = [
      {%Events.ChatgptAuthTokensRefreshRequested{id: 81, reason: "expired"},
       :chatgpt_auth_tokens_refresh},
      {%Events.AttestationGenerateRequested{id: 82}, :attestation_generate}
    ]

    Enum.with_index(events, 1)
    |> Enum.each(fn {{unsupported, request_type}, index} ->
      config = SessionConfig.new(session, provider_options: %{client: ClientMock})
      expect_open()

      expect(ClientMock, :start_thread, fn :codex_options, _ -> {:ok, :thread} end)
      expect(ClientMock, :run_streamed, fn :thread, _, _ -> {:ok, :streaming} end)

      expect(ClientMock, :raw_events, fn :streaming ->
        [
          %Events.ThreadStarted{thread_id: "thread-unsupported-#{index}"},
          %Events.TurnStarted{
            thread_id: "thread-unsupported-#{index}",
            turn_id: "turn-unsupported-#{index}"
          },
          unsupported
        ]
      end)

      expect(ClientMock, :disconnect, fn :connection -> :ok end)

      {:ok, runtime, _} = CodexProvider.open_session(config, sink)
      monitor = Process.monitor(runtime)
      local_turn_id = "local-unsupported-#{index}"
      turn = Turn.new(session.id, "Trigger request", id: local_turn_id)
      assert {:ok, _turn_ref} = CodexProvider.start_turn(runtime, turn, "Trigger", [])

      assert {:session_updated, %{provider_session_id: "thread-unsupported-" <> _}} =
               receive_provider(sink)

      reason = {:unsupported_provider_request, request_type}

      assert {:finish, ^local_turn_id, :failed, %{reason: ^reason}, ^unsupported} =
               receive_provider(sink)

      assert {:transport_down, ^reason} = receive_provider(sink)
      assert_receive {:DOWN, ^monitor, :process, ^runtime, _reason}
    end)
  end

  test "tears down an auth request that arrives before lazy turn subscription", %{
    session: session,
    sink: sink
  } do
    config = SessionConfig.new(session, provider_options: %{client: ClientMock})
    expect_open()

    expect(ClientMock, :start_thread, fn :codex_options, _ -> {:ok, :thread} end)
    expect(ClientMock, :run_streamed, fn :thread, _, _ -> {:ok, :streaming} end)

    expect(ClientMock, :raw_events, fn :streaming ->
      delayed_end([], [], 1_000)
    end)

    expect(ClientMock, :disconnect, fn :connection -> :ok end)

    {:ok, runtime, _} = CodexProvider.open_session(config, sink)
    monitor = Process.monitor(runtime)
    turn = Turn.new(session.id, "Start", id: "local-early-auth")
    assert {:ok, _turn_ref} = CodexProvider.start_turn(runtime, turn, "Start", [])

    params = %{"reason" => "expired"}

    send(
      runtime,
      {:codex_unresolvable_request, 83, "account/chatgptAuthTokens/refresh", params}
    )

    raw = %{
      id: 83,
      method: "account/chatgptAuthTokens/refresh",
      params: params
    }

    reason = {:unsupported_provider_request, :chatgpt_auth_tokens_refresh}

    assert {:finish, "local-early-auth", :failed, %{reason: ^reason}, ^raw} =
             receive_provider(sink)

    assert {:transport_down, ^reason} = receive_provider(sink)
    assert_receive {:DOWN, ^monitor, :process, ^runtime, :normal}
  end

  test "an asynchronous interrupt failure fails the turn and connection", %{
    session: session,
    sink: sink
  } do
    config = SessionConfig.new(session, provider_options: %{client: ClientMock})
    expect_open()

    expect(ClientMock, :start_thread, fn :codex_options, _ -> {:ok, :thread} end)
    expect(ClientMock, :run_streamed, fn :thread, _, _ -> {:ok, :streaming} end)

    expect(ClientMock, :raw_events, fn :streaming ->
      delayed_end(
        [
          %Events.ThreadStarted{thread_id: "thread-cancel-failure"},
          %Events.TurnStarted{
            thread_id: "thread-cancel-failure",
            turn_id: "turn-cancel-failure"
          }
        ],
        [],
        1_000
      )
    end)

    expect(ClientMock, :turn_interrupt, fn
      :connection, "thread-cancel-failure", "turn-cancel-failure" ->
        {:error, :interrupt_rejected}
    end)

    expect(ClientMock, :disconnect, fn :connection -> :ok end)

    {:ok, runtime, _} = CodexProvider.open_session(config, sink)
    monitor = Process.monitor(runtime)
    turn = Turn.new(session.id, "Cancel", id: "local-cancel-failure")
    assert {:ok, turn_ref} = CodexProvider.start_turn(runtime, turn, "Cancel", [])

    assert {:session_updated, %{provider_session_id: "thread-cancel-failure"}} =
             receive_provider(sink)

    assert :ok = CodexProvider.cancel(runtime, turn_ref)

    failure = {:cancel_failed, :interrupt_rejected}

    assert {:finish, "local-cancel-failure", :failed, %{reason: ^failure}, nil} =
             receive_provider(sink)

    assert {:transport_down, ^failure} = receive_provider(sink)
    assert_receive {:DOWN, ^monitor, :process, ^runtime, _reason}
  end

  test "a nonterminal stream failure tears down the connection before another turn", %{
    session: session,
    sink: sink
  } do
    config = SessionConfig.new(session, provider_options: %{client: ClientMock})
    expect_open()

    expect(ClientMock, :start_thread, fn :codex_options, _ -> {:ok, :thread} end)
    expect(ClientMock, :run_streamed, fn :thread, _, _ -> {:ok, :streaming} end)

    expect(ClientMock, :raw_events, fn :streaming ->
      Stream.concat(
        [
          %Events.ThreadStarted{thread_id: "thread-stream-failure"},
          %Events.TurnStarted{
            thread_id: "thread-stream-failure",
            turn_id: "turn-stream-failure"
          }
        ],
        Stream.map([:fail], fn :fail -> raise "reader exploded" end)
      )
    end)

    expect(ClientMock, :cancel_stream, fn :streaming, :immediate -> :ok end)
    expect(ClientMock, :disconnect, fn :connection -> :ok end)

    {:ok, runtime, _} = CodexProvider.open_session(config, sink)
    monitor = Process.monitor(runtime)
    turn = Turn.new(session.id, "Read", id: "local-stream-failure")
    assert {:ok, _turn_ref} = CodexProvider.start_turn(runtime, turn, "Read", [])

    assert {:session_updated, %{provider_session_id: "thread-stream-failure"}} =
             receive_provider(sink)

    stream_reason = {:stream_error, "reader exploded"}

    assert {:finish, "local-stream-failure", :failed, %{reason: ^stream_reason}, nil} =
             receive_provider(sink)

    assert {:transport_down, {:stream_failure, ^stream_reason}} =
             receive_provider(sink)

    assert_receive {:DOWN, ^monitor, :process, ^runtime, :normal}

    next_turn = Turn.new(session.id, "Overlap", id: "must-not-start")

    assert {:error, :provider_not_found} =
             CodexProvider.start_turn(runtime, next_turn, "Overlap", [])
  end

  test "a completion timeout tears down the connection", %{
    session: session,
    sink: sink
  } do
    config = SessionConfig.new(session, provider_options: %{client: ClientMock})
    expect_open()

    expect(ClientMock, :start_thread, fn :codex_options, _ -> {:ok, :thread} end)
    expect(ClientMock, :run_streamed, fn :thread, _, _ -> {:ok, :streaming} end)

    expect(ClientMock, :raw_events, fn :streaming ->
      Stream.concat(
        [
          %Events.ThreadStarted{thread_id: "thread-timeout"},
          %Events.TurnStarted{thread_id: "thread-timeout", turn_id: "turn-timeout"}
        ],
        Stream.map([:timeout], fn :timeout -> throw({:completion_timeout, 50}) end)
      )
    end)

    expect(ClientMock, :cancel_stream, fn :streaming, :immediate -> :ok end)
    expect(ClientMock, :disconnect, fn :connection -> :ok end)

    {:ok, runtime, _} = CodexProvider.open_session(config, sink)
    monitor = Process.monitor(runtime)
    turn = Turn.new(session.id, "Timeout", id: "local-timeout")
    assert {:ok, _turn_ref} = CodexProvider.start_turn(runtime, turn, "Timeout", [])

    assert {:session_updated, %{provider_session_id: "thread-timeout"}} =
             receive_provider(sink)

    stream_reason = {:stream_error, {:throw, {:completion_timeout, 50}}}

    assert {:finish, "local-timeout", :failed, %{reason: ^stream_reason}, nil} =
             receive_provider(sink)

    assert {:transport_down, {:stream_failure, ^stream_reason}} =
             receive_provider(sink)

    assert_receive {:DOWN, ^monitor, :process, ^runtime, :normal}
  end

  test "an untrappable runner exit tears down the connection", %{
    session: session,
    sink: sink
  } do
    config = SessionConfig.new(session, provider_options: %{client: ClientMock})
    expect_open()

    expect(ClientMock, :start_thread, fn :codex_options, _ -> {:ok, :thread} end)
    expect(ClientMock, :run_streamed, fn :thread, _, _ -> {:ok, :streaming} end)

    expect(ClientMock, :raw_events, fn :streaming ->
      Stream.concat(
        [
          %Events.ThreadStarted{thread_id: "thread-runner-down"},
          %Events.TurnStarted{
            thread_id: "thread-runner-down",
            turn_id: "turn-runner-down"
          }
        ],
        Stream.map([:kill], fn :kill -> Process.exit(self(), :kill) end)
      )
    end)

    expect(ClientMock, :cancel_stream, fn :streaming, :immediate -> :ok end)
    expect(ClientMock, :disconnect, fn :connection -> :ok end)

    {:ok, runtime, _} = CodexProvider.open_session(config, sink)
    monitor = Process.monitor(runtime)
    turn = Turn.new(session.id, "Crash", id: "local-runner-down")
    assert {:ok, _turn_ref} = CodexProvider.start_turn(runtime, turn, "Crash", [])

    assert {:session_updated, %{provider_session_id: "thread-runner-down"}} =
             receive_provider(sink)

    stream_reason = {:stream_task_down, :killed}

    assert {:finish, "local-runner-down", :failed, %{reason: ^stream_reason}, nil} =
             receive_provider(sink)

    assert {:transport_down, {:stream_failure, ^stream_reason}} =
             receive_provider(sink)

    assert_receive {:DOWN, ^monitor, :process, ^runtime, :normal}
  end

  test "defers an early cancel until Codex publishes provider turn identifiers", %{
    session: session,
    sink: sink
  } do
    config = SessionConfig.new(session, provider_options: %{client: ClientMock})
    expect_open()

    expect(ClientMock, :start_thread, fn :codex_options, _ -> {:ok, :thread} end)
    expect(ClientMock, :run_streamed, fn :thread, _, _ -> {:ok, :streaming} end)

    expect(ClientMock, :raw_events, fn :streaming ->
      delayed_end(
        [
          %Events.ThreadStarted{thread_id: "thread-early"},
          %Events.TurnStarted{thread_id: "thread-early", turn_id: "turn-early"}
        ],
        [
          %Events.TurnCompleted{
            thread_id: "thread-early",
            turn_id: "turn-early",
            status: "interrupted"
          }
        ]
      )
    end)

    expect(ClientMock, :turn_interrupt, fn :connection, "thread-early", "turn-early" ->
      :ok
    end)

    expect(ClientMock, :disconnect, fn :connection -> :ok end)

    {:ok, runtime, _} = CodexProvider.open_session(config, sink)
    turn = Turn.new(session.id, "Wait", id: "turn-early-local")

    assert {:ok, provider_turn_ref} =
             CodexProvider.start_turn(runtime, turn, "Wait", [])

    assert :ok = CodexProvider.cancel(runtime, provider_turn_ref)

    assert {:session_updated, %{provider_session_id: "thread-early"}} =
             receive_provider(sink)

    assert {:finish, "turn-early-local", :interrupted, result, _raw} =
             receive_provider(sink)

    assert result.provider_turn_id == "turn-early"
    assert :ok = CodexProvider.close_session(runtime)
  end

  test ":sys.get_status on a live session never leaks credentials", %{
    session: session,
    sink: sink
  } do
    secret = "sk-codex-live-secret"

    config =
      SessionConfig.new(session,
        env: %{"OPENAI_API_KEY" => secret},
        provider_options: %{
          client: ClientMock,
          auth: :inherit,
          codex_options: %{api_key: secret}
        }
      )

    expect(ClientMock, :options, fn %{api_key: ^secret} ->
      {:ok, %Codex.Options{api_key: secret}}
    end)

    expect(ClientMock, :connect, fn %Codex.Options{}, _options -> {:ok, :connection} end)
    expect(ClientMock, :disconnect, fn :connection -> :ok end)

    assert {:ok, runtime, _info} = CodexProvider.open_session(config, sink)

    # Crash reports render the state with Erlang ~p formatting, which
    # bypasses the Inspect protocol, so the raw term must be scrubbed.
    rendered =
      ~c"~p"
      |> :io_lib.format([:sys.get_status(runtime)])
      |> IO.iodata_to_binary()

    refute rendered =~ secret

    assert :ok = CodexProvider.close_session(runtime)
  end

  defp expect_open do
    expect(ClientMock, :options, fn %{} -> {:ok, :codex_options} end)

    expect(ClientMock, :connect, fn :codex_options, _options ->
      {:ok, :connection}
    end)
  end

  defp receive_provider(%Sink{ref: ref}) do
    assert_receive {:agent_harness_provider, ^ref, actual}, 1_000
    actual
  end

  defp delayed_end(initial_events, final_events, delay \\ 100) do
    Stream.resource(
      fn -> {:initial, initial_events, final_events} end,
      fn
        {:initial, events, final} ->
          {events, {:wait, final}}

        {:wait, final} ->
          Process.sleep(delay)
          {final, :done}

        :done ->
          {:halt, :done}
      end,
      fn _ -> :ok end
    )
  end
end
