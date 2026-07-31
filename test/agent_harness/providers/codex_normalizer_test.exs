defmodule AgentHarness.Providers.Codex.NormalizerTest do
  use ExUnit.Case, async: true

  alias AgentHarness.Providers.Codex.Normalizer
  alias Codex.Events
  alias Codex.Protocol.RequestUserInput.{Option, Question}

  test "normalizes streaming deltas, plans, usage, and completed items" do
    assert {:event, :message_delta, %{text: "hello"}} =
             Normalizer.normalize(%Events.ItemAgentMessageDelta{item: %{"text" => "hello"}})

    assert {:event, :command_output_delta, %{item_id: "command", text: "compiled"}} =
             Normalizer.normalize(%Events.CommandOutputDelta{
               item_id: "command",
               delta: "compiled"
             })

    assert {:event, :plan_updated, %{explanation: "Next", plan: [%{step: "Test"}]}} =
             Normalizer.normalize(%Events.TurnPlanUpdated{
               explanation: "Next",
               plan: [%{step: "Test"}]
             })

    assert {:event, :usage_updated,
            %{usage: %{"input_tokens" => 2}, delta: nil, rate_limits: nil}} =
             Normalizer.normalize(%Events.ThreadTokenUsageUpdated{
               usage: %{"input_tokens" => 2}
             })
  end

  test "keeps multi-question shape and rich approval metadata" do
    question = %Events.RequestUserInput{
      id: 10,
      thread_id: "thread",
      turn_id: "turn",
      item_id: "item",
      questions: [
        %Question{
          id: "database",
          header: "Database",
          question: "Which database?",
          is_other: true,
          options: [%Option{label: "Postgres", description: "Recommended"}]
        }
      ]
    }

    assert {:request, 10, attrs} = Normalizer.normalize(question)
    assert attrs[:kind] == :question

    assert [
             %{
               id: "database",
               prompt: "Which database?",
               allows_other: true,
               choices: [
                 %{label: "Postgres", value: "Postgres", description: "Recommended"}
               ]
             }
           ] = attrs[:questions]

    approval = %Events.CommandApprovalRequested{
      id: 11,
      thread_id: "thread",
      turn_id: "turn",
      item_id: "command",
      command: "git status",
      cwd: "/work",
      proposed_execpolicy_amendment: ["git", "status"],
      available_decisions: [
        "accept",
        "decline",
        %{
          "acceptWithExecpolicyAmendment" => %{
            "execpolicyAmendment" => ["git", "status"]
          }
        }
      ]
    }

    assert {:request, 11, approval_attrs} = Normalizer.normalize(approval)
    assert approval_attrs[:kind] == :command_approval
    assert approval_attrs[:metadata].command == "git status"
    assert approval_attrs[:metadata].proposed_execpolicy_amendment == ["git", "status"]

    assert approval_attrs[:choices] == [
             %{label: "Approve once", value: :approve},
             %{label: "Deny", value: :deny},
             %{
               label: "Approve with exec policy amendment",
               value: %{
                 "acceptWithExecpolicyAmendment" => %{
                   "execpolicyAmendment" => ["git", "status"]
                 }
               }
             }
           ]
  end

  test "offers the session scope supported by file and permission approval protocols" do
    file = %Events.FileApprovalRequested{
      id: 12,
      thread_id: "thread",
      turn_id: "turn",
      item_id: "file"
    }

    assert {:request, 12, attrs} = Normalizer.normalize(file)

    assert attrs[:choices] == [
             %{label: "Approve once", value: :approve},
             %{label: "Approve for session", value: :approve_for_session},
             %{label: "Deny", value: :deny},
             %{label: "Cancel", value: :cancel}
           ]
  end

  test "maps authoritative terminal statuses and detects terminal events" do
    completed = %Events.TurnCompleted{
      thread_id: "thread",
      turn_id: "turn",
      status: "completed"
    }

    failed = %Events.TurnCompleted{
      thread_id: "thread",
      turn_id: "turn",
      status: "failed",
      error: %{"message" => "boom"}
    }

    interrupted = %Events.TurnAborted{turn_id: "turn", reason: :cancelled}

    assert {:finish, :completed, %{provider_session_id: "thread", provider_turn_id: "turn"}} =
             Normalizer.normalize(completed)

    assert {:finish, :failed,
            %{
              provider_session_id: "thread",
              provider_turn_id: "turn",
              error: %{"message" => "boom"}
            }} = Normalizer.normalize(failed)

    assert {:finish, :interrupted, %{provider_turn_id: "turn", reason: :cancelled}} =
             Normalizer.normalize(interrupted)

    assert Normalizer.terminal?(completed)
    assert Normalizer.terminal?(failed)
    assert Normalizer.terminal?(interrupted)
    refute Normalizer.terminal?(%Events.TurnStarted{})
  end
end
