defmodule AgentHarness.Providers.Codex.ProtocolTest do
  use ExUnit.Case, async: true

  alias AgentHarness.Providers.Codex.Protocol
  alias AgentHarness.Response
  alias Codex.Events
  alias Codex.Protocol.RequestPermissions
  alias Codex.Protocol.RequestUserInput.Question

  test "requires one answer for every question in a user-input request" do
    event = %Events.RequestUserInput{
      id: 1,
      questions: [
        %Question{id: "one", header: "One", question: "First?"},
        %Question{id: "two", header: "Two", question: "Second?"}
      ]
    }

    assert {:error, {:missing_answer, "two"}} =
             Protocol.encode_response(event, Response.answer(%{"one" => "yes"}))

    assert {:ok,
            %{
              "answers" => %{
                "one" => %{"answers" => ["yes"]},
                "two" => %{"answers" => ["2", "three"]}
              }
            }} =
             Protocol.encode_response(
               event,
               Response.answer(%{one: :yes, two: [2, "three"]})
             )
  end

  test "encodes command and file approval decisions with scope" do
    command = %Events.CommandApprovalRequested{
      id: 1,
      thread_id: "thread",
      turn_id: "turn",
      item_id: "command",
      available_decisions: ["accept", "decline"]
    }

    file = %Events.FileApprovalRequested{
      id: 2,
      thread_id: "thread",
      turn_id: "turn",
      item_id: "file"
    }

    assert {:error, {:decision_not_available, "acceptForSession"}} =
             Protocol.encode_response(command, Response.approve(scope: :session))

    assert {:ok, %{"decision" => "accept"}} =
             Protocol.encode_response(command, Response.approve())

    assert {:ok, %{"decision" => "decline"}} =
             Protocol.encode_response(file, Response.deny("no"))

    assert {:ok, %{"decision" => "cancel"}} =
             Protocol.encode_response(file, Response.cancel())
  end

  test "grants exactly the requested structured permissions" do
    requested =
      RequestPermissions.RequestPermissionProfile.from_map(%{
        "fileSystem" => %{"write" => ["/work/generated"]}
      })

    event = %Events.PermissionsApprovalRequested{
      id: 3,
      thread_id: "thread",
      turn_id: "turn",
      item_id: "permissions",
      permissions: requested
    }

    assert {:ok,
            %{
              "permissions" => %{
                "fileSystem" => %{"write" => ["/work/generated"]}
              },
              "scope" => "session"
            }} =
             Protocol.encode_response(event, Response.approve(scope: :session))

    assert {:ok, %{"permissions" => %{}, "scope" => "turn"}} =
             Protocol.encode_response(event, Response.deny())

    assert {:ok,
            %{
              "permissions" => %{
                "fileSystem" => %{"write" => ["/work/generated"]}
              },
              "scope" => "turn"
            }} =
             Protocol.encode_response(
               event,
               %Response{
                 action: :approve,
                 value: %{
                   network: %{enabled: true},
                   fileSystem: %{write: ["/work/generated", "/etc"]}
                 }
               }
             )
  end

  test "encodes MCP elicitation and dynamic host-tool responses" do
    elicitation = %Events.McpElicitationRequested{
      id: 4,
      thread_id: "thread",
      server_name: "crm",
      request: %{}
    }

    dynamic_tool = %Events.DynamicToolCallRequested{
      id: 5,
      thread_id: "thread",
      turn_id: "turn",
      call_id: "call",
      tool_name: "lookup"
    }

    assert {:ok, %{"action" => "accept", "content" => %{"account" => "Acme"}}} =
             Protocol.encode_response(
               elicitation,
               Response.answer(%{"account" => "Acme"})
             )

    assert {:ok, %{"action" => "decline"}} =
             Protocol.encode_response(elicitation, Response.deny())

    assert {:ok,
            %{
              "success" => true,
              "contentItems" => [%{"type" => "inputText", "text" => output}]
            }} =
             Protocol.encode_response(
               dynamic_tool,
               Response.answer(%{records: [%{id: 1}]})
             )

    assert {:ok, %{"records" => [%{"id" => 1}]}} = JSON.decode(output)

    assert {:ok,
            %{
              "success" => true,
              "contentItems" => [%{"type" => "inputText", "text" => "plain text"}]
            }} =
             Protocol.encode_response(dynamic_tool, Response.answer("plain text"))

    native_envelope = %{
      success: true,
      contentItems: [
        %{type: "inputImage", imageUrl: "data:image/png;base64,AA=="}
      ]
    }

    assert {:ok,
            %{
              "success" => true,
              "contentItems" => [
                %{"type" => "inputImage", "imageUrl" => "data:image/png;base64,AA=="}
              ]
            }} =
             Protocol.encode_response(dynamic_tool, Response.answer(native_envelope))
  end
end
