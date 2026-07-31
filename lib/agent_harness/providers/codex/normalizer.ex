defmodule AgentHarness.Providers.Codex.Normalizer do
  @moduledoc false

  alias AgentHarness.Providers.Codex.Protocol
  alias Codex.Events

  @type normalized ::
          :ignore
          | {:session_updated, map()}
          | {:event, atom(), term()}
          | {:request, String.t() | integer(), keyword()}
          | {:finish, :completed | :failed | :cancelled | :interrupted, map()}

  @spec normalize(term()) :: normalized()
  def normalize(%Events.ThreadStarted{thread_id: thread_id}) do
    {:session_updated, %{provider_session_id: thread_id}}
  end

  def normalize(%Events.TurnStarted{}), do: :ignore

  def normalize(%Events.ItemAgentMessageDelta{item: item}) do
    {:event, :message_delta, %{text: fetch(item, :text) || ""}}
  end

  def normalize(%Events.CommandOutputDelta{} = event) do
    {:event, :command_output_delta, %{item_id: event.item_id, text: event.delta}}
  end

  def normalize(%Events.FileChangeOutputDelta{} = event) do
    {:event, :file_change_output_delta, %{item_id: event.item_id, text: event.delta}}
  end

  def normalize(%Events.ReasoningDelta{} = event) do
    {:event, :reasoning_delta,
     %{item_id: event.item_id, text: event.delta, content_index: event.content_index}}
  end

  def normalize(%Events.ReasoningSummaryDelta{} = event) do
    {:event, :reasoning_summary_delta,
     %{item_id: event.item_id, text: event.delta, summary_index: event.summary_index}}
  end

  def normalize(%Events.TurnPlanUpdated{} = event) do
    {:event, :plan_updated, %{explanation: event.explanation, plan: event.plan}}
  end

  def normalize(%Events.TurnDiffUpdated{} = event) do
    {:event, :diff_updated, %{diff: event.diff}}
  end

  def normalize(%Events.ThreadTokenUsageUpdated{} = event) do
    {:event, :usage_updated,
     %{usage: event.usage, delta: event.delta, rate_limits: event.rate_limits}}
  end

  def normalize(%Events.ItemStarted{item: item}),
    do: {:event, :item_started, %{item: item}}

  def normalize(%Events.ItemUpdated{item: item}),
    do: {:event, :item_updated, %{item: item}}

  def normalize(%Events.ItemCompleted{item: item}),
    do: {:event, :item_completed, %{item: item}}

  def normalize(%Events.McpToolCallProgress{} = event) do
    {:event, :mcp_progress, %{item_id: event.item_id, message: event.message}}
  end

  def normalize(%Events.Warning{message: message}),
    do: {:event, :warning, %{message: message}}

  def normalize(%Events.ConfigWarning{} = event),
    do: {:event, :warning, %{message: event.summary, details: event.details}}

  def normalize(%Events.Error{} = event) do
    {:event, :provider_error,
     %{
       message: event.message,
       details: event.additional_details,
       error_info: event.codex_error_info,
       will_retry: event.will_retry
     }}
  end

  def normalize(%Events.RequestUserInput{} = event) do
    questions = Protocol.questions(event.questions)

    prompt =
      case questions do
        [%{prompt: prompt}] -> prompt
        questions -> "Codex needs answers to #{length(questions)} questions."
      end

    {:request, event.id,
     [
       kind: :question,
       prompt: prompt,
       questions: questions,
       metadata: %{
         provider_item_id: event.item_id,
         provider_turn_id: event.turn_id,
         provider_session_id: event.thread_id
       }
     ]}
  end

  def normalize(%Events.CommandApprovalRequested{} = event) do
    {:request, event.id,
     [
       kind: :command_approval,
       prompt: event.reason || command_prompt(event.command),
       choices: approval_choices(event.available_decisions),
       metadata: %{
         command: event.command,
         cwd: event.cwd,
         command_actions: event.command_actions,
         network_approval_context: event.network_approval_context,
         additional_permissions: event.additional_permissions,
         skill_metadata: event.skill_metadata,
         proposed_execpolicy_amendment: event.proposed_execpolicy_amendment,
         proposed_network_policy_amendments: event.proposed_network_policy_amendments,
         available_decisions: event.available_decisions,
         provider_item_id: event.item_id,
         provider_turn_id: event.turn_id,
         provider_session_id: event.thread_id
       }
     ]}
  end

  def normalize(%Events.FileApprovalRequested{} = event) do
    {:request, event.id,
     [
       kind: :file_change_approval,
       prompt: event.reason || "Codex wants to modify files.",
       choices: approval_choices(nil),
       metadata: %{
         grant_root: event.grant_root,
         provider_item_id: event.item_id,
         provider_turn_id: event.turn_id,
         provider_session_id: event.thread_id
       }
     ]}
  end

  def normalize(%Events.PermissionsApprovalRequested{} = event) do
    {:request, event.id,
     [
       kind: :permission,
       prompt: event.reason || "Codex requests additional permissions.",
       choices: approval_choices(nil),
       schema: permission_schema(event.permissions),
       metadata: %{
         requested_permissions: event.permissions,
         provider_item_id: event.item_id,
         provider_turn_id: event.turn_id,
         provider_session_id: event.thread_id
       }
     ]}
  end

  def normalize(%Events.McpElicitationRequested{} = event) do
    {:request, event.id,
     [
       kind: :mcp_elicitation,
       prompt: event.message || "MCP server #{event.server_name} needs input.",
       schema: fetch(event.request, :requested_schema) || fetch(event.request, :requestedSchema),
       metadata: %{
         server_name: event.server_name,
         mode: event.request_mode,
         request: event.request,
         provider_turn_id: event.turn_id,
         provider_session_id: event.thread_id
       }
     ]}
  end

  def normalize(%Events.DynamicToolCallRequested{} = event) do
    {:request, event.id,
     [
       kind: :confirmation,
       prompt: "Codex wants the host to execute #{event.tool_name}.",
       metadata: %{
         call_id: event.call_id,
         tool_name: event.tool_name,
         arguments: event.arguments,
         provider_turn_id: event.turn_id,
         provider_session_id: event.thread_id
       }
     ]}
  end

  def normalize(%Events.CurrentTimeReadRequested{} = event) do
    {:request, event.id,
     [
       kind: :confirmation,
       prompt: "Codex requests the current Unix time.",
       metadata: %{
         request_type: :current_time,
         provider_session_id: event.thread_id
       }
     ]}
  end

  def normalize(%Events.ServerRequestResolved{request_id: request_id}) do
    {:event, :provider_request_resolved, %{provider_request_ref: request_id}}
  end

  def normalize(%Events.TurnCompleted{} = event) do
    status = terminal_status(event.status, event.error)

    {:finish, status,
     %{
       usage: event.usage,
       error: event.error,
       provider_session_id: event.thread_id,
       provider_turn_id: event.turn_id,
       response_id: event.response_id,
       duration_ms: event.duration_ms,
       time_to_first_token_ms: event.time_to_first_token_ms
     }
     |> compact()}
  end

  def normalize(%Events.TurnFailed{} = event) do
    {:finish, :failed,
     %{
       reason: event.error,
       provider_session_id: event.thread_id,
       provider_turn_id: event.turn_id
     }
     |> compact()}
  end

  def normalize(%Events.TurnAborted{} = event) do
    {:finish, :interrupted, %{reason: event.reason, provider_turn_id: event.turn_id} |> compact()}
  end

  def normalize(%Events.AppServerNotification{} = event) do
    {:event, :provider_event, %{method: event.method, params: event.params}}
  end

  def normalize(%Events.Unknown{} = event) do
    {:event, :provider_event, %{type: event.type, data: event.raw}}
  end

  def normalize(event) when is_struct(event) do
    {:event, :provider_event, %{event: event}}
  end

  def normalize(_event), do: :ignore

  @spec terminal?(term()) :: boolean()
  def terminal?(%Events.TurnCompleted{}), do: true
  def terminal?(%Events.TurnFailed{}), do: true
  def terminal?(%Events.TurnAborted{}), do: true
  def terminal?(_event), do: false

  defp terminal_status(status, _error) when status in [:failed, "failed"], do: :failed

  defp terminal_status(status, _error) when status in [:interrupted, "interrupted"],
    do: :interrupted

  defp terminal_status(status, _error)
       when status in [:cancelled, :canceled, "cancelled", "canceled"],
       do: :cancelled

  defp terminal_status(_status, error) when not is_nil(error), do: :failed
  defp terminal_status(_status, _error), do: :completed

  defp command_prompt(nil), do: "Codex wants to run a command."
  defp command_prompt(command), do: "Codex wants to run: #{command}"

  defp approval_choices(nil) do
    [
      %{label: "Approve once", value: :approve},
      %{label: "Approve for session", value: :approve_for_session},
      %{label: "Deny", value: :deny},
      %{label: "Cancel", value: :cancel}
    ]
  end

  defp approval_choices(decisions) when is_list(decisions) do
    Enum.map(decisions, &approval_choice/1)
  end

  defp approval_choice("accept"), do: %{label: "Approve once", value: :approve}

  defp approval_choice("acceptForSession"),
    do: %{label: "Approve for session", value: :approve_for_session}

  defp approval_choice("decline"), do: %{label: "Deny", value: :deny}
  defp approval_choice("cancel"), do: %{label: "Cancel", value: :cancel}

  defp approval_choice(%{"decision" => decision}), do: approval_choice(decision)

  defp approval_choice(%{"acceptWithExecpolicyAmendment" => _details} = decision) do
    %{label: "Approve with exec policy amendment", value: decision}
  end

  defp approval_choice(%{} = decision) do
    %{label: "Provider approval option", value: decision}
  end

  defp approval_choice(decision) do
    %{label: to_string(decision), value: decision}
  end

  defp permission_schema(%{} = permissions) when not is_struct(permissions), do: permissions
  defp permission_schema(_permissions), do: nil

  defp fetch(%{} = map, key) do
    Map.get(map, key, Map.get(map, Atom.to_string(key)))
  end

  defp fetch(_other, _key), do: nil

  defp compact(map), do: Map.reject(map, fn {_key, value} -> is_nil(value) end)
end
