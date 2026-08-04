defmodule AgentHarness.Providers.Pi.Normalizer do
  @moduledoc false

  # Pi frames are decoded JSON maps with string keys. Normalization is pure and
  # frame-local; anything that needs run-scoped context (which stop reason ends
  # the run, which command a response answers) is decided by the Session.

  @dialog_methods ~w(confirm select input editor)

  @type normalized ::
          :ignore
          | {:event, atom(), map()}
          | {:request, String.t(), keyword()}
          | {:stopped, :completed | :cancelled | :failed, map()}
          | {:settle, map()}
          | {:response, String.t() | nil, map()}

  @spec normalize(map()) :: normalized()
  def normalize(%{"type" => "response"} = frame) do
    data =
      %{
        command: frame["command"],
        success: frame["success"] == true,
        data: frame["data"]
      }
      |> put_unless_nil(:error, frame["error"])

    {:response, frame["id"], data}
  end

  def normalize(%{"type" => "agent_start"}), do: :ignore

  def normalize(%{"type" => "agent_end"} = frame) do
    {:event, :agent_run_ended, %{will_retry: frame["willRetry"] == true}}
  end

  def normalize(%{"type" => "agent_settled"}), do: {:settle, %{}}

  def normalize(%{"type" => "turn_start"}), do: {:event, :provider_turn_started, %{}}

  def normalize(%{"type" => "turn_end"} = frame) do
    message = frame["message"] || %{}
    stop_reason = message["stopReason"]

    {:stopped, stop_status(stop_reason),
     %{
       stop_reason: stop_reason,
       raw_stop_reason: message["rawStopReason"],
       usage: message["usage"],
       tool_results: frame["toolResults"] || []
     }}
  end

  def normalize(%{"type" => "message_start", "message" => %{"role" => role}} = frame) do
    {:event, :message_started, %{role: role, message: frame["message"]}}
  end

  def normalize(%{"type" => "message_end", "message" => %{"role" => "assistant"} = message}) do
    {:event, :message_completed,
     %{
       role: "assistant",
       text: message_text(message),
       usage: message["usage"],
       stop_reason: message["stopReason"],
       message: message
     }}
  end

  def normalize(%{"type" => "message_end", "message" => %{"role" => "toolResult"} = message}) do
    {:event, :tool_result,
     %{
       tool_call_id: message["toolCallId"],
       tool_name: message["toolName"],
       content: message["content"],
       error?: message["isError"] == true
     }}
  end

  # User and system entries are echoed back by pi as it commits them to the
  # session. They are not assistant output, so they get their own type and
  # `:message_completed` stays one-per-assistant-message.
  def normalize(%{"type" => "message_end", "message" => message}) do
    {:event, :provider_message, %{role: message["role"], message: message}}
  end

  def normalize(%{"type" => "message_update", "assistantMessageEvent" => event}) do
    assistant_event(event)
  end

  def normalize(%{"type" => "tool_execution_start"} = frame) do
    {:event, :tool_started,
     %{
       tool_call_id: frame["toolCallId"],
       tool_name: frame["toolName"],
       args: frame["args"]
     }}
  end

  def normalize(%{"type" => "tool_execution_update"} = frame) do
    {:event, :tool_progress,
     %{
       tool_call_id: frame["toolCallId"],
       tool_name: frame["toolName"],
       partial_result: frame["partialResult"]
     }}
  end

  def normalize(%{"type" => "tool_execution_end"} = frame) do
    {:event, :tool_completed,
     %{
       tool_call_id: frame["toolCallId"],
       tool_name: frame["toolName"],
       result: frame["result"],
       error?: frame["isError"] == true
     }}
  end

  def normalize(%{"type" => "bash_execution_update"} = frame) do
    {:event, :command_output_delta, %{item_id: frame["id"], text: frame["delta"] || ""}}
  end

  def normalize(%{"type" => "queue_update"} = frame) do
    {:event, :queue_updated,
     %{steering: frame["steering"] || [], follow_up: frame["followUp"] || []}}
  end

  def normalize(%{"type" => "extension_ui_request", "method" => method} = frame)
      when method in @dialog_methods do
    {:request, frame["id"],
     [
       kind: :question,
       prompt: dialog_prompt(frame),
       choices: dialog_choices(frame),
       metadata:
         %{
           method: method,
           title: frame["title"],
           message: frame["message"],
           placeholder: frame["placeholder"],
           prefill: frame["prefill"],
           timeout: frame["timeout"]
         }
         |> Map.reject(fn {_key, value} -> is_nil(value) end)
     ]}
  end

  def normalize(%{"type" => "extension_ui_request", "method" => "notify"} = frame) do
    {:event, :provider_notice, %{level: frame["notifyType"] || "info", message: frame["message"]}}
  end

  def normalize(%{"type" => "extension_ui_request", "method" => method} = frame) do
    {:event, :provider_ui_update, %{method: method, data: frame}}
  end

  def normalize(%{"type" => "extension_error"} = frame) do
    {:event, :provider_error,
     %{
       message: frame["error"],
       extension_path: frame["extensionPath"],
       event: frame["event"]
     }}
  end

  def normalize(%{"type" => type} = frame) when is_binary(type) do
    {:event, :provider_event, %{type: type, data: frame}}
  end

  def normalize(_frame), do: :ignore

  @doc """
  True when a dialog method blocks the extension until the client answers.

  Fire-and-forget methods must not be answered; pi has no pending request to
  resolve and would treat the response as an unknown frame.
  """
  @spec dialog_method?(String.t()) :: boolean()
  def dialog_method?(method), do: method in @dialog_methods

  defp assistant_event(%{"type" => "text_start"} = event) do
    {:event, :message_started, %{content_index: event["contentIndex"], role: "assistant"}}
  end

  defp assistant_event(%{"type" => "text_delta"} = event) do
    {:event, :message_delta, %{text: event["delta"] || "", content_index: event["contentIndex"]}}
  end

  # Per-content-block completion. The message-level `:message_completed` comes
  # from `message_end`, so a caller counting completions sees one per message.
  defp assistant_event(%{"type" => "text_end"} = event) do
    {:event, :message_content_completed,
     %{role: "assistant", text: event["content"] || "", content_index: event["contentIndex"]}}
  end

  defp assistant_event(%{"type" => "thinking_start"} = event) do
    {:event, :reasoning_started, %{content_index: event["contentIndex"]}}
  end

  defp assistant_event(%{"type" => "thinking_delta"} = event) do
    {:event, :reasoning_delta,
     %{text: event["delta"] || "", content_index: event["contentIndex"]}}
  end

  defp assistant_event(%{"type" => "thinking_end"} = event) do
    {:event, :reasoning_completed,
     %{text: event["content"] || "", content_index: event["contentIndex"]}}
  end

  defp assistant_event(%{"type" => "toolcall_start"} = event) do
    {:event, :tool_call_started, %{content_index: event["contentIndex"]}}
  end

  defp assistant_event(%{"type" => "toolcall_delta"} = event) do
    {:event, :tool_call_delta,
     %{text: event["delta"] || "", content_index: event["contentIndex"]}}
  end

  defp assistant_event(%{"type" => "toolcall_end"} = event) do
    {:event, :tool_call_completed,
     %{content_index: event["contentIndex"], tool_call: event["toolCall"]}}
  end

  defp assistant_event(event) do
    {:event, :provider_event, %{type: event["type"], data: event}}
  end

  # Pi reports "aborted" for both a client abort and an extension-cancelled run.
  # Everything unrecognized is treated as a completion so an unknown stop reason
  # cannot silently fail an otherwise successful turn.
  defp stop_status("aborted"), do: :cancelled
  defp stop_status("error"), do: :failed
  defp stop_status(_reason), do: :completed

  defp dialog_prompt(%{"title" => title, "message" => message})
       when is_binary(title) and is_binary(message) and message != "" do
    title <> "\n\n" <> message
  end

  defp dialog_prompt(%{"title" => title}) when is_binary(title), do: title
  defp dialog_prompt(%{"message" => message}) when is_binary(message), do: message
  defp dialog_prompt(_frame), do: "Pi needs input."

  defp dialog_choices(%{"method" => "confirm"}) do
    [%{label: "Yes", value: true}, %{label: "No", value: false}]
  end

  defp dialog_choices(%{"method" => "select", "options" => options}) when is_list(options) do
    Enum.map(options, &%{label: to_string(&1), value: &1})
  end

  defp dialog_choices(_frame), do: []

  defp message_text(%{"content" => content}) when is_list(content) do
    content
    |> Enum.filter(&match?(%{"type" => "text"}, &1))
    |> Enum.map_join(&(&1["text"] || ""))
  end

  defp message_text(_message), do: ""

  defp put_unless_nil(map, _key, nil), do: map
  defp put_unless_nil(map, key, value), do: Map.put(map, key, value)
end
