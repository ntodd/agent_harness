defmodule AgentHarness.Providers.Codex.Protocol do
  @moduledoc false

  alias AgentHarness.Response
  alias Codex.AppServer.ApprovalDecision
  alias Codex.Events
  alias Codex.Protocol.RequestUserInput.Question

  @type encoded_response ::
          {:ok, map()}
          | {:interrupt, String.t(), String.t()}
          | {:error, term()}

  @spec encode_response(struct(), Response.t()) :: encoded_response()
  def encode_response(%Events.RequestUserInput{} = event, %Response{action: :answer} = response) do
    with {:ok, answers} <- question_answers(event.questions, response.value) do
      {:ok, %{"answers" => answers}}
    end
  end

  def encode_response(
        %Events.RequestUserInput{thread_id: thread_id, turn_id: turn_id},
        %Response{action: action}
      )
      when action in [:deny, :cancel] and is_binary(thread_id) and is_binary(turn_id) do
    {:interrupt, thread_id, turn_id}
  end

  def encode_response(%Events.CommandApprovalRequested{} = event, %Response{} = response),
    do: approval_response(event, response)

  def encode_response(%Events.FileApprovalRequested{}, %Response{} = response),
    do: approval_response(nil, response)

  def encode_response(%Events.PermissionsApprovalRequested{} = event, %Response{} = response),
    do: permissions_response(event, response)

  def encode_response(%Events.McpElicitationRequested{}, %Response{} = response),
    do: elicitation_response(response)

  def encode_response(%Events.DynamicToolCallRequested{}, %Response{action: :answer, value: value}),
      do: dynamic_tool_response(value)

  def encode_response(%Events.DynamicToolCallRequested{}, %Response{action: :approve}),
    do: dynamic_tool_response(%{})

  def encode_response(
        %Events.DynamicToolCallRequested{},
        %Response{action: action, reason: reason}
      )
      when action in [:deny, :cancel],
      do: dynamic_tool_failure(reason || Atom.to_string(action))

  def encode_response(
        %Events.CurrentTimeReadRequested{},
        %Response{action: :answer, value: value}
      )
      when is_integer(value),
      do: {:ok, %{"currentTimeAt" => value}}

  def encode_response(event, response),
    do: {:error, {:unsupported_response, event.__struct__, response.action}}

  @spec questions([term()]) :: [map()]
  def questions(questions) when is_list(questions) do
    Enum.map(questions, &question/1)
  end

  defp question(%Question{} = question) do
    %{
      id: question.id,
      header: question.header,
      prompt: question.question,
      choices: Enum.map(question.options || [], &option/1),
      allows_other: question.is_other,
      secret: question.is_secret
    }
  end

  defp question(%{} = question) do
    %{
      id: fetch(question, :id),
      header: fetch(question, :header),
      prompt: fetch(question, :question) || fetch(question, :prompt),
      choices: Enum.map(fetch(question, :options) || [], &option/1),
      allows_other: fetch(question, :is_other) || fetch(question, :isOther) || false,
      secret: fetch(question, :is_secret) || fetch(question, :isSecret) || false
    }
  end

  defp question(other) do
    %{
      id: nil,
      header: nil,
      prompt: inspect(other),
      choices: [],
      allows_other: false,
      secret: false
    }
  end

  defp option(%{label: label, description: description}) do
    %{label: label, value: label, description: description}
  end

  defp option(%{} = option) do
    label = fetch(option, :label)
    %{label: label, value: label, description: fetch(option, :description)}
  end

  defp option(other), do: %{label: to_string(other), value: to_string(other)}

  defp question_answers(questions, value) do
    ids = Enum.map(questions, &question_id/1)

    values =
      case {ids, value} do
        {[id], value} when not is_map(value) -> %{id => value}
        {_ids, value} when is_map(value) -> stringify_top_level_keys(value)
        _ -> %{}
      end

    Enum.reduce_while(ids, {:ok, %{}}, fn id, {:ok, answers} ->
      values
      |> Map.fetch(id)
      |> append_question_answer(id, answers)
    end)
  end

  defp append_question_answer(:error, id, _answers) do
    {:halt, {:error, {:missing_answer, id}}}
  end

  defp append_question_answer({:ok, value}, id, answers) do
    case normalize_answer(value) do
      {:ok, value} ->
        {:cont, {:ok, Map.put(answers, id, %{"answers" => value})}}

      {:error, _reason} = error ->
        {:halt, error}
    end
  end

  defp question_id(%Question{id: id}), do: id
  defp question_id(%{} = question), do: to_string(fetch(question, :id))

  defp normalize_answer(%{"answers" => answers}), do: normalize_answer(answers)
  defp normalize_answer(%{answers: answers}), do: normalize_answer(answers)

  defp normalize_answer(answers) when is_list(answers) do
    answers
    |> Enum.reduce_while({:ok, []}, fn answer, {:ok, acc} ->
      case answer_string(answer) do
        {:ok, answer} -> {:cont, {:ok, [answer | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, normalized} -> {:ok, Enum.reverse(normalized)}
      error -> error
    end
  end

  defp normalize_answer(answer) do
    case answer_string(answer) do
      {:ok, answer} -> {:ok, [answer]}
      error -> error
    end
  end

  defp answer_string(answer) when is_binary(answer), do: {:ok, answer}

  defp answer_string(answer) when is_atom(answer) or is_number(answer),
    do: {:ok, to_string(answer)}

  defp answer_string(answer), do: {:error, {:invalid_answer, answer}}

  defp approval_response(event, response) do
    with {:ok, payload} <- encode_approval_response(response),
         :ok <- ensure_available_decision(event, payload) do
      {:ok, payload}
    end
  end

  defp encode_approval_response(%Response{action: :approve, scope: :session}),
    do: {:ok, %{"decision" => "acceptForSession"}}

  defp encode_approval_response(%Response{action: :approve}),
    do: {:ok, %{"decision" => "accept"}}

  defp encode_approval_response(%Response{action: :deny}),
    do: {:ok, %{"decision" => "decline"}}

  defp encode_approval_response(%Response{action: :cancel}),
    do: {:ok, %{"decision" => "cancel"}}

  defp encode_approval_response(%Response{action: :answer, value: value}) when is_map(value) do
    value = stringify_keys(value)

    if Map.has_key?(value, "decision") do
      {:ok, value}
    else
      {:ok, %{"decision" => value}}
    end
  end

  defp encode_approval_response(%Response{action: :answer, value: value})
       when is_binary(value) or is_atom(value),
       do: {:ok, %{"decision" => encode_decision(value)}}

  defp encode_approval_response(%Response{action: action}),
    do: {:error, {:unsupported_approval_response, action}}

  defp permissions_response(event, %Response{action: :approve} = response) do
    permissions = permission_grant(response.value, event.permissions)
    scope = if response.scope == :session, do: :session, else: :turn

    decision = {:allow, permissions: permissions, scope: scope}
    {:ok, ApprovalDecision.from_permissions_hook(decision, event.permissions)}
  end

  defp permissions_response(event, %Response{action: action, reason: reason})
       when action in [:deny, :cancel] do
    {:ok,
     ApprovalDecision.from_permissions_hook(
       {:deny, reason || action},
       event.permissions
     )}
  end

  defp permissions_response(_event, %Response{action: action}),
    do: {:error, {:unsupported_permissions_response, action}}

  defp permission_grant(nil, requested), do: requested

  defp permission_grant(%{} = value, _requested) do
    fetch(value, :permissions) || value
  end

  defp permission_grant(_value, requested), do: requested

  defp ensure_available_decision(nil, _payload), do: :ok

  defp ensure_available_decision(
         %Events.CommandApprovalRequested{available_decisions: nil},
         _payload
       ),
       do: :ok

  defp ensure_available_decision(
         %Events.CommandApprovalRequested{available_decisions: decisions},
         %{"decision" => decision}
       )
       when is_list(decisions) do
    available = Enum.map(decisions, &available_decision_value/1)
    decision = stringify_keys(decision)

    if decision in available do
      :ok
    else
      {:error, {:decision_not_available, decision}}
    end
  end

  defp available_decision_value(%{} = decision) do
    decision = stringify_keys(decision)
    Map.get(decision, "decision", decision)
  end

  defp available_decision_value(decision), do: decision

  defp dynamic_tool_response(value) do
    value = stringify_keys(value)

    case value do
      %{"success" => success, "contentItems" => content_items}
      when is_boolean(success) and is_list(content_items) ->
        {:ok, %{"success" => success, "contentItems" => content_items}}

      text when is_binary(text) ->
        {:ok, dynamic_text_envelope(true, text)}

      value ->
        try do
          {:ok, dynamic_text_envelope(true, JSON.encode!(value))}
        rescue
          error -> {:error, {:invalid_dynamic_tool_output, Exception.message(error)}}
        end
    end
  end

  defp dynamic_tool_failure(reason) do
    {:ok, dynamic_text_envelope(false, to_string(reason))}
  end

  defp dynamic_text_envelope(success, text) do
    %{
      "success" => success,
      "contentItems" => [%{"type" => "inputText", "text" => text}]
    }
  end

  defp elicitation_response(%Response{action: :answer, value: content}),
    do: {:ok, %{"action" => "accept", "content" => content || %{}}}

  defp elicitation_response(%Response{action: :approve, value: content}),
    do: {:ok, %{"action" => "accept", "content" => content || %{}}}

  defp elicitation_response(%Response{action: :deny}),
    do: {:ok, %{"action" => "decline"}}

  defp elicitation_response(%Response{action: :cancel}),
    do: {:ok, %{"action" => "cancel"}}

  defp encode_decision(:approve), do: "accept"
  defp encode_decision(:approve_for_session), do: "acceptForSession"
  defp encode_decision(:deny), do: "decline"
  defp encode_decision(:cancel), do: "cancel"
  defp encode_decision(value) when is_atom(value), do: Atom.to_string(value)
  defp encode_decision(value), do: value

  defp stringify_top_level_keys(map) do
    Map.new(map, fn {key, value} -> {to_string(key), value} end)
  end

  defp stringify_keys(%{} = map) do
    Map.new(map, fn
      {key, value} when is_atom(key) -> {Atom.to_string(key), stringify_keys(value)}
      {key, value} -> {key, stringify_keys(value)}
    end)
  end

  defp stringify_keys(list) when is_list(list), do: Enum.map(list, &stringify_keys/1)
  defp stringify_keys(value), do: value

  defp fetch(map, key) do
    Map.get(map, key, Map.get(map, Atom.to_string(key)))
  end
end
