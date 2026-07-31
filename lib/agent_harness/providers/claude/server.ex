defmodule AgentHarness.Providers.Claude.Server do
  @moduledoc false

  use GenServer

  alias AgentHarness.Provider.{OpenGuardian, Sink}
  alias AgentHarness.Providers.Claude.Options
  alias AgentHarness.{Response, Turn}

  alias ClaudeCode.Content.{TextBlock, ThinkingBlock, ToolResultBlock, ToolUseBlock}

  alias ClaudeCode.Message.{
    AssistantMessage,
    PartialAssistantMessage,
    RateLimitEvent,
    ResultMessage,
    ToolProgressMessage,
    UserMessage
  }

  defmodule State do
    @moduledoc false

    @enforce_keys [
      :config,
      :sink,
      :client,
      :client_session,
      :sink_monitor,
      :client_monitor,
      :question_timeout
    ]
    defstruct [
      :config,
      :sink,
      :client,
      :client_session,
      :sink_monitor,
      :client_monitor,
      :provider_session_id,
      :current_turn_id,
      :runner,
      :runner_monitor,
      :question_timeout,
      pending_requests: %{},
      cancelled_turns: MapSet.new(),
      cleanup_paths: []
    ]
  end

  @type provider_request_ref :: reference()
  @default_call_timeout 5_000
  @default_client_interrupt_timeout 3_000
  @default_client_stop_timeout 1_000

  def child_spec(opts) do
    config = Keyword.fetch!(opts, :config)

    %{
      id: {__MODULE__, config.session_id},
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary,
      type: :worker
    }
  end

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  def provider_session_id(server, startup_timeout) do
    case call(server, :provider_session_id, startup_timeout) do
      {:error, reason} -> {:error, reason}
      provider_session_id -> {:ok, provider_session_id}
    end
  end

  def start_turn(server, turn, input, opts) do
    call(server, {:start_turn, turn, input, opts})
  end

  def respond(server, provider_request_ref, response) do
    call(server, {:respond, provider_request_ref, response})
  end

  def cancel(server, provider_turn_ref) do
    call(server, {:cancel, provider_turn_ref})
  end

  def close(server) do
    case call(server, :close) do
      {:error, {:provider_call_failed, _reason}} -> :ok
      result -> result
    end
  end

  @doc false
  def await_tool_decision(server, input, tool_use_id) do
    GenServer.call(server, {:tool_request, input, tool_use_id}, :infinity)
  catch
    :exit, _reason ->
      {:deny, message: "AgentHarness closed before the request was answered", interrupt: true}
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    config = Keyword.fetch!(opts, :config)
    sink = Keyword.fetch!(opts, :sink)
    guardian = OpenGuardian.start(self(), sink.pid)

    {:ok, {:opening, config, sink, guardian}, {:continue, :open}}
  end

  @impl true
  def handle_continue(:open, {:opening, config, sink, guardian} = opening) do
    case Options.prepare(config) do
      {:ok, prepared} ->
        case start_client(config, sink, prepared) do
          {:ok, state} ->
            OpenGuardian.disarm(guardian)
            {:noreply, state}

          {:stop, reason} ->
            {:stop, reason, opening}
        end

      {:error, reason} ->
        {:stop, {:invalid_claude_options, reason}, opening}
    end
  rescue
    error ->
      {:stop, {:claude_start_failed, error}, opening}
  end

  @impl true
  def handle_call(:provider_session_id, _from, state) do
    {:reply, state.provider_session_id, state}
  end

  def handle_call(
        {:start_turn, %Turn{} = turn, input, opts},
        _from,
        %State{current_turn_id: nil} = state
      )
      when is_binary(input) do
    stream_opts = Keyword.take(opts, [:timeout])
    filter = Keyword.get(opts, :filter, :all)
    server = self()

    case Task.Supervisor.start_child(AgentHarness.RunnerSupervisor, fn ->
           run_stream(
             server,
             state.client,
             state.client_session,
             state.sink,
             turn.id,
             input,
             stream_opts,
             filter
           )
         end) do
      {:ok, runner} ->
        {:reply, {:ok, turn.id},
         %{
           state
           | current_turn_id: turn.id,
             runner: runner,
             runner_monitor: Process.monitor(runner)
         }}

      {:error, reason} ->
        {:reply, {:error, {:runner_start_failed, reason}}, state}
    end
  end

  def handle_call({:start_turn, %Turn{}, input, _opts}, _from, state)
      when not is_binary(input) do
    {:reply, {:error, {:unsupported_input, input}}, state}
  end

  def handle_call({:start_turn, %Turn{}, _input, _opts}, _from, state) do
    {:reply, {:error, {:turn_in_progress, state.current_turn_id}}, state}
  end

  def handle_call(
        {:tool_request, _input, _tool_use_id},
        from,
        %State{current_turn_id: nil} = state
      ) do
    GenServer.reply(
      from,
      {:deny, message: "Claude requested input without an active turn", interrupt: true}
    )

    {:noreply, state}
  end

  def handle_call({:tool_request, input, tool_use_id}, from, state) do
    provider_ref = make_ref()
    pending = pending_request(from, input, tool_use_id, state.current_turn_id)
    timer = schedule_request_timeout(provider_ref, state.question_timeout)
    pending = Map.put(pending, :timer, timer)
    attrs = request_attrs(pending)

    Sink.request(
      state.sink,
      state.current_turn_id,
      provider_ref,
      attrs,
      input
    )

    {:noreply,
     %{state | pending_requests: Map.put(state.pending_requests, provider_ref, pending)}}
  end

  def handle_call({:respond, provider_ref, %Response{} = response}, _from, state) do
    case Map.fetch(state.pending_requests, provider_ref) do
      :error ->
        {:reply, {:error, :request_not_found}, state}

      {:ok, pending} ->
        case decision_for(pending, response) do
          {:ok, decision} ->
            cancel_timer(pending.timer)
            GenServer.reply(pending.from, decision)

            {:reply, :ok,
             %{state | pending_requests: Map.delete(state.pending_requests, provider_ref)}}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end
    end
  end

  def handle_call({:cancel, turn_id}, from, %State{current_turn_id: turn_id} = state) do
    state = reject_pending_for_turn(state, turn_id, "Turn cancelled")

    case safe_interrupt(state.client, state.client_session) do
      :ok ->
        state = %{state | cancelled_turns: MapSet.put(state.cancelled_turns, turn_id)}
        {:reply, :ok, state}

      {:error, reason} ->
        failure = {:claude_interrupt_failed, reason}
        Sink.transport_down(state.sink, failure)
        GenServer.reply(from, {:error, reason})
        {:stop, :normal, state}
    end
  end

  def handle_call({:cancel, _turn_id}, _from, state) do
    {:reply, {:error, :turn_not_active}, state}
  end

  def handle_call({:terminal_status, turn_id, is_error, runner}, _from, state) do
    status =
      cond do
        MapSet.member?(state.cancelled_turns, turn_id) -> :interrupted
        is_error -> :failed
        true -> :completed
      end

    state = reject_pending_for_turn(state, turn_id, "Turn finished before input was resolved")
    {:reply, status, clear_runner(state, turn_id, runner)}
  end

  def handle_call({:runner_failed, turn_id, runner, kind, failure}, from, state) do
    if kind == :eof and MapSet.member?(state.cancelled_turns, turn_id) do
      state = reject_pending_for_turn(state, turn_id, "Turn stream ended")
      {:reply, :interrupted, clear_runner(state, turn_id, runner)}
    else
      state = reject_pending_for_turn(state, turn_id, "Turn stream failed")
      state = detach_runner(state, turn_id, runner)
      Sink.finish(state.sink, turn_id, :failed, failure)
      Sink.transport_down(state.sink, {:claude_stream_failed, failure.reason})
      GenServer.reply(from, :handled)
      {:stop, :normal, state}
    end
  end

  def handle_call(:close, _from, state) do
    state = reject_all_pending(state, "Session closed")
    {:stop, :normal, :ok, state}
  end

  @impl true
  def handle_cast({:provider_session_id, session_id}, state) do
    {:noreply, %{state | provider_session_id: session_id}}
  end

  @impl true
  def handle_info({:request_timeout, provider_ref}, state) do
    case Map.pop(state.pending_requests, provider_ref) do
      {nil, _pending} ->
        {:noreply, state}

      {pending, remaining} ->
        GenServer.reply(
          pending.from,
          {:deny, message: "AgentHarness timed out waiting for input", interrupt: true}
        )

        Sink.expire_request(
          state.sink,
          pending.turn_id,
          provider_ref,
          :question_timeout,
          pending.input
        )

        {:noreply, %{state | pending_requests: remaining}}
    end
  end

  def handle_info(
        {:DOWN, monitor, :process, runner, reason},
        %State{runner_monitor: monitor, runner: runner} = state
      ) do
    turn_id = state.current_turn_id
    state = reject_pending_for_turn(state, turn_id, "Turn runner stopped")
    failure = %{reason: {:runner_down, reason}}

    if turn_id do
      Sink.finish(state.sink, turn_id, :failed, failure)
      Sink.transport_down(state.sink, {:claude_stream_failed, failure.reason})
    end

    {:stop, :normal, detach_runner(state, turn_id, runner)}
  end

  def handle_info(
        {:DOWN, monitor, :process, pid, _reason},
        %State{sink_monitor: monitor, sink: %Sink{pid: pid}} = state
      ) do
    {:stop, :normal, state}
  end

  def handle_info(
        {:DOWN, monitor, :process, pid, reason},
        %State{client_monitor: monitor, client_session: pid} = state
      ) do
    Sink.transport_down(state.sink, {:claude_session_down, reason})
    {:stop, {:claude_session_down, reason}, state}
  end

  def handle_info({:EXIT, pid, reason}, %State{client_session: pid} = state) do
    if reason == :normal do
      {:noreply, state}
    else
      Sink.transport_down(state.sink, {:claude_session_exit, reason})
      {:stop, {:claude_session_exit, reason}, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %State{} = state) do
    reject_all_pending(state, "Session closed")

    if is_pid(state.runner) and Process.alive?(state.runner) do
      Task.Supervisor.terminate_child(AgentHarness.RunnerSupervisor, state.runner)
    end

    safe_stop(state.client, state.client_session)
    cleanup(state.cleanup_paths)
    :ok
  end

  def terminate(_reason, {:opening, _config, _sink, _guardian}), do: :ok

  defp start_client(config, sink, prepared) do
    server = self()

    callback = fn input, tool_use_id ->
      await_tool_decision(server, input, tool_use_id)
    end

    client_options = Keyword.put(prepared.client_options, :can_use_tool, callback)

    with :ok <-
           safe_verify_subscription_auth(
             prepared.client,
             client_options,
             prepared.auth,
             prepared.auth_check_timeout
           ),
         {:ok, client_session} <- safe_start_client(prepared.client, client_options) do
      finish_client_start(config, sink, prepared, client_session)
    else
      {:error, reason} ->
        cleanup(prepared.cleanup_paths)
        {:stop, {:claude_start_failed, reason}}
    end
  end

  defp finish_client_start(config, sink, prepared, client_session) do
    case safe_await_ready(prepared.client, client_session, prepared.readiness_timeout) do
      :ok ->
        provider_session_id = safe_session_id(prepared.client, client_session)

        {:ok,
         %State{
           config: config,
           sink: sink,
           client: prepared.client,
           client_session: client_session,
           provider_session_id: provider_session_id,
           sink_monitor: Process.monitor(sink.pid),
           client_monitor: Process.monitor(client_session),
           question_timeout: prepared.question_timeout,
           cleanup_paths: prepared.cleanup_paths
         }}

      {:error, reason} ->
        safe_stop(prepared.client, client_session)
        cleanup(prepared.cleanup_paths)
        {:stop, {:claude_not_ready, reason}}
    end
  rescue
    error ->
      safe_stop(prepared.client, client_session)
      cleanup(prepared.cleanup_paths)
      {:stop, {:claude_start_failed, error}}
  end

  defp safe_start_client(client, client_options) do
    case client.start_link(client_options) do
      {:ok, client_session} when is_pid(client_session) ->
        {:ok, client_session}

      {:ok, client_session} ->
        {:error, {:invalid_client_session, client_session}}

      {:error, reason} ->
        {:error, reason}

      other ->
        {:error, {:unexpected_start_result, other}}
    end
  rescue
    error -> {:error, {:exception, error}}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp safe_verify_subscription_auth(_client, _client_options, :inherit, _timeout), do: :ok

  defp safe_verify_subscription_auth(client, client_options, :subscription, timeout) do
    case client.verify_subscription_auth(client_options, timeout) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
      other -> {:error, {:unexpected_auth_check_result, other}}
    end
  rescue
    error -> {:error, {:exception, error}}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp safe_await_ready(client, client_session, timeout) do
    case client.await_ready(client_session, timeout) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
      other -> {:error, {:unexpected_readiness_result, other}}
    end
  rescue
    error -> {:error, {:exception, error}}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp run_stream(
         server,
         client,
         client_session,
         sink,
         turn_id,
         input,
         stream_opts,
         filter
       ) do
    result =
      client_session
      |> client.stream(input, stream_opts)
      |> Enum.reduce_while(%{terminal: false, session_id: nil}, fn message, acc ->
        acc = update_provider_session(server, sink, message, acc)

        case message do
          %ResultMessage{} = result ->
            status = terminal_status(server, turn_id, result.is_error, self())
            Sink.finish(sink, turn_id, status, result_data(result), result)
            {:halt, %{acc | terminal: true}}

          message ->
            emit_if_matches(sink, turn_id, message, filter)
            {:cont, acc}
        end
      end)

    unless result.terminal do
      failure = %{reason: :missing_result}

      case runner_failed(server, turn_id, self(), :eof, failure) do
        :interrupted ->
          Sink.finish(sink, turn_id, :interrupted, %{reason: :cancelled})

        :handled ->
          :ok
      end
    end
  rescue
    error ->
      failure = %{
        reason: {:exception, error.__struct__, Exception.message(error)}
      }

      runner_failed(server, turn_id, self(), :error, failure)
  catch
    kind, reason ->
      runner_failed(server, turn_id, self(), :error, %{reason: {kind, reason}})
  end

  defp terminal_status(server, turn_id, is_error, runner) do
    GenServer.call(server, {:terminal_status, turn_id, is_error, runner})
  catch
    :exit, _reason -> if(is_error, do: :failed, else: :completed)
  end

  defp runner_failed(server, turn_id, runner, kind, failure) do
    GenServer.call(server, {:runner_failed, turn_id, runner, kind, failure})
  catch
    :exit, _reason -> :handled
  end

  defp update_provider_session(server, sink, message, acc) do
    case Map.get(message, :session_id) do
      session_id when is_binary(session_id) and session_id != acc.session_id ->
        Sink.session_updated(sink, %{provider_session_id: session_id})
        GenServer.cast(server, {:provider_session_id, session_id})
        %{acc | session_id: session_id}

      _other ->
        acc
    end
  end

  defp message_matches_filter?(_message, :all), do: true
  defp message_matches_filter?(_message, :result), do: false

  defp message_matches_filter?(%AssistantMessage{message: %{content: content}}, :tool_use) do
    Enum.any?(content || [], &match?(%ToolUseBlock{}, &1))
  end

  defp message_matches_filter?(
         %PartialAssistantMessage{
           event: %{type: :content_block_delta, delta: %{type: :text_delta}}
         },
         :text_delta
       ),
       do: true

  defp message_matches_filter?(%{type: type}, type), do: true
  defp message_matches_filter?(_message, _filter), do: false

  defp emit_if_matches(sink, turn_id, message, filter) do
    if message_matches_filter?(message, filter) do
      emit_message(sink, turn_id, message)
    end
  end

  defp emit_message(
         sink,
         turn_id,
         %PartialAssistantMessage{
           event: %{type: :content_block_delta, delta: %{type: :text_delta, text: text}}
         } = message
       ) do
    Sink.emit(sink, turn_id, :message_delta, %{text: text}, message)
  end

  defp emit_message(
         sink,
         turn_id,
         %PartialAssistantMessage{
           event: %{
             type: :content_block_delta,
             delta: %{type: :thinking_delta, thinking: thinking}
           }
         } = message
       ) do
    Sink.emit(sink, turn_id, :thinking_delta, %{thinking: thinking}, message)
  end

  defp emit_message(
         sink,
         turn_id,
         %PartialAssistantMessage{
           event: %{
             type: :content_block_delta,
             delta: %{type: :input_json_delta, partial_json: json}
           }
         } = message
       ) do
    Sink.emit(sink, turn_id, :tool_input_delta, %{partial_json: json}, message)
  end

  defp emit_message(sink, turn_id, %PartialAssistantMessage{} = message) do
    Sink.emit(sink, turn_id, :stream_event, %{event: message.event}, message)
  end

  defp emit_message(sink, turn_id, %AssistantMessage{} = message) do
    content = message.message.content || []

    Sink.emit(
      sink,
      turn_id,
      :assistant_message,
      %{
        text: text_content(content),
        content: content,
        stop_reason: message.message.stop_reason,
        error: message.error
      },
      message
    )

    Enum.each(content, fn
      %ToolUseBlock{} = tool ->
        Sink.emit(
          sink,
          turn_id,
          :tool_started,
          %{id: tool.id, name: tool.name, input: tool.input},
          tool
        )

      _content ->
        :ok
    end)
  end

  defp emit_message(sink, turn_id, %UserMessage{} = message) do
    content = List.wrap(message.message.content)

    Enum.each(content, fn
      %ToolResultBlock{} = result ->
        Sink.emit(
          sink,
          turn_id,
          :tool_completed,
          %{
            id: result.tool_use_id,
            content: result.content,
            is_error: result.is_error
          },
          result
        )

      _content ->
        :ok
    end)

    Sink.emit(sink, turn_id, :user_message, %{content: content}, message)
  end

  defp emit_message(sink, turn_id, %ToolProgressMessage{} = message) do
    Sink.emit(
      sink,
      turn_id,
      :tool_progress,
      %{
        id: message.tool_use_id,
        name: message.tool_name,
        elapsed_seconds: message.elapsed_time_seconds,
        task_id: message.task_id
      },
      message
    )
  end

  defp emit_message(sink, turn_id, %RateLimitEvent{} = message) do
    Sink.emit(sink, turn_id, :rate_limit, message.rate_limit_info, message)
  end

  defp emit_message(sink, turn_id, message) do
    Sink.emit(sink, turn_id, :provider_event, %{message: message}, message)
  end

  defp text_content(content) do
    content
    |> Enum.flat_map(fn
      %TextBlock{text: text} -> [text]
      %ThinkingBlock{} -> []
      _block -> []
    end)
    |> Enum.join()
  end

  defp result_data(%ResultMessage{} = result) do
    %{
      text: result.result,
      is_error: result.is_error,
      subtype: result.subtype,
      stop_reason: result.stop_reason,
      session_id: result.session_id,
      usage: result.usage,
      model_usage: result.model_usage,
      total_cost_usd: result.total_cost_usd,
      num_turns: result.num_turns,
      permission_denials: result.permission_denials,
      structured_output: result.structured_output,
      errors: result.errors
    }
  end

  defp pending_request(from, input, tool_use_id, turn_id) do
    %{
      from: from,
      input: input,
      tool_input: tool_input(input),
      tool_name: tool_name(input),
      tool_use_id: tool_use_id,
      turn_id: turn_id
    }
  end

  defp tool_input(input) do
    Map.get(input, :tool_input) ||
      Map.get(input, :input) ||
      Map.get(input, "tool_input") ||
      Map.get(input, "input") ||
      %{}
  end

  defp tool_name(input) do
    Map.get(input, :tool_name) || Map.get(input, "tool_name") || "unknown"
  end

  defp request_attrs(pending) do
    questions = normalize_questions(pending.tool_input["questions"] || [])
    kind = request_kind(pending.tool_name)

    prompt =
      case {kind, questions} do
        {:question, [%{prompt: prompt}]} -> prompt
        {:question, [_ | _]} -> "Claude needs additional input"
        _other -> "Allow Claude to use #{pending.tool_name}?"
      end

    [
      kind: kind,
      prompt: prompt,
      questions: questions,
      choices: if(length(questions) == 1, do: hd(questions).choices, else: []),
      metadata: %{
        tool_name: pending.tool_name,
        tool_input: pending.tool_input,
        tool_use_id: pending.tool_use_id
      }
    ]
  end

  defp normalize_questions(questions) do
    questions
    |> Enum.with_index(1)
    |> Enum.map(fn {question, index} ->
      prompt = question["question"] || "Question #{index}"
      id = question["header"] || "question-#{index}"

      %{
        id: id,
        header: question["header"],
        prompt: prompt,
        choices:
          Enum.map(question["options"] || [], fn option ->
            %{
              label: option["label"],
              value: option["label"],
              description: option["description"]
            }
          end),
        multiple: question["multiSelect"] || false
      }
    end)
  end

  defp request_kind("AskUserQuestion"), do: :question
  defp request_kind("Bash"), do: :command_approval

  defp request_kind(tool) when tool in ["Write", "Edit", "NotebookEdit"],
    do: :file_change_approval

  defp request_kind(_tool), do: :permission

  defp decision_for(pending, %Response{action: :answer, value: value}) do
    kind = request_kind(pending.tool_name)

    if kind == :question do
      with [_ | _] = questions <- Map.get(pending.tool_input, "questions", []),
           {:ok, answers} <- answers_for(questions, value) do
        {:ok,
         {:allow,
          updated_input:
            pending.tool_input
            |> Map.put("answers", answers)}}
      else
        [] -> {:error, :question_has_no_prompts}
        {:error, reason} -> {:error, reason}
        _invalid_questions -> {:error, :invalid_question_prompts}
      end
    else
      {:error, {:invalid_response_action, kind, :answer}}
    end
  end

  defp decision_for(pending, %Response{action: :approve, scope: scope})
       when scope in [nil, :once] do
    kind = request_kind(pending.tool_name)

    if kind == :question do
      {:error, {:invalid_response_action, :question, :approve}}
    else
      {:ok, {:allow, updated_input: pending.tool_input}}
    end
  end

  defp decision_for(_pending, %Response{action: :approve, scope: :session}) do
    {:error, {:unsupported_approval_scope, :session}}
  end

  defp decision_for(pending, %Response{action: :approve, scope: scope}) do
    {:error, {:invalid_approval_scope, request_kind(pending.tool_name), scope}}
  end

  defp decision_for(_pending, %Response{action: :deny, reason: reason}) do
    {:ok, {:deny, message: reason || "Denied by user"}}
  end

  defp decision_for(_pending, %Response{action: :cancel, reason: reason}) do
    {:ok, {:deny, message: reason || "Cancelled by user", interrupt: true}}
  end

  defp decision_for(pending, %Response{action: action}) do
    {:error, {:invalid_response_action, request_kind(pending.tool_name), action}}
  end

  defp answers_for(questions, value) do
    source =
      if is_map(value) do
        Map.get(value, "answers") || Map.get(value, :answers) || value
      else
        value
      end

    questions
    |> Enum.with_index(1)
    |> Enum.reduce_while({:ok, %{}}, fn {question, index}, {:ok, answers} ->
      prompt = question["question"] || "Question #{index}"
      header = question["header"]

      case answer_value(source, [prompt, header, "question-#{index}"], length(questions)) do
        {:ok, answer} ->
          put_answer(answers, prompt, answer)

        :error ->
          {:halt, {:error, {:missing_answer, prompt}}}
      end
    end)
  end

  defp put_answer(answers, prompt, answer) do
    case stringify_answer(answer) do
      {:ok, answer} -> {:cont, {:ok, Map.put(answers, prompt, answer)}}
      {:error, reason} -> {:halt, {:error, reason}}
    end
  end

  defp answer_value(source, _keys, 1) when not is_map(source), do: {:ok, source}

  defp answer_value(source, keys, question_count) when is_map(source) do
    found = Enum.find_value(keys, &find_answer(source, &1))

    case found do
      {:found, value} ->
        {:ok, value}

      nil when question_count == 1 and map_size(source) == 1 ->
        {_key, value} = Enum.at(source, 0)
        {:ok, value}

      nil ->
        :error
    end
  end

  defp answer_value(_source, _keys, _question_count), do: :error

  defp find_answer(_source, nil), do: nil

  defp find_answer(source, key) do
    Enum.find_value(source, &matching_answer(&1, key))
  end

  defp matching_answer({source_key, value}, key) do
    if comparable_answer_key?(source_key) and to_string(source_key) == key,
      do: {:found, value}
  end

  defp comparable_answer_key?(key), do: is_binary(key) or is_atom(key) or is_number(key)

  defp stringify_answer(values) when is_list(values) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, acc} ->
      case stringify_answer(value) do
        {:ok, value} -> {:cont, {:ok, [value | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, values |> Enum.reverse() |> Enum.join(", ")}
      {:error, reason} -> {:error, reason}
    end
  end

  defp stringify_answer(nil), do: {:error, {:invalid_answer, nil}}
  defp stringify_answer(value) when is_binary(value), do: {:ok, value}

  defp stringify_answer(value) when is_atom(value) or is_number(value),
    do: {:ok, to_string(value)}

  defp stringify_answer(value), do: {:error, {:invalid_answer, value}}

  defp reject_pending_for_turn(state, turn_id, reason) do
    Enum.reduce(state.pending_requests, state, fn
      {provider_ref, %{turn_id: ^turn_id} = pending}, acc ->
        cancel_timer(pending.timer)
        GenServer.reply(pending.from, {:deny, message: reason, interrupt: true})
        %{acc | pending_requests: Map.delete(acc.pending_requests, provider_ref)}

      {_provider_ref, _pending}, acc ->
        acc
    end)
  end

  defp reject_all_pending(state, reason) do
    Enum.each(state.pending_requests, fn {_provider_ref, pending} ->
      cancel_timer(pending.timer)
      GenServer.reply(pending.from, {:deny, message: reason, interrupt: true})
    end)

    %{state | pending_requests: %{}}
  end

  defp schedule_request_timeout(_provider_ref, :infinity), do: nil

  defp schedule_request_timeout(provider_ref, timeout) do
    Process.send_after(self(), {:request_timeout, provider_ref}, timeout)
  end

  defp cancel_timer(nil), do: :ok
  defp cancel_timer(timer), do: Process.cancel_timer(timer, async: true, info: false)

  defp safe_session_id(client, client_session) do
    client.session_id(client_session)
  catch
    _kind, _reason -> nil
  end

  defp safe_stop(client, client_session) do
    case bounded_invoke(fn -> client.stop(client_session) end, client_stop_timeout()) do
      {:error, :timeout} ->
        kill_client(client_session)
        :ok

      _result ->
        :ok
    end
  end

  defp kill_client(client_session) when is_pid(client_session) do
    if Process.alive?(client_session), do: Process.exit(client_session, :kill)
  end

  defp kill_client(_client_session), do: :ok

  defp safe_interrupt(client, client_session) do
    case bounded_invoke(
           fn -> client.interrupt(client_session) end,
           client_interrupt_timeout()
         ) do
      {:ok, :ok} -> :ok
      {:ok, {:error, reason}} -> {:error, reason}
      {:ok, other} -> {:error, {:unexpected_interrupt_result, other}}
      {:error, :timeout} -> {:error, :interrupt_timeout}
      {:error, reason} -> {:error, reason}
    end
  end

  defp bounded_invoke(function, timeout) do
    caller = self()
    reply_ref = make_ref()

    {worker, monitor} =
      spawn_monitor(fn ->
        result =
          try do
            {:ok, function.()}
          rescue
            error -> {:error, {:exception, error}}
          catch
            kind, reason -> {:error, {kind, reason}}
          end

        send(caller, {reply_ref, result})
      end)

    receive do
      {^reply_ref, result} ->
        Process.demonitor(monitor, [:flush])
        result

      {:DOWN, ^monitor, :process, ^worker, reason} ->
        {:error, {:worker_down, reason}}
    after
      timeout ->
        Process.exit(worker, :kill)
        {:error, :timeout}
    end
  end

  defp cleanup(paths) do
    Enum.each(paths, &File.rm_rf/1)
  end

  defp call(server, message) do
    call(server, message, call_timeout())
  end

  defp call(server, message, timeout) do
    GenServer.call(server, message, timeout)
  catch
    :exit, reason -> {:error, {:provider_call_failed, reason}}
  end

  defp call_timeout do
    Application.get_env(:agent_harness, :claude_call_timeout, @default_call_timeout)
  end

  defp client_interrupt_timeout do
    Application.get_env(
      :agent_harness,
      :claude_interrupt_timeout,
      @default_client_interrupt_timeout
    )
  end

  defp client_stop_timeout do
    Application.get_env(:agent_harness, :claude_stop_timeout, @default_client_stop_timeout)
  end

  defp clear_runner(
         %State{current_turn_id: turn_id, runner: runner} = state,
         turn_id,
         runner
       ) do
    demonitor(state.runner_monitor)

    %{
      state
      | current_turn_id: nil,
        runner: nil,
        runner_monitor: nil,
        cancelled_turns: MapSet.delete(state.cancelled_turns, turn_id)
    }
  end

  defp clear_runner(state, _turn_id, _runner), do: state

  defp detach_runner(
         %State{current_turn_id: turn_id, runner: runner} = state,
         turn_id,
         runner
       ) do
    demonitor(state.runner_monitor)
    %{state | runner: nil, runner_monitor: nil}
  end

  defp detach_runner(state, _turn_id, _runner), do: state

  defp demonitor(nil), do: :ok
  defp demonitor(monitor), do: Process.demonitor(monitor, [:flush])
end
