defmodule AgentHarness do
  @moduledoc """
  Supervised logical sessions for locally installed coding-agent harnesses.

  A session has a stable `AgentHarness.SessionRef` and one active turn at a
  time. Provider adapters own the external CLI lifecycle and publish normalized
  events through the session.
  """

  alias AgentHarness.{
    Event,
    Request,
    Response,
    SessionConfig,
    SessionRef,
    SessionServer,
    Subscription,
    Turn
  }

  @session_supervisor AgentHarness.SessionSupervisor
  @terminal_events [:turn_completed, :turn_failed, :turn_cancelled, :turn_interrupted]
  @call_timeout 30_000

  @doc """
  Starts a supervised logical session for `provider`.

  Authentication remains the responsibility of the locally installed CLI.
  """
  @spec start_session(atom(), keyword()) :: {:ok, SessionRef.t()} | {:error, term()}
  def start_session(provider, opts \\ [])

  def start_session(provider, opts) when is_atom(provider) and is_list(opts) do
    with :ok <- validate_keyword_options(opts, :session),
         id = Keyword.get(opts, :id, AgentHarness.ID.generate()),
         :ok <- validate_id(id, :session),
         {:ok, provider_module} <- resolve_provider_module(provider, opts),
         session = SessionRef.new(provider, id: id),
         {:ok, config} <-
           build_session_config(session, Keyword.drop(opts, [:id, :provider_module])) do
      child_opts = [session: session, config: config, provider: provider_module]

      case DynamicSupervisor.start_child(@session_supervisor, {SessionServer, child_opts}) do
        {:ok, _pid} -> {:ok, session}
        {:error, {:already_started, _pid}} -> {:error, :session_already_exists}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  def start_session(provider, _opts) when not is_atom(provider),
    do: {:error, {:invalid_provider, provider}}

  def start_session(_provider, opts), do: {:error, {:invalid_session_options, opts}}

  @doc """
  Starts one turn. A session rejects a second concurrent turn.
  """
  @spec start_turn(SessionRef.t(), term(), keyword()) :: {:ok, Turn.t()} | {:error, term()}
  def start_turn(session, input, opts \\ [])

  def start_turn(%SessionRef{id: session_id}, input, opts) when is_list(opts) do
    with :ok <- validate_id(session_id, :session),
         :ok <- validate_keyword_options(opts, :turn),
         :ok <- validate_optional_id(Keyword.get(opts, :id), :turn) do
      call(session_id, {:start_turn, input, opts})
    end
  end

  def start_turn(%SessionRef{}, _input, opts), do: {:error, {:invalid_turn_options, opts}}

  @doc """
  Subscribes the calling process, or `:pid`, to session or turn events.

  `:from` may be `:latest`, `:start`, or `{:after, sequence}`.
  """
  @spec subscribe(SessionRef.t() | Turn.t(), keyword()) ::
          {:ok, Subscription.t()} | {:error, term()}
  def subscribe(target, opts \\ [])

  def subscribe(%SessionRef{id: session_id}, opts) when is_list(opts) do
    with :ok <- validate_id(session_id, :session),
         {:ok, subscriber, from} <- validate_subscription_options(opts) do
      call(session_id, {:subscribe, subscriber, nil, from})
    end
  end

  def subscribe(%Turn{session_id: session_id, id: turn_id}, opts) when is_list(opts) do
    with :ok <- validate_id(session_id, :session),
         :ok <- validate_id(turn_id, :turn),
         {:ok, subscriber, from} <- validate_subscription_options(opts) do
      call(session_id, {:subscribe, subscriber, turn_id, from})
    end
  end

  def subscribe(_target, opts), do: {:error, {:invalid_subscription_options, opts}}

  @doc """
  Removes an event subscription.
  """
  @spec unsubscribe(Subscription.t()) :: :ok | {:error, term()}
  def unsubscribe(%Subscription{session_id: session_id, ref: ref}) do
    call(session_id, {:unsubscribe, ref})
  end

  @doc """
  Returns a replay-safe stream for a turn.

  The stream must be consumed by the process that calls this function. It ends
  after the provider's terminal turn event.
  """
  @spec stream(Turn.t(), keyword()) :: {:ok, Enumerable.t()} | {:error, term()}
  def stream(turn, opts \\ [])

  def stream(%Turn{} = turn, opts) when is_list(opts) do
    with :ok <- validate_keyword_options(opts, :stream),
         timeout = Keyword.get(opts, :timeout, :infinity),
         :ok <- validate_timeout(timeout, :stream),
         subscription_opts = Keyword.take(opts, [:from]) |> Keyword.put_new(:from, :start),
         {:ok, subscription} <- subscribe(turn, subscription_opts) do
      stream =
        Stream.resource(
          fn -> {:open, subscription, Process.monitor(subscription.server)} end,
          &stream_next(&1, timeout),
          &close_stream/1
        )

      {:ok, stream}
    end
  end

  def stream(%Turn{}, opts), do: {:error, {:invalid_stream_options, opts}}

  @doc """
  Waits for a turn's terminal event without a completion-subscription race.
  """
  @spec await(Turn.t(), keyword()) :: {:ok, term()} | {:error, term()}
  def await(turn, opts \\ [])

  def await(%Turn{} = turn, opts) when is_list(opts) do
    with :ok <- validate_keyword_options(opts, :await),
         timeout = Keyword.get(opts, :timeout, :infinity),
         :ok <- validate_timeout(timeout, :await),
         {:ok, subscription} <- subscribe(turn, from: :start) do
      deadline = deadline(timeout)
      monitor = Process.monitor(subscription.server)

      try do
        await_terminal(subscription, monitor, deadline)
      after
        Process.demonitor(monitor, [:flush])
        unsubscribe(subscription)
      end
    end
  end

  def await(%Turn{}, opts), do: {:error, {:invalid_await_options, opts}}

  @doc """
  Responds exactly once to a structured provider request.
  """
  @spec respond(Request.t(), Response.t()) :: :ok | {:error, term()}
  def respond(%Request{session_id: session_id, id: request_id}, %Response{} = response) do
    call(session_id, {:respond, request_id, response})
  end

  @doc """
  Requests cancellation.

  The session remains cancelling until the provider emits a terminal event.
  """
  @spec cancel(Turn.t()) :: :ok | {:error, term()}
  def cancel(%Turn{session_id: session_id, id: turn_id}) do
    call(session_id, {:cancel, turn_id})
  end

  @doc """
  Returns the live session snapshot.
  """
  @spec status(SessionRef.t()) :: map() | {:error, :session_not_found}
  def status(%SessionRef{id: session_id}), do: call(session_id, :status)

  @doc """
  Returns provider capabilities for a live session.
  """
  @spec capabilities(SessionRef.t()) :: AgentHarness.Capabilities.t() | {:error, term()}
  def capabilities(%SessionRef{id: session_id}), do: call(session_id, :capabilities)

  @doc """
  Stops an idle session.

  Set `force: true` to request cancellation before closing an active session.
  """
  @spec stop_session(SessionRef.t(), keyword()) :: :ok | {:error, term()}
  def stop_session(session, opts \\ [])

  def stop_session(%SessionRef{id: session_id}, opts) when is_list(opts) do
    with :ok <- validate_id(session_id, :session),
         :ok <- validate_keyword_options(opts, :stop),
         force = Keyword.get(opts, :force, false),
         :ok <- validate_force(force) do
      call(session_id, {:stop, force})
    end
  end

  def stop_session(%SessionRef{}, opts), do: {:error, {:invalid_stop_options, opts}}

  @doc false
  @spec whereis(String.t()) :: pid() | nil
  def whereis(session_id) when is_binary(session_id) do
    case Registry.lookup(AgentHarness.SessionRegistry, session_id) do
      [{pid, _value}] -> pid
      [] -> nil
    end
  end

  defp call(session_id, message) do
    case whereis(session_id) do
      nil ->
        {:error, :session_not_found}

      pid ->
        try do
          GenServer.call(pid, message, @call_timeout)
        catch
          :exit, {:noproc, _details} -> {:error, :session_not_found}
          :exit, {:normal, _details} -> :ok
          :exit, {:timeout, _details} -> {:error, :session_call_timeout}
          :exit, reason -> {:error, {:session_call_failed, reason}}
        end
    end
  end

  defp stream_next({:done, subscription, monitor}, _timeout) do
    {:halt, {:done, subscription, monitor}}
  end

  defp stream_next({:open, subscription, monitor}, timeout) do
    case receive_event(subscription, monitor, timeout) do
      {:ok, event} when event.type in @terminal_events ->
        {[event], {:done, subscription, monitor}}

      {:ok, event} ->
        {[event], {:open, subscription, monitor}}

      {:down, reason} ->
        raise AgentHarness.SessionDownError, reason: reason

      :timeout ->
        raise AgentHarness.StreamTimeoutError, timeout: timeout
    end
  end

  defp close_stream({_status, subscription, monitor}) do
    Process.demonitor(monitor, [:flush])
    unsubscribe(subscription)
  end

  defp await_terminal(subscription, monitor, deadline) do
    timeout = remaining(deadline)

    case receive_event(subscription, monitor, timeout) do
      {:ok, %Event{type: :turn_completed, data: result}} ->
        {:ok, result}

      {:ok, %Event{type: type} = event} when type in @terminal_events ->
        {:error, event}

      {:ok, _event} ->
        await_terminal(subscription, monitor, deadline)

      {:down, reason} ->
        {:error, {:session_down, reason}}

      :timeout ->
        {:error, :timeout}
    end
  end

  defp receive_event(%Subscription{ref: subscription_ref}, monitor, :infinity) do
    receive do
      {:agent_harness, ^subscription_ref, %Event{} = event} -> {:ok, event}
      {:DOWN, ^monitor, :process, _pid, reason} -> {:down, reason}
    end
  end

  defp receive_event(%Subscription{ref: subscription_ref}, monitor, timeout) do
    receive do
      {:agent_harness, ^subscription_ref, %Event{} = event} -> {:ok, event}
      {:DOWN, ^monitor, :process, _pid, reason} -> {:down, reason}
    after
      timeout -> :timeout
    end
  end

  defp deadline(:infinity), do: :infinity

  defp deadline(timeout) when is_integer(timeout) and timeout >= 0 do
    System.monotonic_time(:millisecond) + timeout
  end

  defp remaining(:infinity), do: :infinity

  defp remaining(deadline) do
    max(deadline - System.monotonic_time(:millisecond), 0)
  end

  defp resolve_provider_module(provider, opts) do
    module =
      Keyword.get_lazy(opts, :provider_module, fn ->
        case provider do
          :codex -> AgentHarness.Providers.Codex
          :claude -> AgentHarness.Providers.Claude
          other -> other
        end
      end)

    if provider_module?(module) do
      {:ok, module}
    else
      {:error, {:invalid_provider_module, module}}
    end
  end

  defp provider_module?(module) when is_atom(module) do
    Code.ensure_loaded?(module) and
      Enum.all?(
        [
          open_session: 2,
          capabilities: 1,
          start_turn: 4,
          respond: 3,
          cancel: 2,
          close_session: 1
        ],
        fn {function, arity} -> function_exported?(module, function, arity) end
      )
  end

  defp provider_module?(_module), do: false

  defp build_session_config(session, opts) do
    {:ok, SessionConfig.new(session, opts)}
  rescue
    error ->
      {:error, {:invalid_session_options, error.__struct__, Exception.message(error)}}
  catch
    kind, reason ->
      {:error, {:invalid_session_options, {kind, reason}}}
  end

  defp validate_subscription_options(opts) do
    with :ok <- validate_keyword_options(opts, :subscription),
         subscriber = Keyword.get(opts, :pid, self()),
         true <- is_pid(subscriber) or {:error, {:invalid_subscriber, subscriber}},
         from = Keyword.get(opts, :from, :latest),
         true <- valid_replay_cursor?(from) or {:error, {:invalid_replay_cursor, from}} do
      {:ok, subscriber, from}
    end
  end

  defp validate_keyword_options(opts, kind) when is_list(opts) do
    if Keyword.keyword?(opts) do
      :ok
    else
      {:error, {:"invalid_#{kind}_options", opts}}
    end
  end

  defp validate_id(id, _kind) when is_binary(id) and byte_size(id) > 0, do: :ok
  defp validate_id(id, kind), do: {:error, {:"invalid_#{kind}_id", id}}

  defp validate_optional_id(nil, _kind), do: :ok
  defp validate_optional_id(id, kind), do: validate_id(id, kind)

  defp validate_timeout(:infinity, _kind), do: :ok
  defp validate_timeout(timeout, _kind) when is_integer(timeout) and timeout >= 0, do: :ok
  defp validate_timeout(timeout, kind), do: {:error, {:"invalid_#{kind}_timeout", timeout}}

  defp validate_force(force) when is_boolean(force), do: :ok
  defp validate_force(force), do: {:error, {:invalid_force, force}}

  defp valid_replay_cursor?(:latest), do: true
  defp valid_replay_cursor?(:start), do: true

  defp valid_replay_cursor?({:after, sequence})
       when is_integer(sequence) and sequence >= 0,
       do: true

  defp valid_replay_cursor?(_cursor), do: false
end
