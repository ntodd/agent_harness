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
  def start_session(provider, opts \\ []) when is_atom(provider) and is_list(opts) do
    provider_module =
      Keyword.get_lazy(opts, :provider_module, fn -> provider_module(provider) end)

    session = SessionRef.new(provider, id: Keyword.get(opts, :id, AgentHarness.ID.generate()))

    config_opts = Keyword.drop(opts, [:id, :provider_module])
    config = SessionConfig.new(session, config_opts)

    child_opts = [session: session, config: config, provider: provider_module]

    case DynamicSupervisor.start_child(@session_supervisor, {SessionServer, child_opts}) do
      {:ok, _pid} -> {:ok, session}
      {:error, {:already_started, _pid}} -> {:error, :session_already_exists}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Starts one turn. A session rejects a second concurrent turn.
  """
  @spec start_turn(SessionRef.t(), term(), keyword()) :: {:ok, Turn.t()} | {:error, term()}
  def start_turn(%SessionRef{id: session_id}, input, opts \\ []) do
    call(session_id, {:start_turn, input, opts})
  end

  @doc """
  Subscribes the calling process, or `:pid`, to session or turn events.

  `:from` may be `:latest`, `:start`, or `{:after, sequence}`.
  """
  @spec subscribe(SessionRef.t() | Turn.t(), keyword()) ::
          {:ok, Subscription.t()} | {:error, term()}
  def subscribe(target, opts \\ [])

  def subscribe(%SessionRef{id: session_id}, opts) do
    subscribe_to(session_id, nil, opts)
  end

  def subscribe(%Turn{session_id: session_id, id: turn_id}, opts) do
    subscribe_to(session_id, turn_id, opts)
  end

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
  def stream(%Turn{} = turn, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, :infinity)
    subscription_opts = Keyword.take(opts, [:from]) |> Keyword.put_new(:from, :start)

    with {:ok, subscription} <- subscribe(turn, subscription_opts) do
      stream =
        Stream.resource(
          fn -> {:open, subscription, Process.monitor(subscription.server)} end,
          &stream_next(&1, timeout),
          &close_stream/1
        )

      {:ok, stream}
    end
  end

  @doc """
  Waits for a turn's terminal event without a completion-subscription race.
  """
  @spec await(Turn.t(), keyword()) :: {:ok, term()} | {:error, term()}
  def await(%Turn{} = turn, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, :infinity)

    with {:ok, subscription} <- subscribe(turn, from: :start) do
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
  def stop_session(%SessionRef{id: session_id}, opts \\ []) do
    call(session_id, {:stop, Keyword.get(opts, :force, false)})
  end

  @doc false
  @spec whereis(String.t()) :: pid() | nil
  def whereis(session_id) when is_binary(session_id) do
    case Registry.lookup(AgentHarness.SessionRegistry, session_id) do
      [{pid, _value}] -> pid
      [] -> nil
    end
  end

  defp subscribe_to(session_id, turn_id, opts) do
    subscriber = Keyword.get(opts, :pid, self())
    from = Keyword.get(opts, :from, :latest)
    call(session_id, {:subscribe, subscriber, turn_id, from})
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

  defp provider_module(:codex), do: AgentHarness.Providers.Codex
  defp provider_module(:claude), do: AgentHarness.Providers.Claude

  defp provider_module(provider) do
    if Code.ensure_loaded?(provider) and function_exported?(provider, :open_session, 2) do
      provider
    else
      raise ArgumentError, "unknown provider: #{inspect(provider)}"
    end
  end
end
