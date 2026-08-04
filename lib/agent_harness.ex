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
    Telemetry,
    Turn
  }

  @session_supervisor AgentHarness.SessionSupervisor
  @terminal_events [:turn_completed, :turn_failed, :turn_cancelled, :turn_interrupted]
  @default_call_timeout 30_000
  @default_provider_command_call_timeout 60_000

  @doc """
  Starts a supervised logical session for `provider`.

  Authentication remains the responsibility of the locally installed CLI.
  Provider opening and initial Store finalization have separate bounded phases
  (`:startup_timeout` and `:startup_finalization_timeout`). Independent session
  handshakes are not serialized; call from a supervised task when the caller
  itself must remain responsive. Before returning success, the caller and
  SessionServer complete a two-way readiness acknowledgement so a queued ready
  message cannot return a handle to a server that already died.
  """
  @spec start_session(atom(), keyword()) :: {:ok, SessionRef.t()} | {:error, term()}
  def start_session(provider, opts \\ [])

  def start_session(provider, opts) when is_atom(provider) and is_list(opts) do
    Telemetry.span([:command, :start_session], %{provider: provider}, fn ->
      do_start_session(provider, opts)
    end)
  end

  def start_session(provider, _opts) when not is_atom(provider),
    do: {:error, {:invalid_provider, provider}}

  def start_session(_provider, opts), do: {:error, {:invalid_session_options, opts}}

  @doc """
  Accepts one turn locally and starts provider admission asynchronously.

  The returned turn initially has status `:starting`. A session rejects a
  second concurrent turn. If the local SessionServer call times out, the error
  includes the stable turn handle for reconciliation.
  """
  @spec start_turn(SessionRef.t(), term(), keyword()) :: {:ok, Turn.t()} | {:error, term()}
  def start_turn(session, input, opts \\ [])

  def start_turn(%SessionRef{id: session_id, provider: provider}, input, opts)
      when is_list(opts) do
    metadata = %{session_id: session_id, provider: provider}

    Telemetry.span([:command, :start_turn], metadata, fn ->
      do_start_turn(session_id, input, opts)
    end)
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
    idempotent_call(session_id, {:unsubscribe, ref})
  end

  @doc """
  Returns a replay-followed-by-live stream for a turn.

  The stream must be consumed by the process that calls this function. It ends
  after a terminal turn event included by the replay cursor or delivered live.
  The per-event timeout defaults to `:infinity`; pass a finite `:timeout` for
  bounded callers. A completed turn with no terminal event in the requested
  replay returns `{:error, :replay_unavailable}`.
  """
  @spec stream(Turn.t(), keyword()) :: {:ok, Enumerable.t()} | {:error, term()}
  def stream(turn, opts \\ [])

  def stream(%Turn{} = turn, opts) when is_list(opts) do
    with :ok <- validate_keyword_options(opts, :stream),
         timeout = Keyword.get(opts, :timeout, :infinity),
         :ok <- validate_timeout(timeout, :stream),
         from = Keyword.get(opts, :from, :start),
         true <- valid_replay_cursor?(from) or {:error, {:invalid_replay_cursor, from}},
         {:ok, subscription} <-
           call(turn.session_id, {:subscribe_stream, self(), turn.id, from}) do
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

  The event-wait timeout defaults to `:infinity`. Do not call this blocking
  function from a GenServer callback; subscribe and monitor the session
  instead.
  """
  @spec await(Turn.t(), keyword()) :: {:ok, term()} | {:error, term()}
  def await(turn, opts \\ [])

  def await(%Turn{} = turn, opts) when is_list(opts) do
    with :ok <- validate_keyword_options(opts, :await),
         timeout = Keyword.get(opts, :timeout, :infinity),
         :ok <- validate_timeout(timeout, :await),
         result <- call(turn.session_id, {:await, self(), turn.id}) do
      case result do
        {:terminal, terminal_event} ->
          terminal_result(terminal_event)

        {:ok, subscription} ->
          deadline = deadline(timeout)
          monitor = Process.monitor(subscription.server)

          try do
            await_terminal(subscription, monitor, deadline)
          after
            Process.demonitor(monitor, [:flush])
            unsubscribe(subscription)
          end

        error ->
          error
      end
    end
  end

  def await(%Turn{}, opts), do: {:error, {:invalid_await_options, opts}}

  @doc """
  Responds to a structured provider request with exactly-once local ownership.

  The call waits for provider acknowledgement, while the owning SessionServer
  remains responsive to status, cancellation, and shutdown commands. A second
  in-flight response returns `{:error, :response_in_progress}`; a definite
  provider rejection releases the claim. Uncertain acknowledgement returns
  `{:error, {:provider_command_uncertain, reason}}`, fails the active turn, and
  retires the session. The public response-call timeout must remain longer than
  the per-session provider-command watchdog.
  """
  @spec respond(Request.t(), Response.t()) :: :ok | {:error, term()}
  def respond(%Request{session_id: session_id, id: request_id}, %Response{} = response) do
    with :ok <- Response.validate(response) do
      call(
        session_id,
        {:respond, request_id, response},
        provider_command_call_timeout()
      )
    end
  end

  @doc """
  Requests cancellation.

  `:ok` records the intent and schedules at most one provider command; it is not
  provider acknowledgement. The session remains cancelling until the provider
  emits a terminal event. A later provider-command failure retires the session
  and is observed through events and the session monitor.
  """
  @spec cancel(Turn.t()) :: :ok | {:error, term()}
  def cancel(%Turn{session_id: session_id, id: turn_id}) do
    call(session_id, {:cancel, turn_id})
  end

  @doc """
  Returns the live session snapshot.
  """
  @spec status(SessionRef.t()) :: map() | {:error, term()}
  def status(%SessionRef{id: session_id}), do: call(session_id, :status)

  @doc """
  Monitors a live session from the calling process.

  The caller receives the standard `{:DOWN, monitor_ref, :process, pid, reason}`
  message if the session exits. This is the non-blocking lifecycle signal to use
  from an orchestrator GenServer.
  """
  @spec monitor(SessionRef.t() | Subscription.t()) ::
          {:ok, reference()} | {:error, :session_not_found}
  def monitor(%SessionRef{id: session_id}) do
    case whereis(session_id) do
      nil -> {:error, :session_not_found}
      pid -> {:ok, Process.monitor(pid)}
    end
  end

  def monitor(%Subscription{server: server}) when is_pid(server) do
    {:ok, Process.monitor(server)}
  end

  @doc """
  Lists the currently live, PID-free session handles in deterministic ID order.
  """
  @spec list_sessions() :: [SessionRef.t()]
  def list_sessions do
    AgentHarness.SessionRegistry
    |> Registry.select([
      {{:"$1", :"$2", :"$3"}, [], [{{:"$2", :"$3"}}]}
    ])
    |> Enum.flat_map(fn
      {pid, %SessionRef{} = session} ->
        if Process.alive?(pid), do: [session], else: []

      {_pid, _value} ->
        []
    end)
    |> Enum.sort_by(& &1.id)
  end

  @doc """
  Lists stored session snapshots and whether each has a live SessionServer.

  The Store defaults to the built-in Memory instance. Pass
  `store: {module, owner}` to inspect a custom Store; a PID-free session handle
  does not retain that ownership information after its process stops.
  """
  @spec list_stored_sessions(keyword()) :: {:ok, [map()]} | {:error, term()}
  def list_stored_sessions(opts \\ []) when is_list(opts) do
    with :ok <- validate_keyword_options(opts, :stored_session),
         {:ok, {module, owner}} <- store_option(opts) do
      sessions =
        module.list_sessions(owner)
        |> Enum.map(fn {session_id, snapshot} ->
          %{session_id: session_id, snapshot: snapshot, live?: not is_nil(whereis(session_id))}
        end)

      {:ok, sessions}
    end
  rescue
    error -> {:error, {:store_read_failed, error.__struct__, Exception.message(error)}}
  catch
    kind, reason -> {:error, {:store_read_failed, {kind, reason}}}
  end

  @doc """
  Cascades deletion of a non-live session aggregate from a Store.

  The Store defaults to the built-in Memory instance. Pass `:store` again for
  a custom Store because a PID-free stopped handle does not retain its owner.
  """
  @spec purge_session(SessionRef.t() | String.t(), keyword()) :: :ok | {:error, term()}
  def purge_session(session_or_id, opts \\ []) when is_list(opts) do
    session_id = session_id(session_or_id)

    with :ok <- validate_id(session_id, :session),
         :ok <- ensure_session_not_live(session_id),
         {:ok, {module, owner}} <- store_option(opts) do
      module.delete_session(owner, session_id)
    end
  rescue
    error -> {:error, {:store_delete_failed, error.__struct__, Exception.message(error)}}
  catch
    kind, reason -> {:error, {:store_delete_failed, {kind, reason}}}
  end

  @doc """
  Returns provider capabilities for a live session.
  """
  @spec capabilities(SessionRef.t()) :: AgentHarness.Capabilities.t() | {:error, term()}
  def capabilities(%SessionRef{id: session_id}), do: call(session_id, :capabilities)

  @doc """
  Stops an idle session.

  Set `force: true` to interrupt an active turn locally and close the session.
  Forced shutdown does not call the provider cancellation command; it kills
  in-flight provider tasks and retires the local provider session.
  """
  @spec stop_session(SessionRef.t(), keyword()) :: :ok | {:error, term()}
  def stop_session(session, opts \\ [])

  def stop_session(%SessionRef{id: session_id}, opts) when is_list(opts) do
    with :ok <- validate_id(session_id, :session),
         :ok <- validate_keyword_options(opts, :stop),
         force = Keyword.get(opts, :force, false),
         :ok <- validate_force(force) do
      stop_call(session_id, {:stop, force})
    end
  end

  def stop_session(%SessionRef{}, opts), do: {:error, {:invalid_stop_options, opts}}

  @doc false
  @spec whereis(String.t()) :: pid() | nil
  def whereis(session_id) when is_binary(session_id) do
    case Registry.lookup(AgentHarness.SessionRegistry, session_id) do
      [{pid, _value}] -> if Process.alive?(pid), do: pid
      [] -> nil
    end
  end

  defp do_start_session(provider, opts) do
    with :ok <- validate_keyword_options(opts, :session),
         id = Keyword.get(opts, :id, AgentHarness.ID.generate()),
         :ok <- validate_id(id, :session),
         reuse = Keyword.get(opts, :reuse, :never),
         :ok <- validate_reuse(reuse),
         {:ok, provider_module} <- resolve_provider_module(provider, opts),
         session = SessionRef.new(provider, id: id),
         {:ok, config} <-
           build_session_config(
             session,
             Keyword.drop(opts, [:id, :provider_module, :reuse])
           ) do
      start_session_child(session, config, provider_module, reuse)
    end
  end

  defp start_session_child(session, config, provider_module, reuse) do
    start_ref = make_ref()

    child_opts = [
      session: session,
      config: config,
      provider: provider_module,
      starter: {self(), start_ref},
      reuse: reuse
    ]

    case DynamicSupervisor.start_child(@session_supervisor, {SessionServer, child_opts}) do
      {:ok, pid} -> await_session_start(pid, start_ref, session, config)
      {:error, {:already_started, _pid}} -> {:error, :session_already_exists}
      {:error, reason} -> {:error, reason}
    end
  end

  defp do_start_turn(session_id, input, opts) do
    with :ok <- validate_id(session_id, :session),
         :ok <- validate_keyword_options(opts, :turn),
         :ok <- validate_optional_id(Keyword.get(opts, :id), :turn) do
      turn_options = Keyword.take(opts, [:id, :metadata])
      turn = Turn.new(session_id, input, turn_options)
      provider_options = Keyword.drop(opts, [:id, :metadata])
      explicit_id? = Keyword.has_key?(opts, :id)
      start_turn_call(session_id, turn, input, provider_options, explicit_id?)
    end
  end

  defp start_turn_call(session_id, turn, input, provider_options, explicit_id?) do
    message = {:start_turn, turn, input, provider_options, explicit_id?}

    case call(session_id, message) do
      {:error, :session_call_timeout} -> {:error, {:session_call_timeout, turn}}
      result -> result
    end
  end

  defp call(session_id, message), do: call(session_id, message, call_timeout())

  defp call(session_id, message, timeout) do
    case whereis(session_id) do
      nil ->
        {:error, :session_not_found}

      pid ->
        try do
          GenServer.call(pid, message, timeout)
        catch
          :exit, {:noproc, _details} -> {:error, :session_not_found}
          :exit, {:normal, _details} -> {:error, :session_not_found}
          :exit, {:timeout, _details} -> {:error, :session_call_timeout}
          :exit, reason -> {:error, {:session_call_failed, reason}}
        end
    end
  end

  defp idempotent_call(session_id, message) do
    case call(session_id, message) do
      {:error, :session_not_found} -> :ok
      result -> result
    end
  end

  defp stop_call(session_id, message) do
    case whereis(session_id) do
      nil ->
        :ok

      pid ->
        stop_live_session(session_id, pid, message)
    end
  end

  defp stop_live_session(session_id, pid, message) do
    monitor = Process.monitor(pid)

    case idempotent_call(session_id, message) do
      :ok -> finish_session_stop(session_id, monitor, pid)
      error -> demonitor_and_return(monitor, error)
    end
  end

  defp finish_session_stop(session_id, monitor, pid) do
    case await_session_stop(monitor, pid) do
      :ok -> await_registry_release(session_id)
      error -> error
    end
  end

  defp demonitor_and_return(monitor, result) do
    Process.demonitor(monitor, [:flush])
    result
  end

  defp await_session_stop(monitor, pid) do
    receive do
      {:DOWN, ^monitor, :process, ^pid, _reason} -> :ok
    after
      call_timeout() ->
        Process.demonitor(monitor, [:flush])
        {:error, :session_stop_timeout}
    end
  end

  defp await_session_start(pid, start_ref, session, config) do
    monitor = Process.monitor(pid)

    receive do
      {SessionServer, ^start_ref, {:phase, :finalizing}} ->
        await_session_finalization(pid, start_ref, session, monitor, config)

      {SessionServer, ^start_ref, {:ok, ^pid}} ->
        acknowledge_session_start(pid, start_ref, session, monitor, config)

      {SessionServer, ^start_ref, {:error, reason}} ->
        await_start_failure_down(monitor, pid, session.id)
        {:error, reason}

      {:DOWN, ^monitor, :process, ^pid, reason} ->
        result =
          receive do
            {SessionServer, ^start_ref, {:error, startup_reason}} -> {:error, startup_reason}
          after
            0 -> {:error, normalize_start_exit(reason)}
          end

        await_registry_release(session.id)
        result
    after
      config.startup_timeout + 100 ->
        if Process.alive?(pid), do: Process.exit(pid, :kill)
        await_timed_out_session_down(monitor, pid, session.id)
        {:error, :session_start_timeout}
    end
  end

  defp await_session_finalization(pid, start_ref, session, monitor, config) do
    receive do
      {SessionServer, ^start_ref, {:ok, ^pid}} ->
        acknowledge_session_start(pid, start_ref, session, monitor, config)

      {SessionServer, ^start_ref, {:error, reason}} ->
        await_start_failure_down(monitor, pid, session.id)
        {:error, reason}

      {:DOWN, ^monitor, :process, ^pid, reason} ->
        result =
          receive do
            {SessionServer, ^start_ref, {:error, startup_reason}} -> {:error, startup_reason}
          after
            0 -> {:error, normalize_start_exit(reason)}
          end

        await_registry_release(session.id)
        result
    after
      config.startup_finalization_timeout * 3 + 100 ->
        if Process.alive?(pid), do: Process.exit(pid, :kill)
        await_timed_out_session_down(monitor, pid, session.id)
        {:error, :session_start_finalization_timeout}
    end
  end

  defp acknowledge_session_start(pid, start_ref, session, monitor, config) do
    send(pid, {SessionServer, start_ref, :starter_ack})

    receive do
      {SessionServer, ^start_ref, :starter_acknowledged} ->
        Process.demonitor(monitor, [:flush])
        {:ok, session}

      {:DOWN, ^monitor, :process, ^pid, reason} ->
        await_registry_release(session.id)
        {:error, normalize_start_exit(reason)}
    after
      config.startup_finalization_timeout + 100 ->
        if Process.alive?(pid), do: Process.exit(pid, :kill)
        await_timed_out_session_down(monitor, pid, session.id)
        {:error, :session_start_ack_timeout}
    end
  end

  defp await_timed_out_session_down(monitor, pid, session_id) do
    receive do
      {:DOWN, ^monitor, :process, ^pid, _reason} -> :ok
    after
      1_000 -> Process.demonitor(monitor, [:flush])
    end

    await_registry_release(session_id)
  end

  defp normalize_start_exit({:shutdown, reason}), do: reason
  defp normalize_start_exit(reason), do: reason

  defp await_start_failure_down(monitor, pid, session_id) do
    receive do
      {:DOWN, ^monitor, :process, ^pid, _reason} -> await_registry_release(session_id)
    after
      1_000 ->
        Process.demonitor(monitor, [:flush])
        if Process.alive?(pid), do: Process.exit(pid, :kill)
        :ok
    end
  end

  defp await_registry_release(session_id, attempts \\ 100)

  defp await_registry_release(_session_id, 0), do: :ok

  defp await_registry_release(session_id, attempts) do
    if whereis(session_id) do
      Process.sleep(1)
      await_registry_release(session_id, attempts - 1)
    else
      :ok
    end
  end

  defp call_timeout do
    Application.get_env(:agent_harness, :session_call_timeout, @default_call_timeout)
  end

  defp provider_command_call_timeout do
    Application.get_env(
      :agent_harness,
      :provider_command_call_timeout,
      @default_provider_command_call_timeout
    )
  end

  defp store_option(opts) do
    case Keyword.get(opts, :store, {AgentHarness.Store.Memory, AgentHarness.Store.Memory}) do
      {module, owner} when is_atom(module) -> {:ok, {module, owner}}
      store -> {:error, {:invalid_store, store}}
    end
  end

  defp session_id(%SessionRef{id: session_id}), do: session_id
  defp session_id(session_id) when is_binary(session_id), do: session_id
  defp session_id(other), do: other

  defp ensure_session_not_live(session_id) do
    if whereis(session_id), do: {:error, :session_active}, else: :ok
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
      {:ok, %Event{type: type} = event} when type in @terminal_events ->
        terminal_result(event)

      {:ok, _event} ->
        await_terminal(subscription, monitor, deadline)

      {:down, reason} ->
        {:error, {:session_down, reason}}

      :timeout ->
        {:error, :timeout}
    end
  end

  defp terminal_result(%Event{type: :turn_completed, data: result}), do: {:ok, result}
  defp terminal_result(%Event{} = event), do: {:error, event}

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
          :pi -> AgentHarness.Providers.Pi
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

  defp validate_reuse(reuse) when reuse in [:never, :closed, :replace], do: :ok
  defp validate_reuse(reuse), do: {:error, {:invalid_reuse, reuse}}

  defp valid_replay_cursor?(:latest), do: true
  defp valid_replay_cursor?(:start), do: true

  defp valid_replay_cursor?({:after, sequence})
       when is_integer(sequence) and sequence >= 0,
       do: true

  defp valid_replay_cursor?(_cursor), do: false
end
