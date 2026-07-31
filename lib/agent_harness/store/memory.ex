defmodule AgentHarness.Store.Memory do
  @moduledoc """
  Supervised, process-owned in-memory implementation of `AgentHarness.Store`.

  The GenServer is the sole owner and writer of its state. Data does not belong
  to, and is not automatically removed with, the SessionServer process that
  wrote it. `delete_session/2` is the only operation that purges a session and
  it cascades to that session's turns, events, and requests.

  State is intentionally lost if this store process terminates. It is suitable
  for tests and local ephemeral use; durable adapters can implement the same
  behaviour without changing SessionServer.

  One SessionServer should serialize event writes for a given session. Exact
  retries are idempotent, while conflicting or out-of-order writes are rejected.
  Sequence gaps are allowed because callers may choose not to persist every
  high-volume delta.
  """

  use GenServer

  @behaviour AgentHarness.Store

  alias AgentHarness.{Event, Request, Turn}
  alias AgentHarness.Internal.OwnedTask

  defmodule State do
    @moduledoc false

    defstruct sessions: %{}, turns: %{}, events: %{}, requests: %{}
  end

  defmodule EventLog do
    @moduledoc false

    defstruct last_sequence: nil, reversed: [], by_sequence: %{}, by_turn: %{}, ids: %{}
  end

  @type option :: {:name, GenServer.name()} | {:id, term()}

  @doc """
  Starts an empty store.

  Pass `name: MyStore` when the store will be addressed by a supervised name.
  """
  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts \\ []) when is_list(opts) do
    genserver_opts =
      case Keyword.fetch(opts, :name) do
        {:ok, name} -> [name: name]
        :error -> []
      end

    GenServer.start_link(__MODULE__, :empty, genserver_opts)
  end

  @doc false
  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :id, Keyword.get(opts, :name, __MODULE__)),
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      shutdown: 5_000
    }
  end

  @impl AgentHarness.Store
  def save_session(owner, session_id, snapshot) when is_binary(session_id) do
    call(owner, {:save_session, session_id, snapshot})
  end

  @impl AgentHarness.Store
  def fetch_session(owner, session_id) when is_binary(session_id) do
    call(owner, {:fetch_session, session_id})
  end

  @impl AgentHarness.Store
  def list_sessions(owner), do: call(owner, :list_sessions)

  @impl AgentHarness.Store
  def delete_session(owner, session_id) when is_binary(session_id) do
    task_owner = OwnedTask.owner()

    case live_session(session_id) do
      nil -> call(owner, {:delete_session, session_id})
      pid when pid == self() -> call(owner, {:delete_session, session_id})
      pid when pid == task_owner -> call(owner, {:delete_session, session_id})
      _pid -> {:error, :session_active}
    end
  end

  @impl AgentHarness.Store
  def save_turn(owner, %Turn{} = turn) do
    call(owner, {:save_turn, turn})
  end

  @impl AgentHarness.Store
  def fetch_turn(owner, session_id, turn_id)
      when is_binary(session_id) and is_binary(turn_id) do
    call(owner, {:fetch_turn, session_id, turn_id})
  end

  @impl AgentHarness.Store
  def list_turns(owner, session_id) when is_binary(session_id) do
    call(owner, {:list_turns, session_id})
  end

  @impl AgentHarness.Store
  def append_event(owner, %Event{} = event) do
    call(owner, {:append_event, event})
  end

  @doc """
  Reads events in ascending sequence order.

  `:after` is exclusive. `:limit` defaults to `:infinity`. Passing `:turn_id`
  selects that turn through the Store's per-turn index before applying the
  cursor and limit.
  """
  @impl AgentHarness.Store
  @spec events(GenServer.server(), String.t(), keyword()) ::
          {:ok, [Event.t()]} | {:error, term()}
  def events(owner, session_id, opts \\ []) when is_binary(session_id) and is_list(opts) do
    opts = Keyword.validate!(opts, after: nil, limit: :infinity, turn_id: nil)
    after_sequence = validate_after!(opts[:after])
    limit = validate_limit!(opts[:limit])
    turn_id = validate_turn_id!(opts[:turn_id])

    call(owner, {:events, session_id, after_sequence, limit, turn_id})
  end

  @impl AgentHarness.Store
  def latest_sequence(owner, session_id) when is_binary(session_id) do
    call(owner, {:latest_sequence, session_id})
  end

  @impl AgentHarness.Store
  def save_request(owner, %Request{} = request) do
    call(owner, {:save_request, request})
  end

  @impl AgentHarness.Store
  def fetch_request(owner, session_id, request_id)
      when is_binary(session_id) and is_binary(request_id) do
    call(owner, {:fetch_request, session_id, request_id})
  end

  @doc """
  Lists requests deterministically, optionally filtering by turn and status.
  """
  @impl AgentHarness.Store
  @spec list_requests(GenServer.server(), String.t(), keyword()) ::
          {:ok, [Request.t()]} | {:error, term()}
  def list_requests(owner, session_id, opts \\ [])
      when is_binary(session_id) and is_list(opts) do
    opts = Keyword.validate!(opts, turn_id: nil, status: nil)
    call(owner, {:list_requests, session_id, opts[:turn_id], opts[:status]})
  end

  @impl true
  def init(:empty), do: {:ok, %State{}}

  @impl true
  def handle_call({:save_session, session_id, snapshot}, _from, state) do
    {:reply, :ok, put_in(state.sessions[session_id], snapshot)}
  end

  def handle_call({:fetch_session, session_id}, _from, state) do
    {:reply, fetch(state.sessions, session_id), state}
  end

  def handle_call(:list_sessions, _from, state) do
    sessions = Enum.sort_by(state.sessions, fn {session_id, _snapshot} -> session_id end)
    {:reply, sessions, state}
  end

  def handle_call({:delete_session, session_id}, _from, state) do
    state = %{
      state
      | sessions: Map.delete(state.sessions, session_id),
        turns: Map.delete(state.turns, session_id),
        events: Map.delete(state.events, session_id),
        requests: Map.delete(state.requests, session_id)
    }

    {:reply, :ok, state}
  end

  def handle_call({:save_turn, %Turn{} = turn}, _from, state) do
    if session?(state, turn.session_id) do
      turns = put_owned(state.turns, turn.session_id, turn.id, turn)
      {:reply, :ok, %{state | turns: turns}}
    else
      {:reply, {:error, :session_not_found}, state}
    end
  end

  def handle_call({:fetch_turn, session_id, turn_id}, _from, state) do
    reply =
      if session?(state, session_id) do
        state.turns
        |> Map.get(session_id, %{})
        |> fetch(turn_id)
      else
        {:error, :session_not_found}
      end

    {:reply, reply, state}
  end

  def handle_call({:list_turns, session_id}, _from, state) do
    reply =
      if session?(state, session_id) do
        turns =
          state.turns
          |> Map.get(session_id, %{})
          |> Map.values()
          |> Enum.sort_by(& &1.id)

        {:ok, turns}
      else
        {:error, :session_not_found}
      end

    {:reply, reply, state}
  end

  def handle_call({:append_event, %Event{} = event}, _from, state) do
    with :ok <- require_session(state, event.session_id),
         :ok <- require_turn(state, event.session_id, event.turn_id),
         {:ok, log} <- append_to_log(Map.get(state.events, event.session_id), event) do
      {:reply, :ok, put_in(state.events[event.session_id], log)}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:events, session_id, after_sequence, limit, turn_id}, _from, state) do
    reply =
      if session?(state, session_id) do
        log = Map.get(state.events, session_id, %EventLog{})

        events =
          log
          |> events_for_turn(turn_id)
          |> after_sequence(after_sequence)
          |> take(limit)

        {:ok, events}
      else
        {:error, :session_not_found}
      end

    {:reply, reply, state}
  end

  def handle_call({:latest_sequence, session_id}, _from, state) do
    reply =
      if session?(state, session_id) do
        {:ok, get_in(state.events, [session_id, Access.key(:last_sequence)])}
      else
        {:error, :session_not_found}
      end

    {:reply, reply, state}
  end

  def handle_call({:save_request, %Request{} = request}, _from, state) do
    with :ok <- require_session(state, request.session_id),
         :ok <- require_turn(state, request.session_id, request.turn_id) do
      requests = put_owned(state.requests, request.session_id, request.id, request)
      {:reply, :ok, %{state | requests: requests}}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:fetch_request, session_id, request_id}, _from, state) do
    reply =
      if session?(state, session_id) do
        state.requests
        |> Map.get(session_id, %{})
        |> fetch(request_id)
      else
        {:error, :session_not_found}
      end

    {:reply, reply, state}
  end

  def handle_call({:list_requests, session_id, turn_id, status}, _from, state) do
    reply =
      if session?(state, session_id) do
        requests =
          state.requests
          |> Map.get(session_id, %{})
          |> Map.values()
          |> filter_requests(turn_id, status)
          |> Enum.sort_by(& &1.id)

        {:ok, requests}
      else
        {:error, :session_not_found}
      end

    {:reply, reply, state}
  end

  defp append_to_log(nil, event), do: append_to_log(%EventLog{}, event)

  defp append_to_log(%EventLog{} = log, %Event{} = event) do
    case Map.fetch(log.by_sequence, event.seq) do
      {:ok, ^event} ->
        {:ok, log}

      {:ok, _different_event} ->
        {:error, {:event_conflict, event.seq}}

      :error ->
        append_new_event(log, event)
    end
  end

  defp append_new_event(%EventLog{} = log, %Event{} = event) do
    cond do
      Map.has_key?(log.ids, event.id) ->
        {:error, {:event_id_conflict, event.id}}

      is_integer(log.last_sequence) and event.seq <= log.last_sequence ->
        {:error, {:out_of_order, log.last_sequence}}

      true ->
        {:ok,
         %EventLog{
           last_sequence: event.seq,
           reversed: [event | log.reversed],
           by_sequence: Map.put(log.by_sequence, event.seq, event),
           by_turn: put_turn_event(log.by_turn, event),
           ids: Map.put(log.ids, event.id, event.seq)
         }}
    end
  end

  defp require_session(state, session_id) do
    if session?(state, session_id), do: :ok, else: {:error, :session_not_found}
  end

  defp require_turn(_state, _session_id, nil), do: :ok

  defp require_turn(state, session_id, turn_id) do
    if get_in(state.turns, [session_id, turn_id]) do
      :ok
    else
      {:error, :turn_not_found}
    end
  end

  defp session?(state, session_id), do: Map.has_key?(state.sessions, session_id)

  defp put_owned(collection, session_id, child_id, value) do
    Map.update(collection, session_id, %{child_id => value}, &Map.put(&1, child_id, value))
  end

  defp fetch(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} -> {:ok, value}
      :error -> :not_found
    end
  end

  defp after_sequence(events, nil), do: events

  defp after_sequence(events, sequence) do
    Enum.drop_while(events, &(&1.seq <= sequence))
  end

  defp take(events, :infinity), do: events
  defp take(events, limit), do: Enum.take(events, limit)

  defp events_for_turn(%EventLog{} = log, nil), do: Enum.reverse(log.reversed)

  defp events_for_turn(%EventLog{} = log, turn_id) do
    log.by_turn
    |> Map.get(turn_id, [])
    |> Enum.reverse()
  end

  defp put_turn_event(by_turn, %Event{turn_id: nil}), do: by_turn

  defp put_turn_event(by_turn, %Event{turn_id: turn_id} = event) do
    Map.update(by_turn, turn_id, [event], &[event | &1])
  end

  defp filter_requests(requests, turn_id, status) do
    Enum.filter(requests, fn request ->
      (is_nil(turn_id) or request.turn_id == turn_id) and
        (is_nil(status) or request.status == status)
    end)
  end

  defp validate_after!(nil), do: nil
  defp validate_after!(sequence) when is_integer(sequence) and sequence >= 0, do: sequence

  defp validate_after!(value) do
    raise ArgumentError, ":after must be a non-negative integer, got: #{inspect(value)}"
  end

  defp validate_limit!(:infinity), do: :infinity
  defp validate_limit!(limit) when is_integer(limit) and limit >= 0, do: limit

  defp validate_limit!(value) do
    raise ArgumentError,
          ":limit must be a non-negative integer or :infinity, got: #{inspect(value)}"
  end

  defp validate_turn_id!(nil), do: nil
  defp validate_turn_id!(turn_id) when is_binary(turn_id) and byte_size(turn_id) > 0, do: turn_id

  defp validate_turn_id!(value) do
    raise ArgumentError, ":turn_id must be a non-empty string, got: #{inspect(value)}"
  end

  defp call(owner, message) do
    timeout = Application.get_env(:agent_harness, :store_call_timeout, 5_000)
    GenServer.call(owner, message, timeout)
  end

  defp live_session(session_id) do
    AgentHarness.whereis(session_id)
  rescue
    _error -> nil
  catch
    :exit, _reason -> nil
  end
end
