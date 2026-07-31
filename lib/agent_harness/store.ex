defmodule AgentHarness.Store do
  @moduledoc """
  Persistence boundary for logical agent-session state.

  A session is the aggregate root: turns, events, and requests are owned by a
  session and must not outlive it. Store implementations serialize writes made
  by a session before making them visible to readers.

  Session snapshots are deliberately opaque. This keeps the persistence layer
  independent of `AgentHarness.SessionServer`'s private state while allowing a
  durable adapter to restore the public and provider identifiers it needs.

  Events are append-only. `events/3` uses an exclusive sequence cursor, so
  `after: 7` returns events whose sequence is greater than seven.
  """

  alias AgentHarness.{Event, Request, Turn}

  @type owner :: GenServer.server()
  @type session_id :: String.t()
  @type session_snapshot :: term()
  @type event_options :: [
          after: non_neg_integer(),
          limit: non_neg_integer() | :infinity
        ]
  @type request_options :: [
          turn_id: String.t(),
          status: Request.status()
        ]

  @callback save_session(owner(), session_id(), session_snapshot()) ::
              :ok | {:error, term()}

  @callback fetch_session(owner(), session_id()) ::
              {:ok, session_snapshot()} | :not_found | {:error, term()}

  @callback list_sessions(owner()) :: [{session_id(), session_snapshot()}]

  @callback delete_session(owner(), session_id()) :: :ok | {:error, term()}

  @callback save_turn(owner(), Turn.t()) :: :ok | {:error, term()}

  @callback fetch_turn(owner(), session_id(), String.t()) ::
              {:ok, Turn.t()} | :not_found | {:error, term()}

  @callback list_turns(owner(), session_id()) ::
              {:ok, [Turn.t()]} | {:error, term()}

  @callback append_event(owner(), Event.t()) :: :ok | {:error, term()}

  @callback events(owner(), session_id(), event_options()) ::
              {:ok, [Event.t()]} | {:error, term()}

  @callback latest_sequence(owner(), session_id()) ::
              {:ok, non_neg_integer() | nil} | {:error, term()}

  @callback save_request(owner(), Request.t()) :: :ok | {:error, term()}

  @callback fetch_request(owner(), session_id(), String.t()) ::
              {:ok, Request.t()} | :not_found | {:error, term()}

  @callback list_requests(owner(), session_id(), request_options()) ::
              {:ok, [Request.t()]} | {:error, term()}
end
