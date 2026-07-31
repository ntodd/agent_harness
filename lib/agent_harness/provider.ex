defmodule AgentHarness.Provider do
  @moduledoc """
  Behaviour implemented by coding-agent provider adapters.

  `open_session/2` runs in a bounded startup task. Other provider callbacks
  acknowledge commands quickly. Long-running I/O belongs in provider-owned
  processes which publish normalized messages through
  `AgentHarness.Provider.Sink`.

  A PID handle is monitored automatically. For an opaque handle, return a
  runtime PID as `session_info.monitor`; otherwise the adapter must report
  transport loss with `AgentHarness.Provider.Sink.transport_down/2`. A runtime
  tied to the logical session should monitor `sink.pid`, not the temporary
  process executing `open_session/2`.

  If `start_turn/4` cannot determine whether upstream work began, return
  `{:error, {:turn_start_uncertain, reason}}`. AgentHarness records a terminal
  failure and retires that provider session so potentially running work cannot
  be mistaken for a reusable idle conversation.
  """

  alias AgentHarness.{Capabilities, Response, SessionConfig, Turn}
  alias AgentHarness.Provider.Sink

  @type handle :: term()
  @type provider_turn_ref :: term()
  @type session_info :: %{
          optional(:provider_session_id) => String.t() | nil,
          optional(:monitor) => pid(),
          optional(atom()) => term()
        }

  @callback open_session(SessionConfig.t(), Sink.t()) ::
              {:ok, handle(), session_info()} | {:error, term()}

  @callback start_turn(handle(), Turn.t(), input :: term(), keyword()) ::
              {:ok, provider_turn_ref()} | {:error, term()}

  @callback respond(handle(), provider_request_ref :: term(), Response.t()) ::
              :ok | {:error, term()}

  @callback cancel(handle(), provider_turn_ref()) :: :ok | {:error, term()}
  @callback close_session(handle()) :: :ok
  @callback capabilities(handle()) :: Capabilities.t()
end
