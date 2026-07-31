defmodule AgentHarness.Provider do
  @moduledoc """
  Behaviour implemented by coding-agent provider adapters.

  Lifecycle and command callbacks run in bounded, session-owned tasks.
  `capabilities/1` runs directly in the SessionServer and must return without
  blocking. Long-running I/O belongs in provider-owned processes which publish
  normalized messages through `AgentHarness.Provider.Sink`.

  A PID handle is monitored automatically. For an opaque handle, return a
  runtime PID as `session_info.monitor`; otherwise the adapter must report
  transport loss with `AgentHarness.Provider.Sink.transport_down/2`. A runtime
  tied to the logical session should monitor `sink.pid`, not the temporary
  process executing `open_session/2`.

  If `start_turn/4` cannot determine whether upstream work began, return
  `{:error, {:turn_start_uncertain, reason}}`. AgentHarness records a terminal
  failure and retires that provider session so potentially running work cannot
  be mistaken for a reusable idle conversation.

  If `respond/3` or `cancel/2` cannot determine whether the command reached the
  provider, return `{:error, {:provider_command_uncertain, reason}}`. A plain
  `{:error, reason}` from `respond/3` means the response was definitely rejected
  and may be attempted again. Cancellation failures retire the provider session
  because the upstream turn may still be running.
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
  @callback close_session(handle()) :: :ok | {:error, term()}
  @callback capabilities(handle()) :: Capabilities.t()
end
