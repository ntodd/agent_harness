defmodule AgentHarness.Provider do
  @moduledoc """
  Behaviour implemented by coding-agent provider adapters.

  Provider callbacks acknowledge commands quickly. Long-running I/O belongs in
  provider-owned processes which publish normalized messages through
  `AgentHarness.Provider.Sink`.
  """

  alias AgentHarness.{Capabilities, Response, SessionConfig, Turn}
  alias AgentHarness.Provider.Sink

  @type handle :: term()
  @type provider_turn_ref :: term()
  @type session_info :: %{
          optional(:provider_session_id) => String.t() | nil,
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
