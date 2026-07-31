defmodule AgentHarness.Subscription do
  @moduledoc """
  Handle for an event subscription.

  The SessionServer monitors the subscriber so it can remove dead consumers.
  That direction does not notify the consumer if the session dies; use
  `AgentHarness.monitor/1` for that lifecycle signal.
  """

  @enforce_keys [:ref, :session_id, :pid, :server]
  defstruct [:ref, :session_id, :turn_id, :pid, :server]

  @type t :: %__MODULE__{
          ref: reference(),
          session_id: String.t(),
          turn_id: String.t() | nil,
          pid: pid(),
          server: pid()
        }
end
