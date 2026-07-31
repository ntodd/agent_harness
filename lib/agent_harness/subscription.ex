defmodule AgentHarness.Subscription do
  @moduledoc """
  Handle for a monitored event subscription.
  """

  @enforce_keys [:ref, :session_id, :pid]
  defstruct [:ref, :session_id, :turn_id, :pid]

  @type t :: %__MODULE__{
          ref: reference(),
          session_id: String.t(),
          turn_id: String.t() | nil,
          pid: pid()
        }
end
