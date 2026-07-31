defmodule AgentHarness.SessionDownError do
  @moduledoc """
  Raised when an event stream loses its SessionServer before a terminal event.
  """

  defexception [:reason]

  @impl true
  def message(%__MODULE__{reason: reason}) do
    "agent session stopped before the turn completed: #{inspect(reason)}"
  end
end
