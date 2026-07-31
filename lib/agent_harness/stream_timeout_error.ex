defmodule AgentHarness.StreamTimeoutError do
  @moduledoc """
  Raised when a turn stream receives no event within its configured timeout.
  """

  defexception [:timeout]

  @impl true
  def message(%__MODULE__{timeout: timeout}) do
    "agent event stream timed out after #{timeout} milliseconds"
  end
end
