defmodule AgentHarness.SessionRef do
  @moduledoc """
  Stable public reference to a supervised logical agent session.

  References deliberately contain no process identifier so a session process
  may be restarted or passivated without invalidating callers' handles.
  """

  alias AgentHarness.ID

  @enforce_keys [:id, :provider]
  defstruct [:id, :provider]

  @type t :: %__MODULE__{id: String.t(), provider: atom()}

  @doc false
  @spec new(atom(), keyword()) :: t()
  def new(provider, opts \\ []) when is_atom(provider) and is_list(opts) do
    %__MODULE__{
      id: Keyword.get_lazy(opts, :id, &ID.generate/0),
      provider: provider
    }
  end
end
