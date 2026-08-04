defmodule AgentHarness.SessionRef do
  @moduledoc """
  Stable public reference to a supervised logical agent session.

  References deliberately contain no process identifier, keeping callers
  decoupled from process ownership and leaving room for restart or passivation
  without changing the handle shape. AgentHarness v0.x does not automatically
  restart, rehydrate, or passivate sessions.
  """

  alias AgentHarness.ID

  @enforce_keys [:id, :provider]
  defstruct [:id, :provider]

  @type t :: %__MODULE__{id: String.t(), provider: atom()}

  @doc false
  @spec new(atom(), keyword()) :: t()
  def new(provider, opts \\ []) when is_atom(provider) and is_list(opts) do
    id = Keyword.get_lazy(opts, :id, &ID.generate/0)

    unless is_binary(id) and byte_size(id) > 0 do
      raise ArgumentError, "session id must be a non-empty string"
    end

    %__MODULE__{
      id: id,
      provider: provider
    }
  end
end
