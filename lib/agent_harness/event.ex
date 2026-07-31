defmodule AgentHarness.Event do
  @moduledoc """
  A normalized event emitted during an agent session.

  Provider adapters normalize lifecycle events into `type` and `data`, while
  `raw` retains the provider value delivered to the adapter for features that
  are not part of the common API. An upstream SDK can reject a wire message
  before AgentHarness receives it, so this is not a byte-for-byte transport log.
  """

  alias AgentHarness.ID

  @schema_version 1

  @enforce_keys [
    :schema_version,
    :id,
    :seq,
    :session_id,
    :provider,
    :type,
    :at,
    :data
  ]
  defstruct [
    :schema_version,
    :id,
    :seq,
    :session_id,
    :turn_id,
    :provider,
    :type,
    :at,
    :data,
    :raw
  ]

  @type t :: %__MODULE__{
          schema_version: pos_integer(),
          id: String.t(),
          seq: non_neg_integer(),
          session_id: String.t(),
          turn_id: String.t() | nil,
          provider: atom(),
          type: atom(),
          at: DateTime.t(),
          data: term(),
          raw: term()
        }

  @doc false
  @spec new(keyword()) :: t()
  def new(opts) when is_list(opts) do
    %__MODULE__{
      schema_version: @schema_version,
      id: Keyword.get_lazy(opts, :id, &ID.generate/0),
      seq: fetch!(opts, :seq),
      session_id: fetch!(opts, :session_id),
      turn_id: Keyword.get(opts, :turn_id),
      provider: fetch!(opts, :provider),
      type: fetch!(opts, :type),
      at: Keyword.get_lazy(opts, :at, &DateTime.utc_now/0),
      data: Keyword.get(opts, :data, %{}),
      raw: Keyword.get(opts, :raw)
    }
  end

  defp fetch!(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} -> value
      :error -> raise ArgumentError, "missing required event option #{inspect(key)}"
    end
  end
end
