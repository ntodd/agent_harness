defmodule AgentHarness.Request do
  @moduledoc """
  A structured question, permission, confirmation, or MCP elicitation.
  """

  alias AgentHarness.ID

  @type kind :: :question | :permission | :confirmation | :mcp_elicitation
  @type status :: :pending | :resolved

  @enforce_keys [
    :id,
    :session_id,
    :turn_id,
    :kind,
    :prompt,
    :choices,
    :provider_ref,
    :status
  ]
  defstruct [
    :id,
    :session_id,
    :turn_id,
    :kind,
    :prompt,
    :choices,
    :provider_ref,
    :status,
    :deadline,
    :metadata,
    :response
  ]

  @type t :: %__MODULE__{
          id: String.t(),
          session_id: String.t(),
          turn_id: String.t(),
          kind: kind(),
          prompt: String.t(),
          choices: [map()],
          provider_ref: term(),
          status: status(),
          deadline: DateTime.t() | nil,
          metadata: map() | nil,
          response: term()
        }

  @doc false
  @spec new(keyword()) :: t()
  def new(opts) when is_list(opts) do
    %__MODULE__{
      id: Keyword.get_lazy(opts, :id, &ID.generate/0),
      session_id: Keyword.fetch!(opts, :session_id),
      turn_id: Keyword.fetch!(opts, :turn_id),
      kind: Keyword.fetch!(opts, :kind),
      prompt: Keyword.fetch!(opts, :prompt),
      choices: Keyword.get(opts, :choices, []),
      provider_ref: Keyword.fetch!(opts, :provider_ref),
      status: :pending,
      deadline: Keyword.get(opts, :deadline),
      metadata: Keyword.get(opts, :metadata)
    }
  end
end
