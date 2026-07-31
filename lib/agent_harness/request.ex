defmodule AgentHarness.Request do
  @moduledoc """
  A structured question, permission, confirmation, or MCP elicitation.
  """

  alias AgentHarness.ID

  @type kind ::
          :question
          | :command_approval
          | :file_change_approval
          | :permission
          | :confirmation
          | :mcp_elicitation

  @type status :: :pending | :resolved | :expired

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
    :questions,
    :provider_ref,
    :status,
    :deadline,
    :schema,
    :metadata,
    :created_at,
    :response
  ]

  @type t :: %__MODULE__{
          id: String.t(),
          session_id: String.t(),
          turn_id: String.t(),
          kind: kind(),
          prompt: String.t() | nil,
          choices: [map()],
          questions: [map()],
          provider_ref: term(),
          status: status(),
          deadline: DateTime.t() | nil,
          schema: map() | nil,
          metadata: map(),
          created_at: DateTime.t(),
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
      prompt: Keyword.get(opts, :prompt),
      choices: Keyword.get(opts, :choices, []),
      questions: Keyword.get(opts, :questions, []),
      provider_ref: Keyword.fetch!(opts, :provider_ref),
      status: :pending,
      deadline: Keyword.get(opts, :deadline),
      schema: Keyword.get(opts, :schema),
      metadata: Keyword.get(opts, :metadata, %{}),
      created_at: Keyword.get_lazy(opts, :created_at, &DateTime.utc_now/0)
    }
  end
end
