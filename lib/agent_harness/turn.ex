defmodule AgentHarness.Turn do
  @moduledoc """
  One unit of work within a logical session.
  """

  alias AgentHarness.ID

  @type status ::
          :queued
          | :starting
          | :running
          | :awaiting_input
          | :cancelling
          | :completed
          | :failed
          | :cancelled
          | :interrupted

  @enforce_keys [:id, :session_id, :input, :status, :metadata]
  defstruct [:id, :session_id, :input, :status, :metadata, :started_at, :finished_at, :result]

  @type t :: %__MODULE__{
          id: String.t(),
          session_id: String.t(),
          input: term(),
          status: status(),
          metadata: map(),
          started_at: DateTime.t() | nil,
          finished_at: DateTime.t() | nil,
          result: term()
        }

  @doc false
  @spec new(String.t(), term(), keyword()) :: t()
  def new(session_id, input, opts \\ []) when is_binary(session_id) and is_list(opts) do
    %__MODULE__{
      id: Keyword.get_lazy(opts, :id, &ID.generate/0),
      session_id: session_id,
      input: input,
      status: :queued,
      metadata: Keyword.get(opts, :metadata, %{})
    }
  end
end
