defmodule AgentHarness.Response do
  @moduledoc """
  A provider-neutral response to a structured agent request.
  """

  @type action :: :answer | :approve | :deny | :cancel
  @type scope :: :once | :session | nil

  @enforce_keys [:action]
  defstruct [:action, :value, :scope, :reason]

  @type t :: %__MODULE__{
          action: action(),
          value: term(),
          scope: scope(),
          reason: String.t() | nil
        }

  @spec answer(term()) :: t()
  def answer(value), do: %__MODULE__{action: :answer, value: value}

  @spec approve(keyword()) :: t()
  def approve(opts \\ []) do
    %__MODULE__{action: :approve, scope: Keyword.get(opts, :scope, :once)}
  end

  @spec deny(String.t() | nil) :: t()
  def deny(reason \\ nil), do: %__MODULE__{action: :deny, reason: reason}

  @spec cancel(String.t() | nil) :: t()
  def cancel(reason \\ nil), do: %__MODULE__{action: :cancel, reason: reason}
end
