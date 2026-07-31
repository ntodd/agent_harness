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
    unless Keyword.keyword?(opts) do
      raise ArgumentError, "approval options must be a keyword list"
    end

    scope = Keyword.get(opts, :scope, :once)

    unless scope in [:once, :session, nil] do
      raise ArgumentError, "approval scope must be :once, :session, or nil"
    end

    %__MODULE__{action: :approve, scope: scope}
  end

  @spec deny(String.t() | nil) :: t()
  def deny(reason \\ nil), do: %__MODULE__{action: :deny, reason: reason}

  @spec cancel(String.t() | nil) :: t()
  def cancel(reason \\ nil), do: %__MODULE__{action: :cancel, reason: reason}

  @doc false
  @spec validate(t()) :: :ok | {:error, term()}
  def validate(%__MODULE__{action: :approve, scope: scope})
      when scope in [:once, :session, nil],
      do: :ok

  def validate(%__MODULE__{action: action, scope: nil})
      when action in [:answer, :deny, :cancel],
      do: :ok

  def validate(%__MODULE__{} = response), do: {:error, {:invalid_response, response}}
end
