defmodule AgentHarness.Capabilities do
  @moduledoc """
  Describes how a provider supports each harness feature.

  Capability levels are deliberately richer than booleans so callers can
  distinguish a stable native feature from an emulation or experimental path.
  """

  @support_levels [:native, :emulated, :experimental, :unsupported]
  @fields [
    :token_streaming,
    :questions,
    :approvals,
    :cancel,
    :steer,
    :resume,
    :fork,
    :per_session_mcp,
    :skills
  ]

  defstruct token_streaming: :unsupported,
            questions: :unsupported,
            approvals: :unsupported,
            cancel: :unsupported,
            steer: :unsupported,
            resume: :unsupported,
            fork: :unsupported,
            per_session_mcp: :unsupported,
            skills: :unsupported

  @type support :: :native | :emulated | :experimental | :unsupported
  @type t :: %__MODULE__{
          token_streaming: support(),
          questions: support(),
          approvals: support(),
          cancel: support(),
          steer: support(),
          resume: support(),
          fork: support(),
          per_session_mcp: support(),
          skills: support()
        }

  @spec new(keyword()) :: t()
  def new(opts \\ []) when is_list(opts) do
    Enum.reduce(opts, %__MODULE__{}, fn
      {key, value}, capabilities when key in @fields and value in @support_levels ->
        Map.replace!(capabilities, key, value)

      {key, value}, _capabilities ->
        raise ArgumentError,
              "invalid capability #{inspect(key)}: expected one of " <>
                "#{inspect(@support_levels)}, got: #{inspect(value)}"
    end)
  end
end
