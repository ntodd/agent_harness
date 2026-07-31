defmodule AgentHarness.SessionConfig do
  @moduledoc """
  Immutable provider configuration belonging to one logical session.
  """

  alias AgentHarness.SessionRef

  @enforce_keys [:session_id, :provider]
  defstruct [
    :session_id,
    :provider,
    :cwd,
    :model,
    :system_prompt,
    :approval_policy,
    :sandbox,
    mcp_servers: %{},
    skills: [],
    env: %{},
    provider_options: %{},
    metadata: %{},
    event_buffer_size: 1_000
  ]

  @type t :: %__MODULE__{
          session_id: String.t(),
          provider: atom(),
          cwd: String.t() | nil,
          model: String.t() | nil,
          system_prompt: String.t() | nil,
          approval_policy: term(),
          sandbox: term(),
          mcp_servers: map(),
          skills: [map() | String.t()],
          env: map(),
          provider_options: map(),
          metadata: map(),
          event_buffer_size: pos_integer()
        }

  @spec new(SessionRef.t(), keyword()) :: t()
  def new(%SessionRef{} = session, opts \\ []) when is_list(opts) do
    event_buffer_size = Keyword.get(opts, :event_buffer_size, 1_000)

    unless is_integer(event_buffer_size) and event_buffer_size > 0 do
      raise ArgumentError, "event_buffer_size must be a positive integer"
    end

    %__MODULE__{
      session_id: session.id,
      provider: session.provider,
      cwd: Keyword.get(opts, :cwd),
      model: Keyword.get(opts, :model),
      system_prompt: Keyword.get(opts, :system_prompt),
      approval_policy: Keyword.get(opts, :approval_policy),
      sandbox: Keyword.get(opts, :sandbox),
      mcp_servers: Keyword.get(opts, :mcp_servers, %{}),
      skills: Keyword.get(opts, :skills, []),
      env: Keyword.get(opts, :env, %{}),
      provider_options: Keyword.get(opts, :provider_options, %{}),
      metadata: Keyword.get(opts, :metadata, %{}),
      event_buffer_size: event_buffer_size
    }
  end
end
