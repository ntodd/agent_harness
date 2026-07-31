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
    event_buffer_size: 1_000,
    startup_timeout: 30_000,
    turn_start_timeout: 30_000,
    store_failure: :degrade,
    store: {AgentHarness.Store.Memory, AgentHarness.Store.Memory}
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
          event_buffer_size: pos_integer(),
          startup_timeout: pos_integer(),
          turn_start_timeout: pos_integer(),
          store_failure: :degrade | :stop,
          store: false | {module(), term()}
        }

  @spec new(SessionRef.t(), keyword()) :: t()
  def new(%SessionRef{} = session, opts \\ []) when is_list(opts) do
    event_buffer_size = Keyword.get(opts, :event_buffer_size, 1_000)
    startup_timeout = Keyword.get(opts, :startup_timeout, 30_000)
    turn_start_timeout = Keyword.get(opts, :turn_start_timeout, 30_000)
    store_failure = Keyword.get(opts, :store_failure, :degrade)

    unless is_integer(event_buffer_size) and event_buffer_size > 0 do
      raise ArgumentError, "event_buffer_size must be a positive integer"
    end

    validate_timeout!(startup_timeout, :startup_timeout)
    validate_timeout!(turn_start_timeout, :turn_start_timeout)
    validate_store_failure!(store_failure)

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
      event_buffer_size: event_buffer_size,
      startup_timeout: startup_timeout,
      turn_start_timeout: turn_start_timeout,
      store_failure: store_failure,
      store:
        Keyword.get(
          opts,
          :store,
          {AgentHarness.Store.Memory, AgentHarness.Store.Memory}
        )
    }
  end

  defp validate_timeout!(timeout, _name) when is_integer(timeout) and timeout > 0, do: :ok

  defp validate_timeout!(timeout, name) do
    raise ArgumentError, "#{name} must be a positive integer, got: #{inspect(timeout)}"
  end

  defp validate_store_failure!(policy) when policy in [:degrade, :stop], do: :ok

  defp validate_store_failure!(policy) do
    raise ArgumentError, "store_failure must be :degrade or :stop, got: #{inspect(policy)}"
  end
end
