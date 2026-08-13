defmodule AgentHarness.SessionConfig do
  @moduledoc """
  Immutable provider configuration belonging to one logical session.

  `startup_timeout` bounds Store reconciliation and provider opening;
  `startup_finalization_timeout` separately bounds initial persistence,
  rollback, cleanup, and readiness acknowledgement. `turn_start_timeout`
  bounds asynchronous provider admission. `provider_command_timeout` bounds
  response and cancellation callbacks and must be shorter than the application
  `:provider_command_call_timeout` used by the public response call.

  `env`, `provider_options`, and `mcp_servers` can carry credentials (API
  keys, auth headers), so the `Inspect` implementation redacts them: each
  keeps its top-level keys with `"[REDACTED]"` values, which shows what was
  configured without exposing it. Values nested below the top level are
  redacted along with everything else. Code that renders a config through
  means that bypass the Inspect protocol (for example
  `inspect(config, structs: false)` or Erlang `~p` formatting of the raw
  term) still exposes the original values.

  Supervisor progress and child-start reports carry the full start arguments
  of a session process, config included, and none of the redaction here
  applies to them. Elixir's logger drops those SASL reports by default;
  leave `handle_sasl_reports` off in any environment whose logs are shipped
  or retained.
  """

  alias AgentHarness.Internal.Redaction
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
    completed_turn_cache_size: 1_000,
    startup_timeout: 30_000,
    startup_finalization_timeout: 5_000,
    turn_start_timeout: 30_000,
    provider_command_timeout: 30_000,
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
          completed_turn_cache_size: non_neg_integer() | :infinity,
          startup_timeout: pos_integer(),
          startup_finalization_timeout: pos_integer(),
          turn_start_timeout: pos_integer(),
          provider_command_timeout: pos_integer(),
          store_failure: :degrade | :stop,
          store: false | {module(), term()}
        }

  @spec new(SessionRef.t(), keyword()) :: t()
  def new(%SessionRef{} = session, opts \\ []) when is_list(opts) do
    event_buffer_size = Keyword.get(opts, :event_buffer_size, 1_000)

    completed_turn_cache_size =
      Keyword.get(opts, :completed_turn_cache_size, event_buffer_size)

    startup_timeout = Keyword.get(opts, :startup_timeout, 30_000)

    startup_finalization_timeout = Keyword.get(opts, :startup_finalization_timeout, 5_000)

    turn_start_timeout = Keyword.get(opts, :turn_start_timeout, 30_000)
    provider_command_timeout = Keyword.get(opts, :provider_command_timeout, 30_000)
    store_failure = Keyword.get(opts, :store_failure, :degrade)

    unless is_integer(event_buffer_size) and event_buffer_size > 0 do
      raise ArgumentError, "event_buffer_size must be a positive integer"
    end

    validate_completed_turn_cache_size!(completed_turn_cache_size)

    validate_timeout!(startup_timeout, :startup_timeout)
    validate_timeout!(startup_finalization_timeout, :startup_finalization_timeout)
    validate_timeout!(turn_start_timeout, :turn_start_timeout)
    validate_timeout!(provider_command_timeout, :provider_command_timeout)
    validate_provider_command_budget!(provider_command_timeout)
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
      completed_turn_cache_size: completed_turn_cache_size,
      startup_timeout: startup_timeout,
      startup_finalization_timeout: startup_finalization_timeout,
      turn_start_timeout: turn_start_timeout,
      provider_command_timeout: provider_command_timeout,
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

  defp validate_provider_command_budget!(timeout) do
    outer_timeout =
      Application.get_env(:agent_harness, :provider_command_call_timeout, 60_000)

    validate_timeout!(outer_timeout, :provider_command_call_timeout)

    unless timeout < outer_timeout do
      raise ArgumentError,
            "provider_command_timeout must be less than " <>
              "provider_command_call_timeout (#{outer_timeout}), got: #{timeout}"
    end
  end

  defp validate_completed_turn_cache_size!(:infinity), do: :ok

  defp validate_completed_turn_cache_size!(size)
       when is_integer(size) and size >= 0,
       do: :ok

  defp validate_completed_turn_cache_size!(_size) do
    raise ArgumentError,
          "completed_turn_cache_size must be a non-negative integer or :infinity"
  end

  defp validate_store_failure!(policy) when policy in [:degrade, :stop], do: :ok

  defp validate_store_failure!(policy) do
    raise ArgumentError, "store_failure must be :degrade or :stop, got: #{inspect(policy)}"
  end

  # Returns a display-safe copy: env, provider_options, and mcp_servers keep
  # their keys with every value replaced, so credentials cannot reach logs.
  # The result is for formatting only.
  @doc false
  def redact(%__MODULE__{} = config) do
    %{
      config
      | env: Redaction.redact_values(config.env),
        provider_options: Redaction.redact_values(config.provider_options),
        mcp_servers: Redaction.redact_values(config.mcp_servers)
    }
  end
end

defimpl Inspect, for: AgentHarness.SessionConfig do
  def inspect(config, opts) do
    config
    |> AgentHarness.SessionConfig.redact()
    |> Inspect.Any.inspect(opts)
  end
end
