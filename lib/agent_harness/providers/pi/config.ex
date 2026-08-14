defmodule AgentHarness.Providers.Pi.Config do
  @moduledoc false

  alias AgentHarness.SessionConfig

  @default_executable "pi"

  @known_provider_options [
    :agent_dir,
    :api_key,
    :auth,
    :client,
    :exclude_tools,
    :exec,
    :executable,
    :extensions,
    :fork,
    :name,
    :no_tools,
    :offline,
    :provider,
    :resume,
    :session,
    :session_dir,
    :thinking,
    :tools
  ]

  # Credential routes that bypass a pi `/login` subscription. Anything matching
  # these is refused under the default `auth: :subscription` policy so a session
  # cannot silently bill an API account.
  @credential_env_vars ~w(
    ANTHROPIC_AUTH_TOKEN
    ANTHROPIC_OAUTH_TOKEN
    AWS_ACCESS_KEY_ID
    AWS_BEARER_TOKEN_BEDROCK
    AWS_PROFILE
    AWS_SECRET_ACCESS_KEY
    AZURE_OPENAI_BASE_URL
    AZURE_OPENAI_RESOURCE_NAME
    CLOUDFLARE_ACCOUNT_ID
    CLOUDFLARE_GATEWAY_ID
  )

  @type prepared :: %{
          client: module(),
          exec: {module(), keyword()},
          executable: String.t(),
          args: [String.t()],
          env: [{charlist(), charlist()}],
          cwd: String.t() | nil,
          auth: :subscription | :inherit,
          provider: String.t() | nil,
          model: String.t() | nil,
          provider_session_id: String.t() | nil,
          startup_timeout: pos_integer()
        }

  @spec prepare(SessionConfig.t()) :: {:ok, prepared()} | {:error, term()}
  def prepare(%SessionConfig{} = config) do
    options = normalize_options(config.provider_options)

    with :ok <- validate_known_options(options),
         {:ok, auth} <- normalize_auth(Map.get(options, :auth, :subscription)),
         :ok <- validate_unsupported(config),
         :ok <- validate_session_options(options),
         :ok <- validate_subscription_options(auth, options),
         :ok <- validate_subscription_env(auth, config.env),
         {:ok, exec} <- normalize_exec(Map.get(options, :exec)),
         {:ok, skill_args} <- skill_args(config.skills) do
      {:ok,
       %{
         client: client(options),
         exec: exec,
         executable: Map.get(options, :executable, @default_executable),
         args: args(config, options, skill_args),
         env: env(config, options),
         cwd: config.cwd,
         auth: auth,
         provider: Map.get(options, :provider),
         model: config.model,
         provider_session_id: provider_session_id(config, options),
         startup_timeout: config.startup_timeout
       }}
    end
  end

  # An exec option implies the exec client unless the caller picked one
  # explicitly; without it the local port client remains the default.
  defp client(options) do
    Map.get(options, :client) ||
      if Map.get(options, :exec) do
        AgentHarness.Providers.Pi.Client.Exec
      else
        AgentHarness.Providers.Pi.Client.Port
      end
  end

  defp normalize_exec(nil), do: {:ok, {AgentHarness.Exec.Local, []}}

  defp normalize_exec({module, opts}) when is_atom(module) and is_list(opts),
    do: {:ok, {module, opts}}

  defp normalize_exec(module) when is_atom(module), do: {:ok, {module, []}}
  defp normalize_exec(other), do: {:error, {:invalid_exec, other}}

  @doc """
  Session id pi will report back, or `nil` when pi picks its own.

  Resuming or forking adopts the upstream session's id, and `session: false`
  leaves the run ephemeral, so in both cases the harness cannot predict it.
  """
  @spec provider_session_id(SessionConfig.t(), map()) :: String.t() | nil
  def provider_session_id(%SessionConfig{} = config, options) do
    if assigns_session_id?(options), do: config.session_id
  end

  defp args(config, options, skill_args) do
    [
      ["--mode", "rpc"],
      session_args(config, options),
      flag("--model", config.model),
      flag("--provider", Map.get(options, :provider)),
      flag("--system-prompt", config.system_prompt),
      flag("--name", Map.get(options, :name)),
      flag("--thinking", Map.get(options, :thinking)),
      flag("--api-key", Map.get(options, :api_key)),
      flag("--session-dir", Map.get(options, :session_dir)),
      list_flag("--tools", Map.get(options, :tools)),
      list_flag("--exclude-tools", Map.get(options, :exclude_tools)),
      skill_args,
      extension_args(Map.get(options, :extensions)),
      bare_flag("--no-tools", Map.get(options, :no_tools)),
      bare_flag("--offline", Map.get(options, :offline))
    ]
    |> List.flatten()
  end

  defp session_args(config, options) do
    cond do
      resume = Map.get(options, :resume) -> ["--session", to_string(resume)]
      fork = Map.get(options, :fork) -> ["--fork", to_string(fork)]
      Map.get(options, :session) == false -> ["--no-session"]
      true -> ["--session-id", config.session_id]
    end
  end

  defp assigns_session_id?(options) do
    is_nil(Map.get(options, :resume)) and is_nil(Map.get(options, :fork)) and
      Map.get(options, :session) != false
  end

  defp flag(_name, nil), do: []
  defp flag(name, value), do: [name, to_string(value)]

  defp bare_flag(_name, value) when value in [nil, false], do: []
  defp bare_flag(name, _value), do: [name]

  defp list_flag(_name, nil), do: []
  defp list_flag(_name, []), do: []

  defp list_flag(name, values) when is_list(values),
    do: [name, Enum.map_join(values, ",", &to_string/1)]

  defp list_flag(name, value), do: [name, to_string(value)]

  defp extension_args(nil), do: []

  defp extension_args(paths) when is_list(paths),
    do: Enum.flat_map(paths, &["--extension", to_string(&1)])

  defp extension_args(path), do: ["--extension", to_string(path)]

  defp skill_args(skills) do
    Enum.reduce_while(skills, {:ok, []}, fn skill, {:ok, acc} ->
      case skill_path(skill) do
        {:ok, path} -> {:cont, {:ok, acc ++ ["--skill", path]}}
        :error -> {:halt, {:error, {:invalid_skill, skill}}}
      end
    end)
  end

  defp skill_path(skill) when is_binary(skill), do: {:ok, skill}

  defp skill_path(%{} = skill) do
    case Map.get(skill, :path, Map.get(skill, "path")) do
      path when is_binary(path) -> {:ok, path}
      _other -> :error
    end
  end

  defp skill_path(_skill), do: :error

  defp env(config, options) do
    config.env
    |> Map.new(fn {key, value} -> {to_string(key), to_string(value)} end)
    |> put_agent_dir(Map.get(options, :agent_dir))
    |> Enum.map(fn {key, value} -> {String.to_charlist(key), String.to_charlist(value)} end)
  end

  defp put_agent_dir(env, nil), do: env
  defp put_agent_dir(env, dir), do: Map.put(env, "PI_CODING_AGENT_DIR", to_string(dir))

  defp normalize_options(options) when is_map(options) do
    Map.new(options, fn
      {key, value} when is_binary(key) -> {safe_atom(key), value}
      {key, value} -> {key, value}
    end)
  end

  defp normalize_options(_options), do: %{}

  defp safe_atom(key) do
    String.to_existing_atom(key)
  rescue
    ArgumentError -> :__unknown__
  end

  defp validate_known_options(options) do
    case Map.keys(options) -- @known_provider_options do
      [] -> :ok
      unknown -> {:error, {:unknown_provider_options, Enum.sort(unknown)}}
    end
  end

  defp normalize_auth(auth) when auth in [:subscription, :inherit], do: {:ok, auth}
  defp normalize_auth(auth), do: {:error, {:invalid_auth_mode, auth}}

  defp validate_unsupported(%SessionConfig{} = config) do
    cond do
      map_size(config.mcp_servers) > 0 -> {:error, {:unsupported, :per_session_mcp}}
      not is_nil(config.approval_policy) -> {:error, {:unsupported, :approvals}}
      not is_nil(config.sandbox) -> {:error, {:unsupported, :sandbox}}
      true -> :ok
    end
  end

  defp validate_session_options(options) do
    if Map.has_key?(options, :resume) and Map.has_key?(options, :fork) do
      {:error, {:conflicting_session_options, [:resume, :fork]}}
    else
      :ok
    end
  end

  defp validate_subscription_options(:inherit, _options), do: :ok

  defp validate_subscription_options(:subscription, options) do
    cond do
      Map.has_key?(options, :api_key) ->
        {:error, {:subscription_auth_conflict, :api_key}}

      # Subscription auth is verified against local `pi /login` state, which
      # says nothing about the environment an exec would run the CLI in.
      not is_nil(Map.get(options, :exec)) ->
        {:error, {:subscription_auth_conflict, :exec}}

      true ->
        :ok
    end
  end

  defp validate_subscription_env(:inherit, _env), do: :ok

  defp validate_subscription_env(:subscription, env) do
    env
    |> Enum.map(fn {key, _value} -> to_string(key) end)
    |> Enum.sort()
    |> Enum.find(&credential_env_var?/1)
    |> case do
      nil -> :ok
      key -> {:error, {:subscription_auth_conflict, {:env, key}}}
    end
  end

  defp credential_env_var?(key) do
    String.ends_with?(key, "_API_KEY") or key in @credential_env_vars
  end
end
