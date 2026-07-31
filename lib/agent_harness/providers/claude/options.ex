defmodule AgentHarness.Providers.Claude.Options do
  @moduledoc false

  alias AgentHarness.SessionConfig

  @default_auth_check_timeout 5_000
  @default_readiness_timeout 10_000
  @internal_options [
    :auth,
    :auth_check_timeout,
    :client,
    :question_timeout,
    :readiness_timeout
  ]
  @subscription_auth_env ~w(
    ANTHROPIC_API_KEY
    ANTHROPIC_AUTH_TOKEN
    ANTHROPIC_BASE_URL
    ANTHROPIC_CUSTOM_HEADERS
    ANTHROPIC_BEDROCK_BASE_URL
    ANTHROPIC_BEDROCK_MANTLE_BASE_URL
    ANTHROPIC_VERTEX_BASE_URL
    ANTHROPIC_AWS_API_KEY
    ANTHROPIC_FOUNDRY_API_KEY
    ANTHROPIC_FOUNDRY_AUTH_TOKEN
    ANTHROPIC_FOUNDRY_BASE_URL
    ANTHROPIC_FOUNDRY_RESOURCE
    ANTHROPIC_AWS_BASE_URL
    ANTHROPIC_AWS_WORKSPACE_ID
    AWS_BEARER_TOKEN_BEDROCK
    CLAUDE_CODE_OAUTH_TOKEN
    CLAUDE_CODE_OAUTH_REFRESH_TOKEN
    CLAUDE_CODE_HOST_AUTH_ENV_VAR
    CLAUDE_CODE_HOST_CREDS_FILE
    CLAUDE_CODE_PROVIDER_MANAGED_BY_HOST
    CLAUDE_CODE_USE_BEDROCK
    CLAUDE_CODE_USE_MANTLE
    CLAUDE_CODE_USE_VERTEX
    CLAUDE_CODE_USE_FOUNDRY
    CLAUDE_CODE_USE_ANTHROPIC_AWS
  )
  @type prepared :: %{
          auth: :subscription | :inherit,
          auth_check_timeout: pos_integer(),
          client: module(),
          client_options: keyword(),
          cleanup_paths: [String.t()],
          question_timeout: timeout(),
          readiness_timeout: pos_integer()
        }

  @spec prepare(SessionConfig.t()) :: {:ok, prepared()} | {:error, term()}
  def prepare(%SessionConfig{} = config) do
    with {:ok, provider_options} <- normalize_provider_options(config.provider_options),
         client = Map.get(provider_options, :client, AgentHarness.Providers.Claude.Client.Default),
         auth = Map.get(provider_options, :auth, :subscription),
         auth_check_timeout =
           Map.get(provider_options, :auth_check_timeout, @default_auth_check_timeout),
         question_timeout = Map.get(provider_options, :question_timeout, :infinity),
         readiness_timeout =
           Map.get(provider_options, :readiness_timeout, @default_readiness_timeout),
         :ok <- validate_auth(auth),
         :ok <- validate_subscription_auth(auth, provider_options),
         :ok <- validate_auth_check_timeout(auth_check_timeout),
         :ok <- validate_timeout(question_timeout),
         :ok <- validate_readiness_timeout(readiness_timeout),
         {:ok, plugins, cleanup_paths} <- prepare_skills(config) do
      provider_options =
        provider_options
        |> Map.drop(@internal_options)
        |> Enum.to_list()

      case prepare_client_options(config, provider_options, plugins, auth) do
        {:ok, options} ->
          {:ok,
           %{
             auth: auth,
             auth_check_timeout: auth_check_timeout,
             client: client,
             client_options: options,
             cleanup_paths: cleanup_paths,
             question_timeout: question_timeout,
             readiness_timeout: readiness_timeout
           }}

        {:error, reason} ->
          cleanup(cleanup_paths)
          {:error, reason}
      end
    end
  end

  defp normalize_provider_options(options) when is_list(options) do
    normalize_provider_options(Map.new(options))
  end

  defp normalize_provider_options(options) when is_map(options) do
    Enum.reduce_while(options, {:ok, %{}}, fn
      {key, value}, {:ok, normalized} when is_atom(key) ->
        {:cont, {:ok, Map.put(normalized, key, value)}}

      {key, value}, {:ok, normalized} when is_binary(key) ->
        case existing_atom(key) do
          {:ok, atom} -> {:cont, {:ok, Map.put(normalized, atom, value)}}
          :error -> {:halt, {:error, {:unknown_provider_option, key}}}
        end

      {key, _value}, _acc ->
        {:halt, {:error, {:invalid_provider_option, key}}}
    end)
  end

  defp normalize_provider_options(options), do: {:error, {:invalid_provider_options, options}}

  defp existing_atom(value) do
    {:ok, String.to_existing_atom(value)}
  rescue
    ArgumentError -> :error
  end

  defp validate_auth(auth) when auth in [:subscription, :inherit], do: :ok
  defp validate_auth(auth), do: {:error, {:invalid_auth_mode, auth}}

  defp validate_auth_check_timeout(timeout) when is_integer(timeout) and timeout > 0, do: :ok

  defp validate_auth_check_timeout(timeout),
    do: {:error, {:invalid_auth_check_timeout, timeout}}

  defp validate_subscription_auth(:inherit, _provider_options), do: :ok

  defp validate_subscription_auth(:subscription, provider_options) do
    cond do
      Map.has_key?(provider_options, :api_key) ->
        {:error, {:subscription_auth_conflict, :provider_api_key}}

      match?({:ok, _value}, Application.fetch_env(:claude_code, :api_key)) ->
        {:error, {:subscription_auth_conflict, :claude_code_api_key}}

      auth_override = subscription_auth_override(provider_options) ->
        {:error, {:subscription_auth_conflict, auth_override}}

      true ->
        :ok
    end
  end

  defp subscription_auth_override(provider_options) do
    Enum.find_value(
      [
        adapter: nil,
        settings: %{},
        setting_sources: [],
        extra_args: %{}
      ],
      fn {key, safe_value} ->
        case Map.fetch(provider_options, key) do
          {:ok, ^safe_value} -> nil
          {:ok, _value} -> {:provider_option, key}
          :error -> nil
        end
      end
    )
  end

  defp validate_timeout(:infinity), do: :ok
  defp validate_timeout(timeout) when is_integer(timeout) and timeout > 0, do: :ok
  defp validate_timeout(timeout), do: {:error, {:invalid_question_timeout, timeout}}

  defp validate_readiness_timeout(timeout) when is_integer(timeout) and timeout > 0, do: :ok

  defp validate_readiness_timeout(timeout),
    do: {:error, {:invalid_readiness_timeout, timeout}}

  defp environment(env, auth) do
    case auth do
      :subscription -> unset_subscription_auth(env)
      :inherit -> env
    end
  end

  defp merge_environment(options, config_env, auth) do
    provider_env = Keyword.get(options, :env, %{})

    merged =
      config_env
      |> normalize_environment()
      |> Map.merge(normalize_environment(provider_env))

    Keyword.put(options, :env, environment(merged, auth))
  end

  defp enforce_auth_environment(options, :inherit), do: options

  defp enforce_auth_environment(options, :subscription) do
    env =
      options
      |> Keyword.get(:env, %{})
      |> normalize_environment()
      |> unset_subscription_auth()

    Keyword.put(options, :env, env)
  end

  defp enforce_auth_options(options, :inherit), do: options

  defp enforce_auth_options(options, :subscription) do
    options
    |> Keyword.put(:adapter, {ClaudeCode.Adapter.Port, []})
    |> Keyword.put(:settings, %{})
    |> Keyword.put(:setting_sources, [])
    |> Keyword.put(:extra_args, %{})
  end

  defp normalize_environment(env) do
    Map.new(env, fn {key, value} -> {to_string(key), value} end)
  end

  defp unset_subscription_auth(env) do
    Enum.reduce(@subscription_auth_env, env, &Map.put(&2, &1, false))
  end

  defp prepare_skills(%SessionConfig{skills: []}), do: {:ok, [], []}

  defp prepare_skills(%SessionConfig{} = config) do
    Enum.reduce_while(config.skills, {:ok, [], [], []}, fn skill,
                                                           {:ok, plugins, sources, cleanup} ->
      case classify_skill(skill) do
        {:plugin, plugin_path} ->
          {:cont, {:ok, [plugin_path | plugins], sources, cleanup}}

        {:skill, skill_path, name} ->
          {:cont, {:ok, plugins, [{skill_path, name} | sources], cleanup}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, plugins, [], cleanup} ->
        {:ok, Enum.reverse(plugins), cleanup}

      {:ok, plugins, sources, cleanup} ->
        case build_skill_plugin(config.session_id, Enum.reverse(sources)) do
          {:ok, generated_plugin} ->
            {:ok, Enum.reverse(plugins) ++ [generated_plugin], [generated_plugin | cleanup]}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp classify_skill(path) when is_binary(path), do: classify_path(path, nil)

  defp classify_skill(%{path: path} = skill) when is_binary(path) do
    classify_path(path, Map.get(skill, :name))
  end

  defp classify_skill(%{"path" => path} = skill) when is_binary(path) do
    classify_path(path, Map.get(skill, "name"))
  end

  defp classify_skill(skill), do: {:error, {:invalid_skill, skill}}

  defp classify_path(path, configured_name) do
    path = Path.expand(path)

    cond do
      plugin_root?(path) ->
        {:plugin, path}

      File.regular?(path) and Path.basename(path) == "SKILL.md" ->
        source = Path.dirname(path)
        {:skill, source, configured_name || Path.basename(source)}

      File.dir?(path) and File.regular?(Path.join(path, "SKILL.md")) ->
        {:skill, path, configured_name || Path.basename(path)}

      true ->
        {:error, {:invalid_skill_path, path}}
    end
  end

  defp plugin_root?(path) do
    File.dir?(path) and File.regular?(Path.join([path, ".claude-plugin", "plugin.json"]))
  end

  defp build_skill_plugin(session_id, skills) do
    root =
      Path.join(
        System.tmp_dir!(),
        "agent-harness-claude-#{safe_name(session_id)}-#{System.unique_integer([:positive])}"
      )

    with :ok <- File.mkdir_p(Path.join(root, ".claude-plugin")),
         :ok <- File.mkdir_p(Path.join(root, "skills")),
         :ok <- write_manifest(root, session_id),
         :ok <- copy_skills(root, skills) do
      {:ok, root}
    else
      {:error, reason} ->
        File.rm_rf(root)
        {:error, {:skill_plugin_failed, reason}}
    end
  end

  defp write_manifest(root, session_id) do
    manifest = %{
      "name" => "agent-harness-#{safe_name(session_id)}",
      "version" => "0.1.0",
      "description" => "Session-scoped skills generated by AgentHarness"
    }

    File.write(
      Path.join([root, ".claude-plugin", "plugin.json"]),
      JSON.encode!(manifest)
    )
  end

  defp copy_skills(root, skills) do
    Enum.reduce_while(skills, :ok, fn {source, name}, :ok ->
      destination = Path.join([root, "skills", safe_name(name)])

      case File.cp_r(source, destination) do
        {:ok, _files} -> {:cont, :ok}
        {:error, reason, _file} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp safe_name(value) do
    value
    |> to_string()
    |> String.replace(~r/[^a-zA-Z0-9_-]+/u, "-")
    |> String.trim("-")
    |> case do
      "" -> "skill"
      name -> name
    end
  end

  defp merge_plugins(options, []), do: options

  defp merge_plugins(options, plugins) do
    existing = Keyword.get(options, :plugins, [])
    Keyword.put(options, :plugins, existing ++ plugins)
  end

  defp enable_skills(options, []), do: options

  defp enable_skills(options, _plugins) do
    allowed_tools =
      options
      |> Keyword.get(:allowed_tools, [])
      |> List.wrap()
      |> then(fn tools -> if "Skill" in tools, do: tools, else: tools ++ ["Skill"] end)

    options
    |> Keyword.put(:allowed_tools, allowed_tools)
  end

  defp put(options, key, value), do: Keyword.put(options, key, value)
  defp put_present(options, _key, nil), do: options
  defp put_present(options, key, value), do: Keyword.put(options, key, value)

  defp prepare_client_options(config, provider_options, plugins, auth) do
    options =
      []
      |> put(:cli_path, :global)
      |> put_present(:cwd, config.cwd)
      |> put_present(:model, config.model)
      |> put_present(:system_prompt, config.system_prompt)
      |> put_present(:permission_mode, config.approval_policy)
      |> put_present(:sandbox, config.sandbox)
      |> put(:mcp_servers, config.mcp_servers)
      |> put(:strict_mcp_config, true)
      |> put(:include_partial_messages, true)
      |> Keyword.merge(provider_options)
      |> merge_environment(config.env, auth)
      |> enforce_auth_environment(auth)
      |> enforce_auth_options(auth)
      |> merge_plugins(plugins)
      |> enable_skills(plugins)

    {:ok, options}
  rescue
    error ->
      {:error, {:provider_option_preparation_failed, error.__struct__, Exception.message(error)}}
  catch
    kind, reason ->
      {:error, {:provider_option_preparation_failed, {kind, reason}}}
  end

  defp cleanup(paths), do: Enum.each(paths, &File.rm_rf/1)
end
