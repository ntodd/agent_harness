defmodule AgentHarness.Providers.Codex.Config do
  @moduledoc false

  alias AgentHarness.Providers.Codex.Client
  alias AgentHarness.SessionConfig
  alias Codex.Auth.Store
  alias Codex.Config.BaseURL

  @connect_key_aliases %{
    "init_timeout_ms" => :init_timeout_ms,
    "client_name" => :client_name,
    "client_title" => :client_title,
    "client_version" => :client_version,
    "experimental_api" => :experimental_api,
    "cwd" => :cwd,
    "process_env" => :process_env,
    "env" => :env
  }

  @thread_key_aliases %{
    "transport" => :transport,
    "working_directory" => :working_directory,
    "model" => :model,
    "developer_instructions" => :developer_instructions,
    "ask_for_approval" => :ask_for_approval,
    "sandbox" => :sandbox,
    "config" => :config,
    "skills_enabled" => :skills_enabled,
    "web_search_enabled" => :web_search_enabled,
    "dynamic_tools" => :dynamic_tools,
    "output_schema" => :output_schema,
    "approvals_reviewer" => :approvals_reviewer,
    "permission_profile" => :permission_profile,
    "sandbox_policy" => :sandbox_policy,
    "model_reasoning_summary" => :model_reasoning_summary,
    "model_verbosity" => :model_verbosity,
    "service_tier" => :service_tier,
    "ephemeral" => :ephemeral,
    "history_persistence" => :history_persistence,
    "history_mode" => :history_mode,
    "experimental_raw_events" => :experimental_raw_events
  }

  @turn_key_aliases %{
    "client_user_message_id" => :client_user_message_id,
    "completion_timeout_ms" => :completion_timeout_ms,
    "cwd" => :cwd,
    "model" => :model,
    "approval_policy" => :approval_policy,
    "approvals_reviewer" => :approvals_reviewer,
    "sandbox_policy" => :sandbox_policy,
    "permission_profile" => :permission_profile,
    "effort" => :effort,
    "summary" => :summary,
    "additional_context" => :additional_context,
    "personality" => :personality,
    "output_schema" => :output_schema,
    "collaboration_mode" => :collaboration_mode,
    "service_tier" => :service_tier,
    "environments" => :environments
  }

  @type prepared :: %{
          client: module(),
          codex_options: map(),
          connect_options: keyword(),
          thread_options: map(),
          turn_options: map(),
          skills: [map()],
          provider_session_id: String.t() | nil,
          auth: :subscription | :inherit
        }

  @spec prepare(SessionConfig.t()) :: {:ok, prepared()} | {:error, term()}
  def prepare(%SessionConfig{} = config) do
    options = config.provider_options || %{}

    with {:ok, auth} <- normalize_auth(fetch(options, :auth, :subscription)),
         {:ok, codex_options} <-
           options |> fetch(:codex_options, %{}) |> normalize_map(:codex_options),
         :ok <- validate_subscription_options(auth, codex_options),
         {:ok, connect_options} <-
           options
           |> fetch(:connect_options, [])
           |> normalize_keyword(:connect_options, @connect_key_aliases),
         {:ok, thread_options} <-
           options
           |> fetch(:thread_options, %{})
           |> normalize_map(:thread_options, @thread_key_aliases),
         {:ok, turn_options} <-
           options
           |> fetch(:turn_options, %{})
           |> normalize_map(:turn_options, @turn_key_aliases),
         {:ok, skills} <- normalize_skills(config.skills),
         {:ok, provider_session_id} <- provider_session_id(options),
         connect_options = build_connect_options(config, connect_options),
         :ok <- validate_subscription_profile(auth, config, connect_options) do
      codex_options =
        case fetch(options, :codex_path, nil) do
          path when is_binary(path) and path != "" ->
            Map.put_new(codex_options, :codex_path_override, path)

          _ ->
            codex_options
        end

      codex_options = enforce_codex_auth(codex_options, auth)
      connect_options = enforce_process_auth(connect_options, auth)

      {:ok,
       %{
         client: fetch(options, :client, Client.SDK),
         codex_options: codex_options,
         connect_options: connect_options,
         thread_options: thread_options,
         turn_options: turn_options,
         skills: skills,
         provider_session_id: provider_session_id,
         auth: auth
       }}
    end
  end

  @spec thread_options(prepared(), SessionConfig.t(), term()) :: map()
  def thread_options(prepared, %SessionConfig{} = config, connection) do
    base =
      %{
        working_directory: config.cwd,
        model: config.model,
        developer_instructions: config.system_prompt,
        ask_for_approval: config.approval_policy,
        sandbox: config.sandbox
      }
      |> compact()

    options =
      base
      |> Map.merge(prepared.thread_options)
      |> put_mcp_servers(config.mcp_servers)
      |> maybe_enable_skills(prepared.skills)

    Map.put(options, :transport, {:app_server, connection})
  end

  @spec turn_options(prepared(), String.t(), keyword() | map()) :: map()
  def turn_options(prepared, turn_id, options) do
    options =
      case normalize_map(options, :turn_options, @turn_key_aliases) do
        {:ok, normalized} -> normalized
        {:error, _reason} -> %{}
      end

    prepared.turn_options
    |> Map.merge(options)
    |> Map.put_new(:client_user_message_id, turn_id)
  end

  @spec input(prepared(), term()) :: {:ok, [map()]} | {:error, term()}
  def input(%{skills: skills}, input) when is_binary(input) do
    {:ok, skills ++ [%{type: :text, text: input}]}
  end

  def input(%{skills: skills}, input) when is_list(input) do
    if Enum.all?(input, &is_map/1) do
      {:ok, skills ++ input}
    else
      {:error, {:invalid_input, input}}
    end
  end

  def input(_prepared, input), do: {:error, {:invalid_input, input}}

  defp build_connect_options(config, options) do
    defaults = [
      client_name: "agent_harness",
      client_title: "AgentHarness",
      client_version: app_version(),
      experimental_api: true
    ]

    options =
      Enum.reduce(defaults, options, fn {key, value}, acc ->
        Keyword.put_new(acc, key, value)
      end)

    options = if config.cwd, do: Keyword.put_new(options, :cwd, config.cwd), else: options
    merge_process_env(options, config.env)
  end

  defp merge_process_env(options, env) when is_map(env) do
    configured =
      options
      |> Keyword.get(:process_env, Keyword.get(options, :env, %{}))
      |> Map.new()
      |> stringify_env()

    merged = Map.merge(configured, stringify_env(env))

    if map_size(merged) > 0 or
         Keyword.has_key?(options, :process_env) or Keyword.has_key?(options, :env) do
      options
      |> Keyword.delete(:env)
      |> Keyword.put(:process_env, merged)
    else
      options
    end
  end

  defp merge_process_env(options, _env), do: options

  defp stringify_env(env) do
    Map.new(env, fn {key, value} -> {to_string(key), value} end)
  end

  defp app_version do
    case Application.spec(:agent_harness, :vsn) do
      nil -> "0.1.0"
      version -> to_string(version)
    end
  end

  defp put_mcp_servers(options, mcp_servers)
       when is_map(mcp_servers) and map_size(mcp_servers) > 0 do
    thread_config = Map.get(options, :config, %{}) || %{}

    existing =
      Map.get(thread_config, "mcp_servers") ||
        Map.get(thread_config, :mcp_servers) ||
        %{}

    merged =
      existing
      |> stringify_keys()
      |> Map.merge(stringify_keys(mcp_servers))

    thread_config =
      thread_config
      |> Map.delete(:mcp_servers)
      |> Map.put("mcp_servers", merged)

    Map.put(options, :config, thread_config)
  end

  defp put_mcp_servers(options, _mcp_servers), do: options

  defp maybe_enable_skills(options, []), do: options
  defp maybe_enable_skills(options, _skills), do: Map.put(options, :skills_enabled, true)

  defp provider_session_id(options) do
    value =
      fetch(options, :provider_session_id, nil) ||
        fetch(options, :thread_id, nil) ||
        fetch(options, :resume, nil)

    case value do
      nil ->
        {:ok, nil}

      value when value in [:last, "last"] ->
        {:error, {:unsupported_provider_session_id, :last}}

      value when is_binary(value) and value != "" ->
        {:ok, value}

      other ->
        {:error, {:invalid_provider_session_id, other}}
    end
  end

  defp normalize_auth(auth) when auth in [:subscription, :inherit], do: {:ok, auth}
  defp normalize_auth(auth), do: {:error, {:invalid_auth_mode, auth}}

  defp validate_subscription_options(:inherit, _codex_options), do: :ok

  defp validate_subscription_options(:subscription, codex_options) do
    if Map.has_key?(codex_options, :api_key) or Map.has_key?(codex_options, "api_key") do
      {:error, {:subscription_auth_conflict, :provider_api_key}}
    else
      :ok
    end
  end

  defp validate_subscription_profile(:inherit, _config, _connect_options), do: :ok

  defp validate_subscription_profile(:subscription, config, connect_options) do
    {codex_home, explicit?} = effective_codex_home(connect_options)
    cwd = Keyword.get(connect_options, :cwd, config.cwd)
    mode = Store.credentials_store_mode(codex_home, cwd)

    cond do
      mode == :keyring ->
        {:error, {:subscription_auth_conflict, {:uninspectable_credentials_store, mode}}}

      mode == :auto and Store.keyring_supported?() ->
        {:error, {:subscription_auth_conflict, {:uninspectable_credentials_store, mode}}}

      true ->
        validate_file_auth_profile(codex_home, explicit?)
    end
  end

  defp validate_file_auth_profile(codex_home, explicit?) do
    case Store.load(codex_home: codex_home, codex_home_explicit?: explicit?) do
      {:ok, %Store.Record{auth_mode: mode}} when mode in [:api_key, :bedrock_api_key] ->
        {:error, {:subscription_auth_conflict, {:stored_auth_mode, mode}}}

      {:ok, _record_or_nil} ->
        :ok

      {:error, reason} ->
        {:error, {:subscription_auth_check_failed, reason}}
    end
  end

  defp effective_codex_home(connect_options) do
    process_env =
      connect_options
      |> Keyword.get(:process_env, Keyword.get(connect_options, :env, %{}))
      |> Map.new()

    case Map.get(process_env, "CODEX_HOME") || Map.get(process_env, :CODEX_HOME) do
      value when is_binary(value) and value != "" ->
        {value, true}

      _ ->
        configured_home = Codex.Env.get("CODEX_HOME")
        {configured_home || Path.join(System.user_home!(), ".codex"), not is_nil(configured_home)}
    end
  end

  defp enforce_codex_auth(codex_options, :inherit), do: codex_options

  defp enforce_codex_auth(codex_options, :subscription) do
    codex_options
    |> Map.drop([
      :api_key,
      "api_key",
      :base_url,
      "base_url",
      :openai_base_url,
      "openai_base_url",
      :openaiBaseUrl,
      "openaiBaseUrl"
    ])
    |> Map.put(:api_key, false)
    |> Map.put(:base_url, BaseURL.default())
  end

  defp enforce_process_auth(connect_options, :inherit), do: connect_options

  defp enforce_process_auth(connect_options, :subscription) do
    process_env =
      connect_options
      |> Keyword.get(:process_env, Keyword.get(connect_options, :env, %{}))
      |> Map.new(fn {key, value} -> {to_string(key), value} end)
      |> Map.put("CODEX_API_KEY", "")
      |> Map.put("OPENAI_API_KEY", "")
      |> Map.put("OPENAI_BASE_URL", "")

    connect_options
    |> Keyword.delete(:env)
    |> Keyword.put(:process_env, process_env)
  end

  defp normalize_skills(skills) when is_list(skills) do
    skills
    |> Enum.reduce_while({:ok, []}, fn skill, {:ok, acc} ->
      case normalize_skill(skill) do
        {:ok, nil} -> {:cont, {:ok, acc}}
        {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, normalized} -> {:ok, Enum.reverse(normalized)}
      error -> error
    end
  end

  defp normalize_skills(other), do: {:error, {:invalid_skills, other}}

  defp normalize_skill(path) when is_binary(path) and path != "" do
    {:ok, %{type: :skill, name: skill_name(path), path: path}}
  end

  defp normalize_skill(%{} = skill) do
    if fetch(skill, :enabled, true) == false do
      {:ok, nil}
    else
      path = fetch(skill, :path, nil)
      name = fetch(skill, :name, nil)

      cond do
        not (is_binary(path) and path != "") ->
          {:error, {:invalid_skill, skill}}

        is_nil(name) ->
          {:ok, %{type: :skill, name: skill_name(path), path: path}}

        is_binary(name) and name != "" ->
          {:ok, %{type: :skill, name: name, path: path}}

        true ->
          {:error, {:invalid_skill, skill}}
      end
    end
  end

  defp normalize_skill(other), do: {:error, {:invalid_skill, other}}

  defp skill_name(path) do
    case Path.basename(path) do
      "SKILL.md" -> path |> Path.dirname() |> Path.basename()
      filename -> Path.rootname(filename)
    end
  end

  defp normalize_keyword(value, field, _aliases) when is_list(value) do
    if Keyword.keyword?(value) do
      {:ok, value}
    else
      {:error, {:invalid_options, field, value}}
    end
  end

  defp normalize_keyword(value, field, aliases) when is_map(value) do
    with {:ok, map} <- normalize_map(value, field, aliases) do
      {:ok, Map.to_list(map)}
    end
  end

  defp normalize_keyword(value, field, _aliases),
    do: {:error, {:invalid_options, field, value}}

  defp normalize_map(value, field, aliases \\ %{})

  defp normalize_map(value, _field, aliases) when is_list(value) do
    if Keyword.keyword?(value) do
      {:ok, canonicalize(Map.new(value), aliases)}
    else
      {:error, {:invalid_options, :map, value}}
    end
  end

  defp normalize_map(value, _field, aliases) when is_map(value) do
    {:ok, canonicalize(value, aliases)}
  end

  defp normalize_map(value, field, _aliases),
    do: {:error, {:invalid_options, field, value}}

  defp canonicalize(map, aliases) do
    Map.new(map, fn {key, value} -> {Map.get(aliases, key, key), value} end)
  end

  defp compact(map) do
    Map.reject(map, fn {_key, value} -> is_nil(value) end)
  end

  defp stringify_keys(%{} = map) do
    Map.new(map, fn
      {key, value} when is_atom(key) -> {Atom.to_string(key), stringify_keys(value)}
      {key, value} -> {key, stringify_keys(value)}
    end)
  end

  defp stringify_keys(list) when is_list(list), do: Enum.map(list, &stringify_keys/1)
  defp stringify_keys(value), do: value

  defp fetch(map, key, default) when is_map(map) do
    Map.get(map, key, Map.get(map, Atom.to_string(key), default))
  end
end
