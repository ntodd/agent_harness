defmodule AgentHarness.Providers.Claude.OptionsTest do
  use ExUnit.Case, async: false

  alias AgentHarness.Providers.Claude.Options
  alias AgentHarness.SessionConfig

  test "materializes individual SKILL.md directories as one session plugin" do
    source = skill_fixture!()

    config = %SessionConfig{
      session_id: "skills-session",
      provider: :claude,
      skills: [%{name: "release-helper", path: Path.join(source, "SKILL.md")}]
    }

    assert {:ok, prepared} = Options.prepare(config)
    assert [plugin] = prepared.client_options[:plugins]
    assert prepared.cleanup_paths == [plugin]

    assert File.regular?(Path.join([plugin, ".claude-plugin", "plugin.json"]))

    assert File.read!(Path.join([plugin, "skills", "release-helper", "SKILL.md"])) ==
             "# Release helper\n"

    assert File.read!(Path.join([plugin, "skills", "release-helper", "reference.txt"])) ==
             "supporting material\n"

    File.rm_rf!(plugin)
  end

  test "does not create atoms from arbitrary provider option names" do
    config = %SessionConfig{
      session_id: "invalid-options",
      provider: :claude,
      provider_options: %{"this_option_must_not_become_an_atom_938475" => true}
    }

    assert {:error, {:unknown_provider_option, "this_option_must_not_become_an_atom_938475"}} =
             Options.prepare(config)
  end

  test "inherit auth preserves the caller environment without forcing subscription mode" do
    config = %SessionConfig{
      session_id: "inherit-auth",
      provider: :claude,
      env: %{"CUSTOM_VALUE" => "present"},
      provider_options: %{auth: :inherit}
    }

    assert {:ok, prepared} = Options.prepare(config)
    assert prepared.client_options[:env] == %{"CUSTOM_VALUE" => "present"}
  end

  test "merges common and provider environments with provider values taking precedence" do
    config = %SessionConfig{
      session_id: "merged-env",
      provider: :claude,
      env: %{"COMMON" => "present", "OVERRIDE" => "common"},
      provider_options: %{
        auth: :inherit,
        env: %{"PROVIDER" => "present", "OVERRIDE" => "provider"}
      }
    }

    assert {:ok, prepared} = Options.prepare(config)

    assert prepared.client_options[:env] == %{
             "COMMON" => "present",
             "PROVIDER" => "present",
             "OVERRIDE" => "provider"
           }
  end

  test "normalizes environment keys before applying provider precedence" do
    config = %SessionConfig{
      session_id: "normalized-env",
      provider: :claude,
      env: %{"OVERRIDE" => "common"},
      provider_options: %{auth: :inherit, env: %{OVERRIDE: "provider"}}
    }

    assert {:ok, prepared} = Options.prepare(config)
    assert prepared.client_options[:env] == %{"OVERRIDE" => "provider"}
  end

  test "subscription auth fails closed when the Claude SDK has a global API key" do
    previous = Application.get_env(:claude_code, :api_key, :not_configured)
    Application.put_env(:claude_code, :api_key, "global-api-key")

    on_exit(fn ->
      case previous do
        :not_configured -> Application.delete_env(:claude_code, :api_key)
        value -> Application.put_env(:claude_code, :api_key, value)
      end
    end)

    config = %SessionConfig{
      session_id: "subscription-auth",
      provider: :claude,
      provider_options: %{auth: :subscription}
    }

    assert {:error, {:subscription_auth_conflict, :claude_code_api_key}} =
             Options.prepare(config)
  end

  test "subscription auth overrides an API key supplied in the session environment" do
    config = %SessionConfig{
      session_id: "subscription-auth",
      provider: :claude,
      env: %{"ANTHROPIC_API_KEY" => "session-api-key"},
      provider_options: %{
        auth: :subscription,
        env: %{"ANTHROPIC_API_KEY" => "provider-api-key"}
      }
    }

    assert {:ok, prepared} = Options.prepare(config)
    assert prepared.client_options[:env]["ANTHROPIC_API_KEY"] == false
  end

  test "subscription auth unsets alternate API, gateway, cloud, and token routes" do
    auth_env = %{
      "ANTHROPIC_AUTH_TOKEN" => "bearer",
      "ANTHROPIC_BASE_URL" => "https://gateway.example",
      "CLAUDE_CODE_USE_BEDROCK" => "1",
      "CLAUDE_CODE_USE_VERTEX" => "1",
      "CLAUDE_CODE_USE_FOUNDRY" => "1",
      "CLAUDE_CODE_OAUTH_TOKEN" => "ambient-token",
      "CLAUDE_CODE_HOST_AUTH_ENV_VAR" => "HOST_TOKEN",
      "CLAUDE_CODE_HOST_CREDS_FILE" => "/tmp/host-creds.json",
      "CLAUDE_CODE_PROVIDER_MANAGED_BY_HOST" => "1",
      "ANTHROPIC_AWS_API_KEY" => "workspace-key",
      "ANTHROPIC_FOUNDRY_AUTH_TOKEN" => "foundry-token"
    }

    config = %SessionConfig{
      session_id: "isolated-subscription-auth",
      provider: :claude,
      env: auth_env,
      provider_options: %{auth: :subscription}
    }

    assert {:ok, prepared} = Options.prepare(config)

    Enum.each(Map.keys(auth_env), fn key ->
      assert prepared.client_options[:env][key] == false
    end)
  end

  test "subscription auth rejects an explicit SDK API key" do
    config = %SessionConfig{
      session_id: "subscription-auth",
      provider: :claude,
      provider_options: %{auth: :subscription, api_key: "session-api-key"}
    }

    assert {:error, {:subscription_auth_conflict, :provider_api_key}} =
             Options.prepare(config)
  end

  test "subscription auth rejects CLI settings and arguments that can replace authentication" do
    for {key, value} <- [
          adapter: {UnsafeClaudeAdapter, []},
          settings: %{"apiKeyHelper" => "printf fake-key"},
          setting_sources: ["user"],
          extra_args: %{"settings" => ~s({"apiKeyHelper":"printf fake-key"})}
        ] do
      config = %SessionConfig{
        session_id: "subscription-auth-#{key}",
        provider: :claude,
        provider_options: Map.put(%{auth: :subscription}, key, value)
      }

      assert {:error, {:subscription_auth_conflict, {:provider_option, ^key}}} =
               Options.prepare(config)
    end
  end

  test "subscription auth overrides auth-sensitive SDK application defaults" do
    keys = [:adapter, :settings, :setting_sources, :extra_args]
    previous = Map.new(keys, &{&1, Application.get_env(:claude_code, &1, :not_configured)})

    Application.put_env(:claude_code, :settings, %{"apiKeyHelper" => "printf fake-key"})
    Application.put_env(:claude_code, :setting_sources, ["user"])
    Application.put_env(:claude_code, :extra_args, %{"settings" => "unsafe.json"})
    Application.put_env(:claude_code, :adapter, {UnsafeClaudeAdapter, []})

    on_exit(fn ->
      Enum.each(previous, fn
        {key, :not_configured} -> Application.delete_env(:claude_code, key)
        {key, value} -> Application.put_env(:claude_code, key, value)
      end)
    end)

    config = %SessionConfig{
      session_id: "subscription-auth-defaults",
      provider: :claude,
      provider_options: %{auth: :subscription}
    }

    assert {:ok, prepared} = Options.prepare(config)
    assert prepared.client_options[:adapter] == {ClaudeCode.Adapter.Port, []}
    assert prepared.client_options[:settings] == %{}
    assert prepared.client_options[:setting_sources] == []
    assert prepared.client_options[:extra_args] == %{}
  end

  test "maps Claude-native approval and sandbox settings from common configuration" do
    sandbox = %{enabled: true, network: %{allowed_domains: ["hex.pm"]}}

    config = %SessionConfig{
      session_id: "common-options",
      provider: :claude,
      approval_policy: :default,
      sandbox: sandbox
    }

    assert {:ok, prepared} = Options.prepare(config)
    assert prepared.client_options[:permission_mode] == :default
    assert prepared.client_options[:sandbox] == sandbox
  end

  test "isolates MCP and enables partial streaming by default with an explicit opt-out" do
    isolated = %SessionConfig{session_id: "isolated", provider: :claude}

    assert {:ok, prepared} = Options.prepare(isolated)
    assert prepared.client_options[:mcp_servers] == %{}
    assert prepared.client_options[:strict_mcp_config] == true
    assert prepared.client_options[:include_partial_messages] == true

    inherited = %{
      isolated
      | provider_options: %{strict_mcp_config: false, include_partial_messages: false}
    }

    assert {:ok, prepared} = Options.prepare(inherited)
    assert prepared.client_options[:mcp_servers] == %{}
    assert prepared.client_options[:strict_mcp_config] == false
    assert prepared.client_options[:include_partial_messages] == false
  end

  test "cleans a generated skill plugin when later option preparation fails" do
    source = skill_fixture!()
    session_id = "skill-cleanup-#{System.unique_integer([:positive])}"

    config = %SessionConfig{
      session_id: session_id,
      provider: :claude,
      skills: [%{name: "release-helper", path: Path.join(source, "SKILL.md")}],
      provider_options: %{plugins: :invalid}
    }

    assert {:error, {:provider_option_preparation_failed, _, _}} =
             Options.prepare(config)

    pattern = Path.join(System.tmp_dir!(), "agent-harness-claude-#{session_id}-*")
    assert Path.wildcard(pattern) == []
  end

  defp skill_fixture! do
    path =
      Path.join(
        System.tmp_dir!(),
        "agent-harness-claude-skill-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(path)
    File.write!(Path.join(path, "SKILL.md"), "# Release helper\n")
    File.write!(Path.join(path, "reference.txt"), "supporting material\n")

    on_exit(fn -> File.rm_rf!(path) end)
    path
  end
end
