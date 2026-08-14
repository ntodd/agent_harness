defmodule AgentHarness.Providers.Codex.ConfigTest do
  use ExUnit.Case, async: false

  alias AgentHarness.Providers.Codex.Config
  alias AgentHarness.SessionConfig
  alias Codex.Config.BaseURL

  test "subscription auth is the safe default and suppresses API environment overrides" do
    codex_home = empty_codex_home!()

    config = %SessionConfig{
      session_id: "subscription-auth",
      provider: :codex,
      env: %{
        :CODEX_HOME => codex_home,
        "CODEX_API_KEY" => "session-codex-key",
        "OPENAI_API_KEY" => "session-openai-key",
        "OPENAI_BASE_URL" => "https://example.invalid"
      },
      provider_options: %{
        codex_options: %{codex_path_override: "/opt/bin/codex"},
        connect_options: [
          process_env: %{
            "CODEX_HOME" => "/wrong/provider-home",
            "OPENAI_API_KEY" => "connection-openai-key"
          }
        ]
      }
    }

    assert {:ok, prepared} = Config.prepare(config)
    assert prepared.auth == :subscription
    assert prepared.codex_options.api_key == false
    assert prepared.codex_options.base_url == BaseURL.default()
    assert prepared.codex_options.codex_path_override == "/opt/bin/codex"

    assert prepared.connect_options[:process_env] == %{
             "CODEX_HOME" => codex_home,
             "CODEX_API_KEY" => "",
             "CODEX_MODEL_PROVIDER" => "",
             "CODEX_OLLAMA_BASE_URL" => "",
             "CODEX_OSS_BASE_URL" => "",
             "CODEX_OSS_PROVIDER" => "",
             "CODEX_PROVIDER_BACKEND" => "",
             "OPENAI_API_KEY" => "",
             "OPENAI_BASE_URL" => ""
           }
  end

  test "subscription auth rejects SDK model routing and config overrides" do
    codex_home = empty_codex_home!()

    for {key, value} <- [
          model_payload: %{resolved_model: "paid", env_overrides: %{"PAID_KEY" => "present"}},
          provider_backend: :model_provider,
          model_provider: "paid",
          oss_provider: "ollama",
          external_model_overrides: %{"paid" => %{}},
          config_overrides: [{"model_provider", "paid"}],
          governed_authority: %{},
          execution_surface: %{surface_kind: :remote}
        ] do
      config = %SessionConfig{
        session_id: "subscription-codex-option-#{key}",
        provider: :codex,
        env: %{"CODEX_HOME" => codex_home},
        provider_options: %{codex_options: Map.new([{to_string(key), value}])}
      }

      assert {:error, {:subscription_auth_conflict, {:codex_option, ^key}}} =
               Config.prepare(config)
    end
  end

  test "subscription auth rejects per-thread provider and raw config routing" do
    codex_home = empty_codex_home!()

    for {key, value} <- [
          model_provider: "paid",
          provider: "paid",
          oss: true,
          local_provider: "ollama",
          profile: "paid",
          config: %{"model_provider" => "paid"},
          config_overrides: [{"model_provider", "paid"}],
          allow_provider_model_fallback: true
        ] do
      config = %SessionConfig{
        session_id: "subscription-thread-option-#{key}",
        provider: :codex,
        env: %{"CODEX_HOME" => codex_home},
        provider_options: %{thread_options: Map.new([{to_string(key), value}])}
      }

      assert {:error, {:subscription_auth_conflict, {:thread_option, ^key}}} =
               Config.prepare(config)
    end
  end

  test "subscription auth rejects custom providers selected by Codex config" do
    codex_home = empty_codex_home!()

    File.write!(
      Path.join(codex_home, "config.toml"),
      """
      model_provider = "paid"

      [model_providers.paid]
      name = "Paid gateway"
      base_url = "https://paid.example"
      env_key = "PAID_KEY"
      wire_api = "responses"
      """
    )

    config = %SessionConfig{
      session_id: "subscription-config-provider",
      provider: :codex,
      env: %{"CODEX_HOME" => codex_home}
    }

    assert {:error, {:subscription_auth_conflict, {:codex_config, :model_provider}}} =
             Config.prepare(config)
  end

  test "subscription auth resolves only the official backend and pins every thread" do
    codex_home = empty_codex_home!()
    previous_env = Application.get_env(:codex_sdk, :env, %{})

    Application.put_env(
      :codex_sdk,
      :env,
      Map.merge(previous_env, %{
        "CODEX_PROVIDER_BACKEND" => "oss",
        "CODEX_OSS_PROVIDER" => "ollama",
        "CODEX_OLLAMA_BASE_URL" => "https://paid.example"
      })
    )

    on_exit(fn -> Application.put_env(:codex_sdk, :env, previous_env) end)

    config = %SessionConfig{
      session_id: "subscription-resolved-options",
      provider: :codex,
      env: %{"CODEX_HOME" => codex_home}
    }

    assert {:ok, prepared} = Config.prepare(config)
    assert {:ok, options} = prepared.client.options(prepared.codex_options)
    assert :ok = Config.validate_resolved_options(prepared, options)
    assert options.model_payload.provider_backend == :openai
    assert options.model_payload.env_overrides == %{}

    thread_options = Config.thread_options(prepared, config, self())
    assert thread_options.model_provider == "openai"
    assert thread_options.config["model_provider"] == "openai"
  end

  test "inherit auth preserves SDK options and child environment" do
    config = %SessionConfig{
      session_id: "inherit-auth",
      provider: :codex,
      env: %{"OPENAI_API_KEY" => "intentional"},
      provider_options: %{
        auth: :inherit,
        codex_options: %{api_key: "intentional"}
      }
    }

    assert {:ok, prepared} = Config.prepare(config)
    assert prepared.auth == :inherit
    assert prepared.codex_options == %{api_key: "intentional"}
    assert prepared.connect_options[:process_env] == %{"OPENAI_API_KEY" => "intentional"}
  end

  test "subscription auth rejects explicit SDK API keys" do
    config = %SessionConfig{
      session_id: "explicit-api-auth",
      provider: :codex,
      provider_options: %{
        auth: :subscription,
        codex_options: %{api_key: "sk-explicit"}
      }
    }

    assert {:error, {:subscription_auth_conflict, :provider_api_key}} =
             Config.prepare(config)
  end

  test "subscription auth rejects a stored API-billed profile" do
    codex_home = empty_codex_home!()

    File.write!(
      Path.join(codex_home, "auth.json"),
      JSON.encode!(%{"auth_mode" => "api_key", "OPENAI_API_KEY" => "sk-stored"})
    )

    config = %SessionConfig{
      session_id: "stored-api-auth",
      provider: :codex,
      env: %{"CODEX_HOME" => codex_home}
    }

    assert {:error, {:subscription_auth_conflict, {:stored_auth_mode, :api_key}}} =
             Config.prepare(config)
  end

  test "resume requires an exact provider thread id" do
    config = %SessionConfig{
      session_id: "resume-last",
      provider: :codex,
      provider_options: %{auth: :inherit, resume: :last}
    }

    assert {:error, {:unsupported_provider_session_id, :last}} = Config.prepare(config)

    exact = put_in(config.provider_options.resume, "thread-123")
    assert {:ok, %{provider_session_id: "thread-123"}} = Config.prepare(exact)
  end

  test "rejects unknown auth modes" do
    config = %SessionConfig{
      session_id: "invalid-auth",
      provider: :codex,
      provider_options: %{auth: :api}
    }

    assert {:error, {:invalid_auth_mode, :api}} = Config.prepare(config)
  end

  test "an exec option selects the exec client and rides along in connect options" do
    config = %SessionConfig{
      session_id: "exec-select",
      provider: :codex,
      provider_options: %{
        auth: :inherit,
        exec: {AgentHarness.ExecMock, sandbox: :sb}
      }
    }

    assert {:ok, prepared} = Config.prepare(config)
    assert prepared.client == AgentHarness.Providers.Codex.Client.Exec
    assert prepared.connect_options[:exec] == {AgentHarness.ExecMock, sandbox: :sb}
  end

  test "a bare exec module normalizes to empty options" do
    config = %SessionConfig{
      session_id: "exec-bare",
      provider: :codex,
      provider_options: %{auth: :inherit, exec: AgentHarness.ExecMock}
    }

    assert {:ok, prepared} = Config.prepare(config)
    assert prepared.connect_options[:exec] == {AgentHarness.ExecMock, []}
  end

  test "an explicit client wins over the exec default" do
    config = %SessionConfig{
      session_id: "exec-explicit-client",
      provider: :codex,
      provider_options: %{
        auth: :inherit,
        client: SomeCustomClient,
        exec: {AgentHarness.ExecMock, []}
      }
    }

    assert {:ok, prepared} = Config.prepare(config)
    assert prepared.client == SomeCustomClient
  end

  test "subscription auth rejects remote execution" do
    config = %SessionConfig{
      session_id: "exec-subscription",
      provider: :codex,
      provider_options: %{auth: :subscription, exec: {AgentHarness.ExecMock, []}}
    }

    assert {:error, {:subscription_auth_conflict, :exec}} = Config.prepare(config)
  end

  test "rejects a malformed exec option" do
    config = %SessionConfig{
      session_id: "exec-malformed",
      provider: :codex,
      provider_options: %{auth: :inherit, exec: "nope"}
    }

    assert {:error, {:invalid_exec, "nope"}} = Config.prepare(config)
  end

  defp empty_codex_home! do
    path =
      Path.join(
        System.tmp_dir!(),
        "agent-harness-codex-home-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(path)
    on_exit(fn -> File.rm_rf!(path) end)
    path
  end
end
