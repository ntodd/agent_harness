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
             "OPENAI_API_KEY" => "",
             "OPENAI_BASE_URL" => ""
           }
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
