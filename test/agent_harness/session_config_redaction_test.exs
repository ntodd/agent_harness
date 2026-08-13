defmodule AgentHarness.SessionConfigRedactionTest do
  use ExUnit.Case, async: true

  alias AgentHarness.{SessionConfig, SessionRef}

  @secret "sk-ant-api03-super-secret-value"

  defp config do
    session = SessionRef.new(:claude, id: "redaction-session")

    SessionConfig.new(session,
      env: %{"ANTHROPIC_API_KEY" => @secret, "OTHER" => "also-#{@secret}"},
      provider_options: %{
        auth: :inherit,
        api_key: @secret,
        client_options: [auth_token: @secret]
      },
      mcp_servers: %{
        "docs" => %{
          command: "docs-mcp",
          env: %{"DOCS_TOKEN" => @secret},
          headers: %{"Authorization" => "Bearer #{@secret}"}
        }
      }
    )
  end

  describe "Inspect implementation" do
    test "env values never appear in inspect output" do
      rendered = inspect(config())

      refute rendered =~ @secret
    end

    test "env keys remain visible for debugging" do
      rendered = inspect(config())

      assert rendered =~ "ANTHROPIC_API_KEY"
    end

    test "provider_options contents never appear in inspect output" do
      rendered = inspect(config())

      refute rendered =~ @secret
      refute rendered =~ "auth_token"
    end

    test "provider_options keeps top-level keys for debugging" do
      rendered = inspect(config())

      assert rendered =~ "api_key"
      assert rendered =~ "client_options"
    end

    test "redaction survives limit: :infinity" do
      rendered = inspect(config(), limit: :infinity, printable_limit: :infinity)

      refute rendered =~ @secret
    end

    test "mcp_servers contents never appear in inspect output" do
      rendered = inspect(config())

      refute rendered =~ @secret
      refute rendered =~ "Authorization"
    end

    test "mcp_servers keeps server names for debugging" do
      rendered = inspect(config())

      assert rendered =~ "docs"
    end

    test "redact/1 keeps provider_options keys with redacted values" do
      redacted = SessionConfig.redact(config())

      assert redacted.provider_options == %{
               auth: "[REDACTED]",
               api_key: "[REDACTED]",
               client_options: "[REDACTED]"
             }
    end

    test "redact/1 keeps mcp_servers names with redacted values" do
      redacted = SessionConfig.redact(config())

      assert redacted.mcp_servers == %{"docs" => "[REDACTED]"}
    end

    test "non-sensitive fields are still rendered" do
      session = SessionRef.new(:claude, id: "redaction-session")
      config = SessionConfig.new(session, cwd: "/workspace", model: "claude-opus-5")

      rendered = inspect(config)

      assert rendered =~ "/workspace"
      assert rendered =~ "claude-opus-5"
    end
  end
end
