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

    test "redaction survives limit: :infinity" do
      rendered = inspect(config(), limit: :infinity, printable_limit: :infinity)

      refute rendered =~ @secret
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
