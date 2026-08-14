defmodule AgentHarness.Providers.PiConfigTest do
  use ExUnit.Case, async: true

  alias AgentHarness.Providers.Pi.Config
  alias AgentHarness.{SessionConfig, SessionRef}

  defp session_config(opts \\ []) do
    session = %SessionRef{id: Keyword.get(opts, :session_id, "sess-abc"), provider: :pi}

    opts =
      opts
      |> Keyword.delete(:session_id)
      |> Keyword.put_new(:provider_options, %{auth: :inherit})

    SessionConfig.new(session, opts)
  end

  describe "prepare/1 argv" do
    test "always drives the RPC protocol" do
      {:ok, prepared} = Config.prepare(session_config())

      assert prepared.executable == "pi"
      assert ["--mode", "rpc" | _rest] = prepared.args
    end

    test "assigns the harness session id so pi reuses our identity" do
      {:ok, prepared} = Config.prepare(session_config(session_id: "sess-xyz"))

      assert arg_value(prepared.args, "--session-id") == "sess-xyz"
      assert prepared.provider_session_id == "sess-xyz"
    end

    test "passes model and system prompt through" do
      {:ok, prepared} =
        Config.prepare(session_config(model: "openai/gpt-4.1-nano", system_prompt: "be terse"))

      assert arg_value(prepared.args, "--model") == "openai/gpt-4.1-nano"
      assert arg_value(prepared.args, "--system-prompt") == "be terse"
    end

    test "omits flags that have no configured value" do
      {:ok, prepared} = Config.prepare(session_config())

      refute "--model" in prepared.args
      refute "--system-prompt" in prepared.args
      refute "--name" in prepared.args
    end

    test "emits one --skill flag per skill" do
      {:ok, prepared} =
        Config.prepare(
          session_config(
            skills: [
              "/abs/one/SKILL.md",
              %{name: "release", path: "/abs/two/SKILL.md"}
            ]
          )
        )

      assert arg_values(prepared.args, "--skill") == [
               "/abs/one/SKILL.md",
               "/abs/two/SKILL.md"
             ]
    end

    test "rejects a skill without a usable path" do
      assert {:error, {:invalid_skill, %{name: "release"}}} =
               Config.prepare(session_config(skills: [%{name: "release"}]))
    end

    test "carries cwd and merged env" do
      {:ok, prepared} =
        Config.prepare(session_config(cwd: "/work/project", env: %{"FOO" => "bar"}))

      assert prepared.cwd == "/work/project"
      assert {~c"FOO", ~c"bar"} in prepared.env
    end
  end

  describe "prepare/1 provider options" do
    test "supports tool allow and deny lists" do
      {:ok, prepared} =
        Config.prepare(
          session_config(
            provider_options: %{
              auth: :inherit,
              tools: ["read", "bash"],
              exclude_tools: ["write"]
            }
          )
        )

      assert arg_value(prepared.args, "--tools") == "read,bash"
      assert arg_value(prepared.args, "--exclude-tools") == "write"
    end

    test "supports resume and fork" do
      {:ok, resumed} =
        Config.prepare(session_config(provider_options: %{auth: :inherit, resume: "abc123"}))

      assert arg_value(resumed.args, "--session") == "abc123"

      {:ok, forked} =
        Config.prepare(session_config(provider_options: %{auth: :inherit, fork: "abc123"}))

      assert arg_value(forked.args, "--fork") == "abc123"
    end

    test "resume and fork are mutually exclusive" do
      assert {:error, {:conflicting_session_options, [:resume, :fork]}} =
               Config.prepare(
                 session_config(provider_options: %{auth: :inherit, resume: "a", fork: "b"})
               )
    end

    test "resume drops the harness-assigned session id" do
      {:ok, prepared} =
        Config.prepare(session_config(provider_options: %{auth: :inherit, resume: "abc123"}))

      refute "--session-id" in prepared.args
    end

    test "ephemeral sessions disable persistence" do
      {:ok, prepared} =
        Config.prepare(session_config(provider_options: %{auth: :inherit, session: false}))

      assert "--no-session" in prepared.args
      refute "--session-id" in prepared.args
    end

    test "rejects unknown provider options" do
      assert {:error, {:unknown_provider_options, [:nope]}} =
               Config.prepare(session_config(provider_options: %{auth: :inherit, nope: 1}))
    end
  end

  describe "prepare/1 unsupported harness features" do
    test "rejects per-session MCP servers because pi has no MCP support" do
      assert {:error, {:unsupported, :per_session_mcp}} =
               Config.prepare(session_config(mcp_servers: %{"tools" => %{command: "npx"}}))
    end

    test "rejects approval policies because pi has no permission system" do
      assert {:error, {:unsupported, :approvals}} =
               Config.prepare(session_config(approval_policy: :on_request))
    end

    test "rejects sandbox settings because pi does not sandbox" do
      assert {:error, {:unsupported, :sandbox}} =
               Config.prepare(session_config(sandbox: :workspace_write))
    end
  end

  describe "prepare/1 authentication policy" do
    test "defaults to subscription auth" do
      session = %SessionRef{id: "sess-abc", provider: :pi}
      {:ok, prepared} = Config.prepare(SessionConfig.new(session, []))

      assert prepared.auth == :subscription
    end

    test "subscription auth rejects an explicit API key" do
      assert {:error, {:subscription_auth_conflict, :api_key}} =
               Config.prepare(
                 session_config(provider_options: %{auth: :subscription, api_key: "sk-test"})
               )
    end

    test "subscription auth rejects API-key environment overrides" do
      assert {:error, {:subscription_auth_conflict, {:env, "OPENAI_API_KEY"}}} =
               Config.prepare(
                 session_config(
                   env: %{"OPENAI_API_KEY" => "sk-test"},
                   provider_options: %{auth: :subscription}
                 )
               )
    end

    test "inherit auth allows an explicit API key" do
      {:ok, prepared} =
        Config.prepare(session_config(provider_options: %{auth: :inherit, api_key: "sk-test"}))

      assert arg_value(prepared.args, "--api-key") == "sk-test"
      assert prepared.auth == :inherit
    end

    test "rejects an unknown auth mode" do
      assert {:error, {:invalid_auth_mode, :nope}} =
               Config.prepare(session_config(provider_options: %{auth: :nope}))
    end
  end

  describe "prepare/1 remote execution" do
    test "an exec option selects the exec client and rides along in prepared" do
      {:ok, prepared} =
        Config.prepare(
          session_config(
            provider_options: %{auth: :inherit, exec: {AgentHarness.ExecMock, sandbox: :sb}}
          )
        )

      assert prepared.client == AgentHarness.Providers.Pi.Client.Exec
      assert prepared.exec == {AgentHarness.ExecMock, sandbox: :sb}
    end

    test "a bare exec module normalizes to empty options" do
      {:ok, prepared} =
        Config.prepare(
          session_config(provider_options: %{auth: :inherit, exec: AgentHarness.ExecMock})
        )

      assert prepared.exec == {AgentHarness.ExecMock, []}
    end

    test "an explicit client wins over the exec default" do
      {:ok, prepared} =
        Config.prepare(
          session_config(
            provider_options: %{
              auth: :inherit,
              client: SomeCustomClient,
              exec: {AgentHarness.ExecMock, []}
            }
          )
        )

      assert prepared.client == SomeCustomClient
    end

    test "without an exec option the client defaults to the local port" do
      {:ok, prepared} = Config.prepare(session_config())

      assert prepared.client == AgentHarness.Providers.Pi.Client.Port
      assert prepared.exec == {AgentHarness.Exec.Local, []}
    end

    test "subscription auth rejects remote execution" do
      assert {:error, {:subscription_auth_conflict, :exec}} =
               Config.prepare(
                 session_config(
                   provider_options: %{auth: :subscription, exec: {AgentHarness.ExecMock, []}}
                 )
               )
    end

    test "rejects a malformed exec option" do
      assert {:error, {:invalid_exec, "nope"}} =
               Config.prepare(session_config(provider_options: %{auth: :inherit, exec: "nope"}))
    end
  end

  defp arg_value(args, flag) do
    args
    |> arg_values(flag)
    |> List.first()
  end

  defp arg_values(args, flag) do
    args
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.filter(fn [candidate, _value] -> candidate == flag end)
    |> Enum.map(fn [_flag, value] -> value end)
  end
end
