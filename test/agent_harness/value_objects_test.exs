defmodule AgentHarness.ValueObjectsTest do
  use ExUnit.Case, async: true

  alias AgentHarness.{Capabilities, Request, Response, SessionConfig, SessionRef, Turn}

  test "session references are stable public handles without a pid" do
    ref = SessionRef.new(:claude, id: "session-1")

    assert ref == %SessionRef{id: "session-1", provider: :claude}
    refute Map.has_key?(ref, :pid)
  end

  test "session and turn handles reject unusable ids" do
    assert_raise ArgumentError, "session id must be a non-empty string", fn ->
      SessionRef.new(:claude, id: "")
    end

    assert_raise ArgumentError, "session id must be a non-empty string", fn ->
      SessionRef.new(:claude, id: 42)
    end

    assert_raise ArgumentError, "session id must be a non-empty string", fn ->
      Turn.new("", "Implement it")
    end

    assert_raise ArgumentError, "turn id must be a non-empty string", fn ->
      Turn.new("session-1", "Implement it", id: 42)
    end
  end

  test "turns begin queued and retain caller metadata" do
    turn = Turn.new("session-1", "Implement it", metadata: %{job_id: 42})

    assert %Turn{
             id: id,
             session_id: "session-1",
             input: "Implement it",
             status: :queued,
             metadata: %{job_id: 42}
           } = turn

    assert is_binary(id)
  end

  test "requests preserve the provider correlation value" do
    questions = [
      %{
        id: "database",
        prompt: "Which database?",
        choices: [%{label: "Postgres", value: "postgres"}]
      }
    ]

    request =
      Request.new(
        session_id: "session-1",
        turn_id: "turn-1",
        kind: :question,
        prompt: "Configuration needed",
        questions: questions,
        provider_ref: 17
      )

    assert %Request{
             id: id,
             session_id: "session-1",
             turn_id: "turn-1",
             kind: :question,
             prompt: "Configuration needed",
             questions: ^questions,
             provider_ref: 17,
             status: :pending
           } = request

    assert is_binary(id)
  end

  test "response helpers produce an explicit provider-neutral decision" do
    assert Response.answer(%{"database" => ["Postgres"]}) ==
             %Response{action: :answer, value: %{"database" => ["Postgres"]}}

    assert Response.approve(scope: :session) ==
             %Response{action: :approve, scope: :session}

    assert Response.deny("Not in this workspace") ==
             %Response{action: :deny, reason: "Not in this workspace"}

    assert_raise ArgumentError, fn -> Response.approve(scope: :totally_bogus) end

    assert {:error, {:invalid_response, _response}} =
             Response.validate(%Response{action: :approve, scope: :totally_bogus})
  end

  test "capabilities distinguish native, emulated, experimental, and unsupported support" do
    capabilities =
      Capabilities.new(
        token_streaming: :native,
        questions: :emulated,
        steer: :experimental
      )

    assert capabilities.token_streaming == :native
    assert capabilities.questions == :emulated
    assert capabilities.steer == :experimental
    assert capabilities.fork == :unsupported
  end

  test "session configuration retains isolated provider settings" do
    config =
      SessionConfig.new(
        %SessionRef{id: "session-1", provider: :codex},
        cwd: "/work/project",
        mcp_servers: %{"docs" => %{command: "docs-server"}},
        skills: [%{name: "release", path: "/skills/release/SKILL.md"}],
        provider_options: %{experimental_api: true}
      )

    assert %SessionConfig{
             session_id: "session-1",
             provider: :codex,
             cwd: "/work/project",
             mcp_servers: %{"docs" => %{command: "docs-server"}},
             skills: [%{name: "release", path: "/skills/release/SKILL.md"}],
             provider_options: %{experimental_api: true}
           } = config
  end

  test "completed turn cache defaults to the event buffer and accepts bounded overrides" do
    session = %SessionRef{id: "session-1", provider: :codex}

    assert %SessionConfig{completed_turn_cache_size: 7} =
             SessionConfig.new(session, event_buffer_size: 7)

    assert %SessionConfig{completed_turn_cache_size: 0} =
             SessionConfig.new(session, completed_turn_cache_size: 0)

    assert %SessionConfig{completed_turn_cache_size: :infinity} =
             SessionConfig.new(session, completed_turn_cache_size: :infinity)

    assert_raise ArgumentError,
                 "completed_turn_cache_size must be a non-negative integer or :infinity",
                 fn ->
                   SessionConfig.new(session, completed_turn_cache_size: -1)
                 end
  end
end
