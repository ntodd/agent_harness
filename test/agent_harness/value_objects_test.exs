defmodule AgentHarness.ValueObjectsTest do
  use ExUnit.Case, async: true

  alias AgentHarness.{Request, SessionRef, Turn}

  test "session references are stable public handles without a pid" do
    ref = SessionRef.new(:claude, id: "session-1")

    assert ref == %SessionRef{id: "session-1", provider: :claude}
    refute Map.has_key?(ref, :pid)
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
    request =
      Request.new(
        session_id: "session-1",
        turn_id: "turn-1",
        kind: :question,
        prompt: "Which database?",
        choices: [%{label: "Postgres", value: "postgres"}],
        provider_ref: 17
      )

    assert %Request{
             id: id,
             session_id: "session-1",
             turn_id: "turn-1",
             kind: :question,
             prompt: "Which database?",
             choices: [%{label: "Postgres", value: "postgres"}],
             provider_ref: 17,
             status: :pending
           } = request

    assert is_binary(id)
  end
end
