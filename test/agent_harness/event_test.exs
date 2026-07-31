defmodule AgentHarness.EventTest do
  use ExUnit.Case, async: true

  alias AgentHarness.Event

  test "builds a versioned, ordered event without losing the provider payload" do
    raw = %{"type" => "turn.completed", "usage" => %{"input_tokens" => 12}}

    event =
      Event.new(
        session_id: "session-1",
        turn_id: "turn-1",
        provider: :codex,
        type: :turn_completed,
        seq: 3,
        data: %{status: :completed},
        raw: raw
      )

    assert %Event{
             schema_version: 1,
             id: id,
             session_id: "session-1",
             turn_id: "turn-1",
             provider: :codex,
             type: :turn_completed,
             seq: 3,
             data: %{status: :completed},
             raw: ^raw,
             at: %DateTime{}
           } = event

    assert is_binary(id)
    assert id != ""
  end

  test "rejects missing required fields" do
    assert_raise ArgumentError, ~r/session_id/, fn ->
      Event.new(
        turn_id: "turn-1",
        provider: :codex,
        type: :turn_started,
        seq: 1
      )
    end
  end
end
