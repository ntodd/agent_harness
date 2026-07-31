defmodule AgentHarness.Providers.ClaudeLiveTest do
  use ExUnit.Case, async: false

  alias AgentHarness.{Event, Request, Response}

  @moduletag :live
  @moduletag timeout: 180_000

  test "runs a turn through the authenticated global Claude CLI" do
    assert {:ok, session} =
             AgentHarness.start_session(:claude,
               cwd: System.tmp_dir!(),
               model: "haiku",
               provider_options: %{auth: :subscription}
             )

    on_exit(fn ->
      AgentHarness.stop_session(session, force: true)
    end)

    assert {:ok, turn} =
             AgentHarness.start_turn(session, "Reply with exactly OK and nothing else.")

    assert {:ok,
            %{
              status: :completed,
              result: %{text: "OK", is_error: false, session_id: session_id}
            }} = AgentHarness.await(turn, timeout: 120_000)

    assert is_binary(session_id)
    assert :ok = AgentHarness.stop_session(session)
  end

  test "round-trips AskUserQuestion through an AgentHarness request" do
    assert {:ok, session} =
             AgentHarness.start_session(:claude,
               cwd: System.tmp_dir!(),
               model: "haiku",
               provider_options: %{
                 auth: :subscription,
                 tools: ["AskUserQuestion"],
                 max_turns: 3
               }
             )

    on_exit(fn ->
      AgentHarness.stop_session(session, force: true)
    end)

    assert {:ok, subscription} = AgentHarness.subscribe(session, from: :latest)

    assert {:ok, turn} =
             AgentHarness.start_turn(
               session,
               "Call AskUserQuestion once to ask whether I prefer red or blue. " <>
                 "After receiving the answer, state it in one sentence."
             )

    assert_receive {:agent_harness, subscription_ref,
                    %Event{
                      type: :request_created,
                      data: %Request{} = request
                    }},
                   120_000

    assert subscription_ref == subscription.ref
    assert request.kind == :question
    assert request.prompt == "Do you prefer red or blue?"
    assert :ok = AgentHarness.respond(request, Response.answer("Red"))

    assert {:ok, %{status: :completed, result: %{text: text, is_error: false}}} =
             AgentHarness.await(turn, timeout: 120_000)

    assert text =~ "red"
    assert :ok = AgentHarness.stop_session(session)
  end

  test "cancels while Claude is blocked in AskUserQuestion" do
    assert {:ok, session} =
             AgentHarness.start_session(:claude,
               cwd: System.tmp_dir!(),
               model: "haiku",
               provider_options: %{
                 auth: :subscription,
                 tools: ["AskUserQuestion"],
                 max_turns: 3
               }
             )

    on_exit(fn ->
      AgentHarness.stop_session(session, force: true)
    end)

    assert {:ok, subscription} = AgentHarness.subscribe(session, from: :latest)

    assert {:ok, turn} =
             AgentHarness.start_turn(
               session,
               "Call AskUserQuestion once to ask whether I prefer red or blue, then wait."
             )

    assert_receive {:agent_harness, subscription_ref,
                    %Event{type: :request_created, data: %Request{kind: :question}}},
                   120_000

    assert subscription_ref == subscription.ref
    assert :ok = AgentHarness.cancel(turn)

    assert {:error, %Event{type: terminal_type}} =
             AgentHarness.await(turn, timeout: 120_000)

    assert terminal_type in [:turn_cancelled, :turn_interrupted]
    assert :ok = AgentHarness.stop_session(session)
  end
end
