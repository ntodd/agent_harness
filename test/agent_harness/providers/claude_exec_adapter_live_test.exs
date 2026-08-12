defmodule AgentHarness.Providers.ClaudeExecAdapterLiveTest do
  use ExUnit.Case, async: false

  alias AgentHarness.Providers.Claude.Adapter.Exec, as: AdapterExec

  @moduletag :live
  @moduletag timeout: 300_000

  # Runs the real Claude CLI through the Exec adapter and Exec.Local. The
  # exec adapter requires auth: :inherit, so this test uses the local
  # environment's saved authentication (or ANTHROPIC_API_KEY if exported).
  test "runs a turn through the Exec adapter over Exec.Local" do
    assert {:ok, session} =
             AgentHarness.start_session(:claude,
               cwd: System.tmp_dir!(),
               model: "haiku",
               provider_options: %{
                 auth: :inherit,
                 adapter: {AdapterExec, [exec: {AgentHarness.Exec.Local, []}]}
               }
             )

    on_exit(fn ->
      AgentHarness.stop_session(session, force: true)
    end)

    assert {:ok, turn} =
             AgentHarness.start_turn(session, "Reply with exactly OK and nothing else.")

    assert {:ok,
            %{
              status: :completed,
              result: %{text: text, is_error: false, session_id: session_id}
            }} = AgentHarness.await(turn, timeout: 120_000)

    assert String.trim(text) == "OK"
    assert is_binary(session_id)
    assert :ok = AgentHarness.stop_session(session)
  end
end
