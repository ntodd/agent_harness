defmodule AgentHarness.Providers.Codex.ClientExecLiveTest do
  @moduledoc """
  End-to-end check of the exec client against a locally installed `codex` CLI.

  Runs the real app-server through `Client.Exec` and `Exec.Local`. The exec
  client requires `auth: :inherit`, so this runs with the local environment's
  saved Codex authentication (or an exported API key).
  """

  use ExUnit.Case, async: false

  @moduletag :live
  @moduletag timeout: 300_000

  test "runs a turn through the exec connection over Exec.Local" do
    assert {:ok, session} =
             AgentHarness.start_session(:codex,
               cwd: System.tmp_dir!(),
               provider_options: %{
                 auth: :inherit,
                 exec: {AgentHarness.Exec.Local, []}
               }
             )

    on_exit(fn -> AgentHarness.stop_session(session, force: true) end)

    assert {:ok, turn} =
             AgentHarness.start_turn(session, "Reply with exactly OK and nothing else.")

    assert {:ok, %{status: :completed, result: %{text: text}}} =
             AgentHarness.await(turn, timeout: 180_000)

    assert String.trim(text) =~ "OK"
    assert :ok = AgentHarness.stop_session(session)
  end
end
