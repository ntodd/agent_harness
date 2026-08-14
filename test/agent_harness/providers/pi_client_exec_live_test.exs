defmodule AgentHarness.Providers.Pi.ClientExecLiveTest do
  @moduledoc """
  End-to-end check of the exec client against a locally installed `pi` CLI.

  Runs the real CLI through `Client.Exec` and `Exec.Local`. The exec client
  requires `auth: :inherit`, so this runs with whichever API key is already
  exported. Set `PI_LIVE_MODEL` to override the model.
  """

  use ExUnit.Case, async: false

  @moduletag :live
  @moduletag timeout: 240_000

  @model System.get_env("PI_LIVE_MODEL", "openai/gpt-4.1-nano")

  setup do
    agent_dir =
      Path.join(
        System.tmp_dir!(),
        "agent-harness-pi-exec-live-#{System.unique_integer([:positive])}"
      )

    cwd = Path.join(agent_dir, "workspace")
    File.mkdir_p!(cwd)
    on_exit(fn -> File.rm_rf(agent_dir) end)

    %{agent_dir: agent_dir, cwd: cwd}
  end

  test "runs a turn through the exec client over Exec.Local", context do
    assert {:ok, session} =
             AgentHarness.start_session(:pi,
               cwd: context.cwd,
               model: @model,
               provider_options: %{
                 auth: :inherit,
                 agent_dir: context.agent_dir,
                 offline: true,
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
