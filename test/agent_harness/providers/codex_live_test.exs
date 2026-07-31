defmodule AgentHarness.Providers.CodexLiveTest do
  use ExUnit.Case, async: false

  @moduletag :live
  @moduletag timeout: 180_000

  alias AgentHarness.Provider.Sink
  alias AgentHarness.Providers.Codex, as: CodexProvider
  alias AgentHarness.{SessionConfig, SessionRef}

  test "connects to the installed authenticated Codex app-server" do
    session = SessionRef.new(:codex, id: "codex-live-open")
    config = SessionConfig.new(session, cwd: File.cwd!())

    assert {:ok, runtime, %{provider_session_id: nil}} =
             CodexProvider.open_session(config, Sink.new(self()))

    assert Process.alive?(runtime)
    assert :ok = CodexProvider.close_session(runtime)
  end

  test "runs a turn through the authenticated Codex app-server" do
    assert {:ok, session} =
             AgentHarness.start_session(:codex,
               cwd: System.tmp_dir!(),
               approval_policy: :never,
               sandbox: :read_only,
               provider_options: %{thread_options: %{ephemeral: true}}
             )

    on_exit(fn ->
      AgentHarness.stop_session(session, force: true)
    end)

    assert {:ok, turn} =
             AgentHarness.start_turn(session, "Reply with exactly OK and nothing else.")

    assert {:ok,
            %{
              status: :completed,
              result: %{
                text: text,
                provider_session_id: provider_session_id,
                provider_turn_id: provider_turn_id
              }
            }} = AgentHarness.await(turn, timeout: 120_000)

    assert String.trim(text) == "OK"
    assert is_binary(provider_session_id)
    assert is_binary(provider_turn_id)
    assert :ok = AgentHarness.stop_session(session)
  end
end
