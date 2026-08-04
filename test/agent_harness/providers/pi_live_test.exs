defmodule AgentHarness.Providers.PiLiveTest do
  @moduledoc """
  End-to-end checks against a locally installed `pi` CLI.

  Pi is bring-your-own-model, so these run with `auth: :inherit` and whichever
  API key is already exported. Set `PI_LIVE_MODEL` to override the model; the
  default is a small one so a full run costs a fraction of a cent.
  """

  use ExUnit.Case, async: false

  @moduletag :live
  @moduletag timeout: 240_000

  alias AgentHarness.Event
  alias AgentHarness.Provider.Sink
  alias AgentHarness.Providers.Pi, as: PiProvider
  alias AgentHarness.{SessionConfig, SessionRef}

  @model System.get_env("PI_LIVE_MODEL", "openai/gpt-4.1-nano")

  setup do
    # Each test gets its own pi config and session directory so live runs never
    # read or write the developer's real ~/.pi state.
    agent_dir =
      Path.join(System.tmp_dir!(), "agent-harness-pi-live-#{System.unique_integer([:positive])}")

    cwd = Path.join(agent_dir, "workspace")
    File.mkdir_p!(cwd)
    on_exit(fn -> File.rm_rf(agent_dir) end)

    %{agent_dir: agent_dir, cwd: cwd}
  end

  defp provider_options(agent_dir, extra \\ %{}) do
    Map.merge(
      %{auth: :inherit, agent_dir: agent_dir, offline: true},
      extra
    )
  end

  defp session_opts(context, extra \\ []) do
    Keyword.merge(
      [
        cwd: context.cwd,
        model: @model,
        provider_options: provider_options(context.agent_dir)
      ],
      extra
    )
  end

  test "opens a session against the installed CLI and adopts the harness id", context do
    session = SessionRef.new(:pi, id: "pi-live-open")

    config =
      SessionConfig.new(session,
        cwd: context.cwd,
        model: @model,
        provider_options: provider_options(context.agent_dir)
      )

    assert {:ok, runtime, %{provider_session_id: "pi-live-open"}} =
             PiProvider.open_session(config, Sink.new(self()))

    assert Process.alive?(runtime)
    assert :ok = PiProvider.close_session(runtime)
  end

  test "runs a turn and streams assistant text", context do
    assert {:ok, session} = AgentHarness.start_session(:pi, session_opts(context))
    on_exit(fn -> AgentHarness.stop_session(session, force: true) end)

    assert {:ok, turn} =
             AgentHarness.start_turn(session, "Reply with exactly OK and nothing else.")

    assert {:ok, %{status: :completed, result: %{text: text}}} =
             AgentHarness.await(turn, timeout: 180_000)

    assert String.trim(text) =~ "OK"
    assert :ok = AgentHarness.stop_session(session)
  end

  test "delivers token deltas to a subscriber before the turn completes", context do
    assert {:ok, session} = AgentHarness.start_session(:pi, session_opts(context))
    on_exit(fn -> AgentHarness.stop_session(session, force: true) end)

    assert {:ok, turn} = AgentHarness.start_turn(session, "Count from 1 to 5, space separated.")
    assert {:ok, subscription} = AgentHarness.subscribe(turn, from: :start)

    streamed = collect_deltas(subscription.ref, "")

    assert {:ok, %{status: :completed, result: %{text: text}}} =
             AgentHarness.await(turn, timeout: 180_000)

    assert streamed != ""
    assert String.trim(streamed) == String.trim(text)
    AgentHarness.unsubscribe(subscription)
  end

  test "runs a real tool call and reports its result", context do
    File.write!(Path.join(context.cwd, "marker.txt"), "pi-live-marker\n")

    opts =
      session_opts(context,
        provider_options: provider_options(context.agent_dir, %{tools: ["read", "ls"]})
      )

    assert {:ok, session} = AgentHarness.start_session(:pi, opts)
    on_exit(fn -> AgentHarness.stop_session(session, force: true) end)

    assert {:ok, turn} =
             AgentHarness.start_turn(
               session,
               "Read marker.txt in the current directory and reply with its exact contents."
             )

    assert {:ok, subscription} = AgentHarness.subscribe(turn, from: :start)
    tools = collect_tool_names(subscription.ref, [])

    assert {:ok, %{status: :completed, result: %{text: text}}} =
             AgentHarness.await(turn, timeout: 180_000)

    assert "read" in tools
    assert text =~ "pi-live-marker"
    AgentHarness.unsubscribe(subscription)
  end

  test "cancels a running turn", context do
    assert {:ok, session} = AgentHarness.start_session(:pi, session_opts(context))
    on_exit(fn -> AgentHarness.stop_session(session, force: true) end)

    assert {:ok, turn} =
             AgentHarness.start_turn(
               session,
               "Write an exhaustive 5000 word essay about the history of the Erlang VM."
             )

    assert {:ok, subscription} = AgentHarness.subscribe(turn, from: :start)
    assert :ok = await_first_delta(subscription.ref)
    assert :ok = AgentHarness.cancel(turn)

    assert {:error, %Event{type: :turn_cancelled}} = AgentHarness.await(turn, timeout: 120_000)
    AgentHarness.unsubscribe(subscription)
  end

  test "resumes a persisted session and keeps its history", context do
    assert {:ok, first} = AgentHarness.start_session(:pi, session_opts(context))

    assert {:ok, turn} =
             AgentHarness.start_turn(
               first,
               "Remember this codeword: banana-frigate. Reply with exactly OK."
             )

    assert {:ok, %{status: :completed}} = AgentHarness.await(turn, timeout: 180_000)
    assert :ok = AgentHarness.stop_session(first)

    # Pi writes session entries as they happen, so the transcript is on disk by
    # the time the first session is closed.
    assert [session_file] = Path.wildcard(Path.join(context.agent_dir, "sessions/**/*.jsonl"))

    assert {:ok, resumed} =
             AgentHarness.start_session(
               :pi,
               session_opts(context,
                 provider_options: provider_options(context.agent_dir, %{resume: session_file})
               )
             )

    on_exit(fn -> AgentHarness.stop_session(resumed, force: true) end)

    assert {:ok, recall} =
             AgentHarness.start_turn(resumed, "What was the codeword? Reply with just the word.")

    assert {:ok, %{status: :completed, result: %{text: text}}} =
             AgentHarness.await(recall, timeout: 180_000)

    assert text =~ "banana-frigate"
    assert :ok = AgentHarness.stop_session(resumed)
  end

  test "rejects a session that asks for features pi does not have", context do
    assert {:error, {:provider_open_failed, {:unsupported, :per_session_mcp}}} =
             AgentHarness.start_session(
               :pi,
               session_opts(context, mcp_servers: %{"docs" => %{command: "docs-mcp"}})
             )
  end

  test "a missing CLI fails to open rather than hanging", context do
    session = SessionRef.new(:pi, id: "pi-live-missing")

    config =
      SessionConfig.new(session,
        cwd: context.cwd,
        provider_options: provider_options(context.agent_dir, %{executable: "pi-does-not-exist"})
      )

    assert {:error, {:cli_not_found, "pi-does-not-exist"}} =
             PiProvider.open_session(config, Sink.new(self()))
  end

  defp collect_deltas(ref, acc) do
    receive do
      {:agent_harness, ^ref, %Event{type: :message_delta, data: %{text: text}}} ->
        collect_deltas(ref, acc <> text)

      {:agent_harness, ^ref, %Event{type: type}}
      when type in [:turn_completed, :turn_failed, :turn_cancelled, :turn_interrupted] ->
        acc

      {:agent_harness, ^ref, %Event{}} ->
        collect_deltas(ref, acc)
    after
      180_000 -> acc
    end
  end

  defp collect_tool_names(ref, acc) do
    receive do
      {:agent_harness, ^ref, %Event{type: :tool_started, data: %{tool_name: name}}} ->
        collect_tool_names(ref, [name | acc])

      {:agent_harness, ^ref, %Event{type: type}}
      when type in [:turn_completed, :turn_failed, :turn_cancelled, :turn_interrupted] ->
        acc

      {:agent_harness, ^ref, %Event{}} ->
        collect_tool_names(ref, acc)
    after
      180_000 -> acc
    end
  end

  defp await_first_delta(ref) do
    receive do
      {:agent_harness, ^ref, %Event{type: :message_delta}} -> :ok
      {:agent_harness, ^ref, %Event{}} -> await_first_delta(ref)
    after
      120_000 -> {:error, :no_delta}
    end
  end
end
