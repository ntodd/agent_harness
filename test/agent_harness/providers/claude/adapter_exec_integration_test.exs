defmodule AgentHarness.Providers.Claude.AdapterExecIntegrationTest do
  use ExUnit.Case, async: false

  alias AgentHarness.Event
  alias AgentHarness.Providers.Claude.Adapter.Exec, as: AdapterExec
  alias AgentHarness.{Request, Response}

  @fixture Path.expand("../../../support/fixtures/fake_claude_cli", __DIR__)

  setup_all do
    File.chmod!(@fixture, 0o755)
    :ok
  end

  defp adapter_tuple(exec_env \\ %{}) do
    {AdapterExec,
     [
       exec: {AgentHarness.Exec.Local, []},
       cli_path: @fixture,
       env: exec_env
     ]}
  end

  defp await_ready(client, attempts \\ 100) do
    ready? =
      ClaudeCode.Session.health(client) == :healthy and
        match?({:ok, info} when not is_nil(info), ClaudeCode.Session.server_info(client))

    cond do
      ready? ->
        :ok

      attempts == 0 ->
        flunk("ClaudeCode session never became ready")

      true ->
        Process.sleep(50)
        await_ready(client, attempts - 1)
    end
  end

  test "drives a real ClaudeCode session end to end through Exec.Local" do
    {:ok, client} =
      ClaudeCode.start_link(
        adapter: adapter_tuple(),
        api_key: "sk-test-integration"
      )

    await_ready(client)

    messages = client |> ClaudeCode.stream("explain this PR") |> Enum.to_list()

    assert Enum.any?(messages, &match?(%ClaudeCode.Message.AssistantMessage{}, &1))

    assert [%ClaudeCode.Message.ResultMessage{} = result] =
             Enum.filter(messages, &match?(%ClaudeCode.Message.ResultMessage{}, &1))

    assert result.result == "fake explainer output"
    assert result.session_id == "fake-cli-session"

    :ok = ClaudeCode.stop(client)
  end

  test "completes a turn through the full agent_harness stack" do
    assert {:ok, session} =
             AgentHarness.start_session(:claude,
               provider_options: %{
                 auth: :inherit,
                 api_key: "sk-test-integration",
                 adapter: adapter_tuple()
               }
             )

    assert {:ok, turn} = AgentHarness.start_turn(session, "explain this PR")

    assert {:ok, %{status: :completed, result: result}} =
             AgentHarness.await(turn, timeout: 30_000)

    assert result.text == "fake explainer output"
    assert result.session_id == "fake-cli-session"

    assert :ok = AgentHarness.stop_session(session)
  end

  test "routes an approval round trip through respond/2 over exec" do
    assert {:ok, session} =
             AgentHarness.start_session(:claude,
               provider_options: %{
                 auth: :inherit,
                 api_key: "sk-test-integration",
                 adapter: adapter_tuple(%{"FAKE_CLI_ASK" => "1"})
               }
             )

    assert {:ok, subscription} = AgentHarness.subscribe(session, from: :start)
    assert {:ok, turn} = AgentHarness.start_turn(session, "explain this PR")

    request =
      receive_request(subscription.ref)

    assert request.turn_id == turn.id

    assert :ok = AgentHarness.respond(request, Response.approve(scope: :once))

    assert {:ok, %{status: :completed}} = AgentHarness.await(turn, timeout: 30_000)

    assert :ok = AgentHarness.unsubscribe(subscription)
    assert :ok = AgentHarness.stop_session(session)
  end

  defp receive_request(subscription_ref) do
    receive do
      {:agent_harness, ^subscription_ref,
       %Event{type: :request_created, data: %Request{} = request}} ->
        request

      {:agent_harness, ^subscription_ref, %Event{}} ->
        receive_request(subscription_ref)
    after
      30_000 -> flunk("no request_created event arrived")
    end
  end
end
