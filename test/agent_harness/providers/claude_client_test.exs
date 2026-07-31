defmodule AgentHarness.Providers.Claude.ClientTest do
  use ExUnit.Case, async: true

  alias AgentHarness.Providers.Claude.Client.Default

  defmodule FakeSession do
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    @impl true
    def init(opts) do
      {:ok,
       %{
         pending_checks: Keyword.get(opts, :pending_checks, 0),
         health: Keyword.get(opts, :health, :healthy)
       }}
    end

    @impl true
    def handle_call(:get_server_info, _from, %{pending_checks: checks} = state)
        when checks > 0 do
      {:reply, {:ok, nil}, %{state | pending_checks: checks - 1}}
    end

    def handle_call(:get_server_info, _from, state) do
      {:reply, {:ok, %{commands: [], models: []}}, state}
    end

    def handle_call(:health, _from, state), do: {:reply, state.health, state}
  end

  test "waits for the Claude initialization handshake" do
    session = start_supervised!({FakeSession, pending_checks: 2})
    assert :ok = Default.await_ready(session, 500)
  end

  test "reports a disconnected Claude transport before the timeout" do
    session =
      start_supervised!({FakeSession, pending_checks: 100, health: {:unhealthy, :not_connected}})

    assert {:error, {:claude_readiness_failed, :not_connected}} =
             Default.await_ready(session, 500)
  end

  test "readiness timeout bounds an unresponsive session call" do
    session =
      spawn(fn ->
        receive do
          _message -> Process.sleep(:infinity)
        end
      end)

    on_exit(fn -> if Process.alive?(session), do: Process.exit(session, :kill) end)
    started_at = System.monotonic_time(:millisecond)

    assert {:error, {:claude_readiness_timeout, :server_info}} =
             Default.await_ready(session, 25)

    assert System.monotonic_time(:millisecond) - started_at < 250
  end

  test "verifies a first-party Claude subscription with the selected CLI" do
    cli =
      executable_fixture!(
        ~s({"loggedIn":true,"authMethod":"claude.ai","apiProvider":"firstParty","subscriptionType":"max"})
      )

    assert :ok =
             Default.verify_subscription_auth(
               [cli_path: cli, env: %{"ANTHROPIC_API_KEY" => false}],
               500
             )
  end

  test "rejects a CLI auth status that would use API billing" do
    cli =
      executable_fixture!(~s({"loggedIn":true,"authMethod":"api_key","apiProvider":"firstParty"}))

    assert {:error,
            {:subscription_auth_required,
             %{
               "loggedIn" => true,
               "authMethod" => "api_key",
               "apiProvider" => "firstParty"
             }}} = Default.verify_subscription_auth([cli_path: cli], 500)
  end

  defp executable_fixture!(output) do
    path =
      Path.join(
        System.tmp_dir!(),
        "agent-harness-claude-auth-#{System.unique_integer([:positive])}"
      )

    File.write!(path, "#!/bin/sh\nprintf '%s' '#{output}'\n")
    File.chmod!(path, 0o700)
    on_exit(fn -> File.rm(path) end)
    path
  end
end
