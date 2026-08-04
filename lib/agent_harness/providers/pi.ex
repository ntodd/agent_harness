defmodule AgentHarness.Providers.Pi do
  @moduledoc """
  Pi provider backed by the locally installed `pi` CLI.

  The adapter drives `pi --mode rpc`, pi's JSONL command and event protocol over
  stdio, and maps it onto AgentHarness's provider-neutral lifecycle.

  Pi is deliberately smaller than the other supported harnesses. It has no
  permission system and no MCP support, so `approvals` and `per_session_mcp`
  are reported as `:unsupported` and a session configured with either is
  rejected rather than silently downgraded. Questions reach the harness through
  pi's extension UI sub-protocol, so they appear only when a loaded extension
  asks for input.

  Pi does support steering a running turn (`steer` and `follow_up` RPC
  commands), but AgentHarness has no public entry point for it yet, so the
  capability is reported as `:unsupported` rather than advertising something a
  caller cannot reach.

  Unlike Claude Code and Codex, pi is bring-your-own-model. The default
  `auth: :subscription` policy requires a credential established through pi's
  `/login` OAuth flow and rejects API-key routes. Set
  `provider_options: %{auth: :inherit}` to use an API key.
  """

  @behaviour AgentHarness.Provider

  alias AgentHarness.{Capabilities, Response, SessionConfig, Turn}
  alias AgentHarness.Provider.Sink
  alias AgentHarness.Providers.Pi.Session

  @impl true
  def open_session(%SessionConfig{} = config, %Sink{} = sink) do
    Session.start(config, sink, sink.pid)
  end

  @impl true
  def start_turn(server, %Turn{} = turn, input, opts) do
    case Session.start_turn(server, turn, input, opts) do
      {:error, {:provider_call_failed, _reason} = failure} ->
        {:error, {:turn_start_uncertain, failure}}

      result ->
        result
    end
  end

  @impl true
  def respond(server, provider_request_ref, %Response{} = response) do
    server
    |> Session.respond(provider_request_ref, response)
    |> normalize_provider_command_result()
  end

  @impl true
  def cancel(server, provider_turn_ref) do
    server
    |> Session.cancel(provider_turn_ref)
    |> normalize_provider_command_result()
  end

  @impl true
  @spec close_session(pid()) :: :ok | {:error, term()}
  def close_session(server), do: Session.close(server)

  @impl true
  def capabilities(_server) do
    Capabilities.new(
      token_streaming: :native,
      questions: :native,
      approvals: :unsupported,
      cancel: :native,
      steer: :unsupported,
      resume: :native,
      fork: :native,
      per_session_mcp: :unsupported,
      skills: :native
    )
  end

  defp normalize_provider_command_result({:error, {:provider_call_failed, _reason} = failure}) do
    {:error, {:provider_command_uncertain, failure}}
  end

  defp normalize_provider_command_result(result), do: result
end
