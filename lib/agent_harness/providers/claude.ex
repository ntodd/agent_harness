defmodule AgentHarness.Providers.Claude do
  @moduledoc """
  Claude Code provider backed by the locally installed, authenticated CLI.

  The adapter uses `claude_code` for its bidirectional stream protocol while
  retaining AgentHarness's provider-neutral lifecycle and request events.
  """

  @behaviour AgentHarness.Provider

  alias AgentHarness.{Capabilities, Response, SessionConfig, Turn}
  alias AgentHarness.Provider.Sink
  alias AgentHarness.Providers.Claude.Server

  @impl true
  def open_session(%SessionConfig{} = config, %Sink{} = sink) do
    case DynamicSupervisor.start_child(
           AgentHarness.ProviderSupervisor,
           {Server, config: config, sink: sink}
         ) do
      {:ok, server} ->
        {:ok, server, %{provider_session_id: Server.provider_session_id(server)}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def start_turn(server, %Turn{} = turn, input, opts) do
    Server.start_turn(server, turn, input, opts)
  end

  @impl true
  def respond(server, provider_request_ref, %Response{} = response) do
    Server.respond(server, provider_request_ref, response)
  end

  @impl true
  def cancel(server, provider_turn_ref) do
    Server.cancel(server, provider_turn_ref)
  end

  @impl true
  def close_session(server) do
    Server.close(server)
  end

  @impl true
  def capabilities(_server) do
    Capabilities.new(
      token_streaming: :native,
      questions: :native,
      approvals: :native,
      cancel: :native,
      steer: :unsupported,
      resume: :native,
      fork: :native,
      per_session_mcp: :native,
      skills: :emulated
    )
  end
end
