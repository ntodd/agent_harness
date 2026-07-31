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
        case Server.provider_session_id(server, config.startup_timeout) do
          {:ok, provider_session_id} ->
            {:ok, server, %{provider_session_id: provider_session_id}}

          {:error, reason} ->
            _ = DynamicSupervisor.terminate_child(AgentHarness.ProviderSupervisor, server)
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def start_turn(server, %Turn{} = turn, input, opts) do
    case Server.start_turn(server, turn, input, opts) do
      {:error, {:provider_call_failed, _reason} = failure} ->
        {:error, {:turn_start_uncertain, failure}}

      result ->
        result
    end
  end

  @impl true
  def respond(server, provider_request_ref, %Response{} = response) do
    server
    |> Server.respond(provider_request_ref, response)
    |> normalize_provider_command_result()
  end

  @impl true
  def cancel(server, provider_turn_ref) do
    server
    |> Server.cancel(provider_turn_ref)
    |> normalize_provider_command_result()
  end

  @impl true
  @spec close_session(pid()) :: :ok | {:error, term()}
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

  defp normalize_provider_command_result({:error, {:provider_call_failed, _reason} = failure}) do
    {:error, {:provider_command_uncertain, failure}}
  end

  defp normalize_provider_command_result(result), do: result
end
