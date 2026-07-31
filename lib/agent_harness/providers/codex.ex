defmodule AgentHarness.Providers.Codex do
  @moduledoc """
  Provider adapter for a locally installed and authenticated Codex CLI.

  Each harness session owns an isolated `codex app-server` connection. The
  default `:subscription` auth mode requires a local ChatGPT-backed Codex
  profile and suppresses API-key environment overrides; use `:inherit` only
  when API or custom environment authentication is intentional.
  """

  @behaviour AgentHarness.Provider

  alias AgentHarness.{Capabilities, Response, SessionConfig, Turn}
  alias AgentHarness.Provider.Sink
  alias AgentHarness.Providers.Codex.Session

  @default_call_timeout 25_000

  @capabilities Capabilities.new(
                  token_streaming: :native,
                  questions: :native,
                  approvals: :native,
                  cancel: :native,
                  steer: :unsupported,
                  resume: :native,
                  fork: :unsupported,
                  per_session_mcp: :native,
                  skills: :native
                )

  @impl true
  def open_session(%SessionConfig{} = config, %Sink{} = sink) do
    Session.start(config, sink, sink.pid)
  end

  @impl true
  def start_turn(session, %Turn{} = turn, input, options) do
    call(session, {:start_turn, turn, input, options})
  end

  @impl true
  def respond(session, provider_request_ref, %Response{} = response) do
    call(session, {:respond, provider_request_ref, response})
  end

  @impl true
  def cancel(session, provider_turn_ref) do
    call(session, {:cancel, provider_turn_ref})
  end

  @impl true
  def close_session(session) do
    case call(session, :close) do
      {:error, :provider_not_found} -> :ok
      result -> result
    end
  end

  @impl true
  def capabilities(_session), do: @capabilities

  defp call(session, message) when is_pid(session) do
    GenServer.call(session, message, call_timeout())
  catch
    :exit, {:noproc, _details} -> {:error, :provider_not_found}
    :exit, {:normal, _details} -> {:error, :provider_not_found}
    :exit, {:timeout, _details} -> {:error, :provider_call_timeout}
    :exit, reason -> {:error, {:provider_call_failed, reason}}
  end

  defp call_timeout do
    Application.get_env(:agent_harness, :codex_call_timeout, @default_call_timeout)
  end
end
