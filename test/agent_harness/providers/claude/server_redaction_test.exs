defmodule AgentHarness.Providers.Claude.ServerRedactionTest do
  use ExUnit.Case, async: true

  alias AgentHarness.Provider.Sink
  alias AgentHarness.Providers.Claude.Server
  alias AgentHarness.{SessionConfig, SessionRef}

  @secret "sk-ant-api03-super-secret-value"

  defp config do
    session = SessionRef.new(:claude, id: "server-redaction-session")

    SessionConfig.new(session,
      env: %{"ANTHROPIC_API_KEY" => @secret},
      provider_options: %{auth: :inherit, api_key: @secret}
    )
  end

  test "format_status scrubs the opening-state config" do
    sink = Sink.new(self())
    status = %{state: {:opening, config(), sink, self()}}

    %{state: {:opening, scrubbed, ^sink, _guardian}} = Server.format_status(status)

    refute inspect(scrubbed, limit: :infinity, printable_limit: :infinity) =~ @secret
    assert scrubbed.env == %{"ANTHROPIC_API_KEY" => "[REDACTED]"}
  end

  test "format_status scrubs the running-state config" do
    state = %Server.State{
      config: config(),
      sink: Sink.new(self()),
      client: AgentHarness.Providers.Claude.ClientMock,
      client_session: self(),
      sink_monitor: make_ref(),
      client_monitor: make_ref(),
      question_timeout: :infinity
    }

    %{state: scrubbed_state} = Server.format_status(%{state: state})

    rendered = inspect(scrubbed_state, limit: :infinity, printable_limit: :infinity)

    refute rendered =~ @secret
    assert scrubbed_state.config.env == %{"ANTHROPIC_API_KEY" => "[REDACTED]"}
  end

  test "format_status leaves other status keys alone" do
    status = %{message: :hello, state: {:opening, config(), Sink.new(self()), self()}}

    assert %{message: :hello} = Server.format_status(status)
  end
end
