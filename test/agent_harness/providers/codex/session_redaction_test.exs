defmodule AgentHarness.Providers.Codex.SessionRedactionTest do
  use ExUnit.Case, async: true

  alias AgentHarness.Provider.Sink
  alias AgentHarness.Providers.Codex.{Config, Session}
  alias AgentHarness.{SessionConfig, SessionRef}

  @secret "sk-codex-super-secret-value"

  defp config do
    session = SessionRef.new(:codex, id: "codex-redaction-session")

    SessionConfig.new(session,
      env: %{"OPENAI_API_KEY" => @secret},
      provider_options: %{
        auth: :inherit,
        codex_options: %{api_key: @secret},
        connect_options: [init_timeout_ms: 1_500]
      }
    )
  end

  defp prepared(config) do
    {:ok, prepared} = Config.prepare(config)
    prepared
  end

  defp state(config, prepared) do
    %Session.State{
      owner: self(),
      owner_monitor: make_ref(),
      config: config,
      prepared: prepared,
      client: AgentHarness.Providers.Codex.ClientMock,
      # In production this field holds the resolved %Codex.Options{} struct,
      # which carries the api_key when auth is :inherit.
      codex_options: %Codex.Options{api_key: @secret},
      connection: self(),
      sink: Sink.new(self())
    }
  end

  defp erlang_format(term) do
    ~c"~p" |> :io_lib.format([term]) |> IO.iodata_to_binary()
  end

  test "the unscrubbed state really carries the credentials" do
    config = config()
    state = state(config, prepared(config))

    rendered = inspect(state, structs: false, limit: :infinity, printable_limit: :infinity)

    assert rendered =~ @secret
  end

  test "format_status scrubs the running state" do
    config = config()
    %{state: scrubbed} = Session.format_status(%{state: state(config, prepared(config))})

    # Crash reports render the raw term with Erlang ~p formatting, which
    # bypasses the Inspect protocol.
    refute erlang_format(scrubbed) =~ @secret

    refute inspect(scrubbed, structs: false, limit: :infinity, printable_limit: :infinity) =~
             @secret
  end

  test "format_status keeps option keys for debugging" do
    config = config()
    %{state: scrubbed} = Session.format_status(%{state: state(config, prepared(config))})

    assert scrubbed.config.env == %{"OPENAI_API_KEY" => "[REDACTED]"}
    assert scrubbed.prepared.auth == :inherit
    assert scrubbed.prepared.codex_options == %{api_key: "[REDACTED]"}
    assert scrubbed.prepared.connect_options[:process_env] == "[REDACTED]"
    assert scrubbed.prepared.connect_options[:client_name] == "[REDACTED]"
    assert scrubbed.codex_options == "[REDACTED]"
  end

  test "format_status scrubs the opening state" do
    sink = Sink.new(self())
    status = %{state: {:opening, config(), sink, self(), self()}}

    %{state: {:opening, scrubbed, ^sink, _owner, _guardian}} = Session.format_status(status)

    refute erlang_format(scrubbed) =~ @secret
    assert scrubbed.env == %{"OPENAI_API_KEY" => "[REDACTED]"}
  end

  test "format_status leaves other status keys alone" do
    status = %{message: :hello, state: {:opening, config(), Sink.new(self()), self(), self()}}

    assert %{message: :hello} = Session.format_status(status)
  end
end
