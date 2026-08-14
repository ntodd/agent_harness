defmodule AgentHarness.Providers.Pi.SessionRedactionTest do
  use ExUnit.Case, async: true

  alias AgentHarness.Provider.Sink
  alias AgentHarness.Providers.Pi.{Config, Session}
  alias AgentHarness.{SessionConfig, SessionRef}

  @secret "sk-pi-super-secret-value"

  defp config do
    session = SessionRef.new(:pi, id: "pi-redaction-session")

    SessionConfig.new(session,
      env: %{"OPENROUTER_API_KEY" => @secret},
      provider_options: %{auth: :inherit, api_key: @secret}
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
      client: AgentHarness.Providers.Pi.ClientMock,
      transport: self(),
      sink: Sink.new(self())
    }
  end

  test "the unscrubbed state really carries the credentials" do
    config = config()
    state = state(config, prepared(config))

    rendered = inspect(state, structs: false, limit: :infinity, printable_limit: :infinity)

    assert rendered =~ @secret
  end

  defp erlang_format(term) do
    ~c"~p" |> :io_lib.format([term]) |> IO.iodata_to_binary()
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

  test "format_status redacts the --api-key argv value but keeps the rest" do
    config = config()
    %{state: scrubbed} = Session.format_status(%{state: state(config, prepared(config))})

    args = scrubbed.prepared.args

    refute @secret in args
    api_key_index = Enum.find_index(args, &(&1 == "--api-key"))
    assert Enum.at(args, api_key_index + 1) == "[REDACTED]"
    assert "--mode" in args
    assert "rpc" in args
  end

  test "format_status keeps env variable names for debugging" do
    config = config()
    %{state: scrubbed} = Session.format_status(%{state: state(config, prepared(config))})

    assert scrubbed.config.env == %{"OPENROUTER_API_KEY" => "[REDACTED]"}
    assert scrubbed.prepared.env == [{~c"OPENROUTER_API_KEY", ~c"[REDACTED]"}]
  end

  test "format_status scrubs the opening state" do
    config = config()
    prepared = prepared(config)
    sink = Sink.new(self())
    status = %{state: {:opening, config, prepared, sink, self(), self()}}

    %{state: {:opening, scrubbed_config, scrubbed_prepared, ^sink, _owner, _guardian}} =
      Session.format_status(status)

    refute erlang_format({scrubbed_config, scrubbed_prepared}) =~ @secret
    assert scrubbed_config.env == %{"OPENROUTER_API_KEY" => "[REDACTED]"}
  end

  test "format_status leaves the failed state and other status keys alone" do
    status = %{message: :hello, state: {:failed, :nope}}

    assert %{message: :hello, state: {:failed, :nope}} = Session.format_status(status)
  end

  test "format_status scrubs exec options, which can carry sandbox credentials" do
    session = SessionRef.new(:pi, id: "pi-redaction-exec")

    config =
      SessionConfig.new(session,
        provider_options: %{
          auth: :inherit,
          exec: {AgentHarness.ExecMock, sandbox_token: @secret}
        }
      )

    %{state: scrubbed} = Session.format_status(%{state: state(config, prepared(config))})

    refute erlang_format(scrubbed) =~ @secret
    assert {AgentHarness.ExecMock, [sandbox_token: "[REDACTED]"]} = scrubbed.prepared.exec
  end
end
