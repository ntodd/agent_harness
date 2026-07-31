defmodule AgentHarness.ConcurrencyTest do
  use ExUnit.Case, async: false

  alias AgentHarness.{Capabilities, SessionRef, Turn}

  defmodule SlowOpenProvider do
    @behaviour AgentHarness.Provider

    @impl true
    def open_session(config, _sink) do
      Process.sleep(config.provider_options.delay)
      {:ok, {:slow_open, config.session_id}, %{}}
    end

    @impl true
    def start_turn(_handle, %Turn{} = turn, _input, _opts), do: {:ok, turn.id}

    @impl true
    def respond(_handle, _provider_ref, _response), do: :ok

    @impl true
    def cancel(_handle, _provider_ref), do: :ok

    @impl true
    def close_session(_handle), do: :ok

    @impl true
    def capabilities(_handle), do: Capabilities.new()
  end

  defmodule BlockingTurnProvider do
    @behaviour AgentHarness.Provider

    @impl true
    def open_session(config, sink) do
      {:ok, %{config: config, sink: sink, owner: self()}, %{}}
    end

    @impl true
    def start_turn(handle, %Turn{} = turn, _input, opts) do
      turn_id = turn.id
      send(opts[:test_pid], {:provider_start_entered, turn_id, self(), handle.owner})

      receive do
        {:release_turn, ^turn_id} -> {:ok, turn_id}
      end
    end

    @impl true
    def respond(_handle, _provider_ref, _response), do: :ok

    @impl true
    def cancel(_handle, _provider_ref), do: :ok

    @impl true
    def close_session(_handle), do: :ok

    @impl true
    def capabilities(_handle), do: Capabilities.new(cancel: :native)
  end

  defmodule SlowClaudeClient do
    @behaviour AgentHarness.Providers.Claude.Client

    @impl true
    def verify_subscription_auth(_opts, _timeout), do: :ok

    @impl true
    def start_link(_opts), do: Task.start_link(fn -> Process.sleep(:infinity) end)

    @impl true
    def await_ready(_session, _timeout) do
      Process.sleep(100)
      :ok
    end

    @impl true
    def stream(_session, _prompt, _opts), do: []

    @impl true
    def session_id(_session), do: nil

    @impl true
    def interrupt(_session), do: :ok

    @impl true
    def stop(session) do
      Process.exit(session, :shutdown)
      :ok
    end
  end

  defmodule SlowCodexClient do
    @behaviour AgentHarness.Providers.Codex.Client

    @impl true
    def options(options), do: {:ok, options}

    @impl true
    def connect(_options, _connect_options) do
      Process.sleep(100)
      Task.start(fn -> Process.sleep(:infinity) end)
    end

    @impl true
    def alive?(connection), do: Process.alive?(connection)

    @impl true
    def disconnect(connection) do
      Process.exit(connection, :shutdown)
      :ok
    end

    @impl true
    def start_thread(_options, _thread_options), do: {:ok, :thread}

    @impl true
    def resume_thread(_thread_id, _options, _thread_options), do: {:ok, :thread}

    @impl true
    def run_streamed(_thread, _input, _turn_options), do: {:ok, []}

    @impl true
    def raw_events(streaming), do: streaming

    @impl true
    def respond(_connection, _id, _payload), do: :ok

    @impl true
    def turn_interrupt(_connection, _thread_id, _turn_id), do: :ok

    @impl true
    def cancel_stream(_streaming, _mode), do: :ok
  end

  test "provider handshakes for independent sessions run concurrently" do
    delay = 100
    started_at = System.monotonic_time(:millisecond)

    results =
      1..6
      |> Task.async_stream(
        fn index ->
          AgentHarness.start_session(:slow,
            id: "parallel-open-#{index}",
            provider_module: SlowOpenProvider,
            provider_options: %{delay: delay}
          )
        end,
        max_concurrency: 6,
        ordered: false,
        timeout: 2_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    elapsed = System.monotonic_time(:millisecond) - started_at

    assert Enum.all?(results, &match?({:ok, %SessionRef{}}, &1))
    assert elapsed < 400

    Enum.each(results, fn {:ok, session} ->
      assert :ok = AgentHarness.stop_session(session)
    end)
  end

  test "a provider open that exceeds startup_timeout is removed without blocking other starts" do
    slow =
      Task.async(fn ->
        AgentHarness.start_session(:slow,
          id: "timed-out-open",
          provider_module: SlowOpenProvider,
          provider_options: %{delay: 500},
          startup_timeout: 20
        )
      end)

    assert {:ok, session} =
             AgentHarness.start_session(:slow,
               id: "fast-open",
               provider_module: SlowOpenProvider,
               provider_options: %{delay: 1}
             )

    assert {:error, :session_start_timeout} = Task.await(slow, 1_000)
    assert AgentHarness.whereis("timed-out-open") == nil
    assert :ok = AgentHarness.stop_session(session)
  end

  test "Claude runtime readiness does not serialize ProviderSupervisor" do
    assert_provider_runtime_opens_in_parallel(:claude,
      auth: :inherit,
      client: SlowClaudeClient
    )
  end

  test "Codex runtime connection does not serialize ProviderSupervisor" do
    assert_provider_runtime_opens_in_parallel(:codex,
      auth: :inherit,
      client: SlowCodexClient
    )
  end

  test "start_turn returns its stable handle while provider admission is still running" do
    assert {:ok, session} =
             AgentHarness.start_session(:blocking,
               provider_module: BlockingTurnProvider,
               turn_start_timeout: 1_000
             )

    started_at = System.monotonic_time(:millisecond)

    assert {:ok, %Turn{status: :starting} = turn} =
             AgentHarness.start_turn(session, "work", test_pid: self())

    assert System.monotonic_time(:millisecond) - started_at < 100
    assert_receive {:provider_start_entered, turn_id, provider_caller, open_caller}
    assert turn_id == turn.id
    refute provider_caller == AgentHarness.whereis(session.id)
    refute open_caller == AgentHarness.whereis(session.id)

    assert %{status: :starting, current_turn: %{id: ^turn_id}} =
             AgentHarness.status(session)

    assert :ok = AgentHarness.cancel(turn)

    assert %{status: :cancelling, current_turn: %{id: ^turn_id}} =
             AgentHarness.status(session)

    send(provider_caller, {:release_turn, turn.id})
    assert :ok = AgentHarness.stop_session(session, force: true)
  end

  defp assert_provider_runtime_opens_in_parallel(provider, provider_options) do
    prefix = "parallel-#{provider}-#{System.unique_integer([:positive])}"
    started_at = System.monotonic_time(:millisecond)

    sessions =
      1..5
      |> Task.async_stream(
        fn index ->
          AgentHarness.start_session(provider,
            id: "#{prefix}-#{index}",
            provider_options: Map.new(provider_options)
          )
        end,
        max_concurrency: 5,
        ordered: false,
        timeout: 2_000
      )
      |> Enum.map(fn {:ok, {:ok, session}} -> session end)

    elapsed = System.monotonic_time(:millisecond) - started_at
    assert elapsed < 400
    Enum.each(sessions, &AgentHarness.stop_session/1)
  end
end
