defmodule AgentHarness.Providers.Codex.Client do
  @moduledoc false

  @type connection :: term()
  @type options :: term()
  @type thread :: term()
  @type streaming :: term()

  @callback options(map() | keyword()) :: {:ok, options()} | {:error, term()}
  @callback connect(options(), keyword()) :: {:ok, connection()} | {:error, term()}
  @callback alive?(connection()) :: boolean()
  @callback disconnect(connection()) :: :ok
  @callback start_thread(options(), map()) :: {:ok, thread()} | {:error, term()}
  @callback resume_thread(String.t(), options(), map()) ::
              {:ok, thread()} | {:error, term()}
  @callback run_streamed(thread(), String.t() | [map()], map()) ::
              {:ok, streaming()} | {:error, term()}
  @callback raw_events(streaming()) :: Enumerable.t()
  @callback respond(connection(), String.t() | integer(), map()) :: :ok | {:error, term()}
  @callback turn_interrupt(connection(), String.t(), String.t()) :: :ok | {:error, term()}
  @callback cancel_stream(streaming(), :immediate | :after_turn) :: :ok
end

defmodule AgentHarness.Providers.Codex.Client.SDK do
  @moduledoc false

  @behaviour AgentHarness.Providers.Codex.Client

  alias AgentHarness.Providers.Codex.ConnectionProxy

  @impl true
  def options(attrs), do: Codex.Options.new(attrs)

  @impl true
  def connect(options, opts) do
    with {:ok, connection} <- Codex.AppServer.connect(options, opts) do
      case ConnectionProxy.start(connection, owner: self()) do
        {:ok, proxy} ->
          {:ok, proxy}

        {:error, reason} ->
          _ = Codex.AppServer.disconnect(connection)
          {:error, reason}
      end
    end
  end

  @impl true
  def alive?(connection), do: Process.alive?(connection)

  @impl true
  def disconnect(connection), do: ConnectionProxy.disconnect(connection)

  @impl true
  def start_thread(options, thread_options), do: Codex.start_thread(options, thread_options)

  @impl true
  def resume_thread(thread_id, options, thread_options),
    do: Codex.resume_thread(thread_id, options, thread_options)

  @impl true
  def run_streamed(thread, input, turn_options) do
    stream =
      Stream.flat_map([:start], fn :start ->
        case Codex.Thread.run_turn_streamed(thread, input, turn_options) do
          {:ok, events} -> events
          {:error, reason} -> throw({:codex_stream_start_failed, reason})
        end
      end)

    {:ok, stream}
  end

  @impl true
  def raw_events(streaming), do: streaming

  @impl true
  def respond(connection, id, payload), do: Codex.AppServer.respond(connection, id, payload)

  @impl true
  def turn_interrupt(connection, thread_id, turn_id),
    do: Codex.AppServer.turn_interrupt(connection, thread_id, turn_id)

  @impl true
  def cancel_stream(_streaming, _mode), do: :ok
end

defmodule AgentHarness.Providers.Codex.Client.Exec do
  @moduledoc """
  Codex client that runs `codex app-server` through an `AgentHarness.Exec`
  implementation instead of the SDK's subprocess transport.

  Only `connect/2` differs from `Client.SDK`: the app-server is spawned via
  `AgentHarness.Providers.Codex.ExecConnection`, whose pid satisfies the same
  call contract as the SDK's own connections, so threads, turns, approvals,
  and interrupts flow through the ordinary SDK code paths.

  Select it per session with `auth: :inherit` (the fail-closed
  `:subscription` mode inspects local Codex state by design):

      provider_options: %{
        auth: :inherit,
        exec: {MyApp.SandboxExec, sandbox: sandbox}
      }

  The spawn spec is remote-safe: the executable (`codex` or the configured
  `codex_path`) resolves in the execution environment, and only explicit
  entries are forwarded — the SDK credential/base-url overrides and the
  session's `process_env`. Model routing config (`model_payload`) is not
  translated into app-server flags; set the model per thread instead.
  """

  @behaviour AgentHarness.Providers.Codex.Client

  alias AgentHarness.Providers.Codex.Client.SDK
  alias AgentHarness.Providers.Codex.ConnectionProxy
  alias AgentHarness.Providers.Codex.ExecConnection
  alias Codex.AppServer.Connection
  alias Codex.Runtime.Env, as: RuntimeEnv

  @default_executable "codex"
  @default_init_timeout_ms 30_000

  @impl true
  def options(attrs), do: SDK.options(attrs)

  @impl true
  def connect(%Codex.Options{} = options, opts) when is_list(opts) do
    init_timeout_ms = Keyword.get(opts, :init_timeout_ms, @default_init_timeout_ms)

    with {:ok, env} <- build_env(options, opts),
         {:ok, conn} <- start_connection(options, opts, env),
         :ok <- await_ready(conn, init_timeout_ms) do
      case ConnectionProxy.start(conn, owner: self(), disconnect: &ExecConnection.stop/1) do
        {:ok, proxy} ->
          {:ok, proxy}

        {:error, reason} ->
          ExecConnection.stop(conn)
          {:error, reason}
      end
    end
  end

  @impl true
  def alive?(connection), do: SDK.alive?(connection)

  @impl true
  def disconnect(connection), do: SDK.disconnect(connection)

  @impl true
  def start_thread(options, thread_options), do: SDK.start_thread(options, thread_options)

  @impl true
  def resume_thread(thread_id, options, thread_options),
    do: SDK.resume_thread(thread_id, options, thread_options)

  @impl true
  def run_streamed(thread, input, turn_options), do: SDK.run_streamed(thread, input, turn_options)

  @impl true
  def raw_events(streaming), do: SDK.raw_events(streaming)

  @impl true
  def respond(connection, id, payload), do: SDK.respond(connection, id, payload)

  @impl true
  def turn_interrupt(connection, thread_id, turn_id),
    do: SDK.turn_interrupt(connection, thread_id, turn_id)

  @impl true
  def cancel_stream(streaming, mode), do: SDK.cancel_stream(streaming, mode)

  defp start_connection(options, opts, env) do
    spec = %{
      cmd: [options.codex_path_override || @default_executable, "app-server"],
      env: env,
      cwd: Keyword.get(opts, :cwd),
      stderr: :passthrough
    }

    ExecConnection.start(spec,
      exec: Keyword.get(opts, :exec),
      owner: self(),
      init_timeout_ms: Keyword.get(opts, :init_timeout_ms, @default_init_timeout_ms),
      client_name: Keyword.get(opts, :client_name, "agent_harness"),
      client_title: Keyword.get(opts, :client_title),
      client_version: Keyword.get(opts, :client_version),
      experimental_api: Keyword.get(opts, :experimental_api, false)
    )
  end

  # The call deadline gets slack over the connection's internal init timer so
  # a handshake failure reports the descriptive {:init_timeout, ms} instead of
  # the caller's generic :timeout winning the race.
  defp await_ready(conn, init_timeout_ms) do
    case Connection.await_ready(conn, init_timeout_ms + 1_000) do
      :ok ->
        :ok

      {:error, reason} ->
        ExecConnection.stop(conn)
        {:error, reason}
    end
  end

  # Mirrors Codex.AppServer.Connection.build_env for the ungoverned case:
  # credential/base-url overrides layered under the explicit process_env.
  # Nothing else is forwarded; the execution environment's own environment
  # remains the base.
  defp build_env(options, opts) do
    process_env = Keyword.get(opts, :process_env, Keyword.get(opts, :env, %{}))

    with {:ok, custom_env} <- RuntimeEnv.normalize_overrides(process_env) do
      options.api_key
      |> RuntimeEnv.base_overrides(options.base_url)
      |> Map.merge(custom_env, fn _key, _base, custom -> custom end)
      |> then(&{:ok, &1})
    end
  end
end
