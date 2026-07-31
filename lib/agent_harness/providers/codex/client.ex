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
