# Writing an Exec implementation

`AgentHarness.Exec` is the behaviour that decides *where* a provider's CLI
runs. The provider adapters describe *what* to run — an argv, environment
entries, a working directory — and stream the provider's protocol over the
resulting byte stream. `AgentHarness.Exec.Local` runs the command as a local
OS process; an application-provided implementation can run it anywhere a
byte stream can reach: a sandbox vendor, an SSH channel, a container runtime.

One implementation covers all three providers, because the contract is
byte-level and protocol-free. This guide walks through the contract and then
through a real implementation against [E2B](https://e2b.dev) sandboxes,
condensed from a production adapter.

The behaviour documentation on `AgentHarness.Exec` is the normative
contract. This guide is the narrative version: the shape that works, and the
invariants that bite when missed.

## The contract in brief

An implementation exports three callbacks:

```elixir
@callback start(spec, owner :: pid(), opts :: keyword()) ::
            {:ok, handle} | {:error, term()}
@callback write(handle, iodata()) :: :ok | {:error, term()}
@callback kill(handle) :: :ok
```

`start/3` receives a spec map:

```elixir
%{
  cmd: ["claude", "--output-format", "stream-json", ...],
  env: %{"ANTHROPIC_API_KEY" => "...", "CLAUDECODE" => false},
  cwd: "/workspace",
  stderr: :passthrough | :merged
}
```

and delivers two kinds of messages to `owner`:

- `{:exec_data, handle, binary}` — output bytes in arrival order. Chunk
  boundaries carry no meaning; the provider adapters do their own line
  reassembly.
- `{:exec_exit, handle, reason}` — exactly once, after which no further
  `:exec_data` for that handle arrives.

Everything in the spec is interpreted **where the command runs**. The
executable resolves against the execution environment's PATH, `cwd` is a
directory inside that environment, and `env` layers over that environment's
own variables. Nothing refers to the orchestrator's filesystem.

## The shape that works

Both `Exec.Local` and the E2B adapter below use the same skeleton: one
GenServer per command, with the GenServer's pid as the handle.

The pid-as-handle choice earns its keep twice. The provider adapters monitor
pid handles natively, so transport loss becomes an ordinary `:DOWN` without
extra plumbing. And the process gives every callback one mailbox to
serialize through, which is what makes exactly-once exit delivery easy to
get right.

Start it with `GenServer.start`, not `start_link`. The contract owns the
lifecycle through two mechanisms — the owner monitor and `kill/1` — and a
link to the caller would add a third that fights the other two.

```elixir
defmodule MyApp.Sandbox.Exec do
  @behaviour AgentHarness.Exec

  use GenServer

  @impl AgentHarness.Exec
  def start(spec, owner, opts) do
    case Keyword.fetch(opts, :sandbox) do
      {:ok, sandbox} -> GenServer.start(__MODULE__, {spec, owner, sandbox})
      _missing -> {:error, :missing_sandbox}
    end
  end

  @impl GenServer
  def init({spec, owner, sandbox}) do
    Process.flag(:trap_exit, true)

    state = %{
      spec: spec,
      owner: owner,
      owner_monitor: Process.monitor(owner),
      sandbox: sandbox,
      remote_pid: nil,
      pending_writes: [],
      exited?: false
    }

    {:ok, state, {:continue, :start_stream}}
  end
end
```

The `opts` keyword is yours: it is whatever the application passed in
`exec: {MyApp.Sandbox.Exec, opts}`. This is where the sandbox reference,
connection pool, or credentials ride in.

## Streaming output without blocking the mailbox

The E2B adapter talks to `envd`, the daemon inside each sandbox, over
Connect RPC. Spawning is a server-streaming `process.Process/Start` call:
one long-lived HTTP request whose response stream carries start, stdout,
stderr, and exit events for the lifetime of the command.

That request must not run in the GenServer's own callbacks, or `write/2`
and `kill/1` would block behind it. Hold the stream in a task and forward
chunks back:

```elixir
@impl GenServer
def handle_continue(:start_stream, state) do
  server = self()
  task = Task.async(fn -> stream(server, state.sandbox, state.spec) end)
  {:noreply, %{state | stream_task: task}}
end

defp stream(server, sandbox, spec) do
  request =
    envd_request(sandbox,
      url: "/process.Process/Start",
      body: encode_start_request(spec),
      into: fn {:data, chunk}, acc ->
        send(server, {:stream_chunk, chunk})
        {:cont, acc}
      end
    )

  case Req.request(request) do
    {:ok, %Req.Response{status: 200}} -> :ok
    {:ok, %Req.Response{status: status}} -> {:http_error, status}
    {:error, error} -> {:transport_error, error}
  end
end
```

The GenServer decodes the vendor's framing out of `{:stream_chunk, chunk}`
messages and maps events onto the contract:

- a `stdout` event becomes `send(state.owner, {:exec_data, self(), data})`;
- an `exit` event becomes the exactly-once
  `{:exec_exit, self(), {:exit_status, code}}`;
- the stream ending *without* an exit event is transport loss, reported as
  an implementation-specific reason such as `:stream_closed`.

That last distinction matters. The provider adapters treat any
`{:exec_exit, ...}` as the end of the session, but the *reason* tells the
operator whether the CLI finished or the transport dropped, and those have
different remediations.

## Translating the spec

Three details of spec translation are easy to miss.

**Executable resolution belongs to the sandbox.** The spec's `cmd` head is
usually a bare name like `"claude"` or `"pi"`, expected to resolve against
the PATH where the command runs. If the vendor API execs directly without a
shell, resolve through a login shell while passing the argv through
untouched:

```elixir
[executable | args] = spec.cmd

%{
  cmd: "/bin/bash",
  args: ["-lc", ~S(exec "$0" "$@"), executable | args],
  envs: envs
}
```

`exec "$0" "$@"` gives the command the sandbox's login PATH (where the
template installed node and the CLI) without shell-quoting any of the
adapter's arguments.

**A `false` env value means unset.** The Claude adapter, for example, sends
`"CLAUDECODE" => false` to keep the CLI from detecting a nested session.
A fresh sandbox has nothing to unset, so dropping the entry is equivalent.
Never stringify it: serializing `false` as `"false"` *sets* the variable.

```elixir
envs =
  for {key, value} <- spec.env, value != false, into: %{} do
    {key, to_string(value)}
  end
```

**stderr `:passthrough` means "not in the stream", not "discarded".** The
Pi and Codex adapters require stderr out of the stream because their
protocols are bare JSONL on stdout. Where it surfaces is up to you; logging
it at debug level preserves the diagnostics without corrupting the frames:

```elixir
defp handle_event({:stderr, data}, state) do
  if Map.get(state.spec, :stderr, :passthrough) == :merged do
    send(state.owner, {:exec_data, self(), data})
  else
    Logger.debug("sandbox exec stderr: #{inspect(data)}")
  end

  {:cont, state}
end
```

## Writes that arrive before the process does

Every provider adapter sends its protocol handshake immediately after
`start/3` returns — before a remote API can possibly have reported the
spawned process's identity. If your vendor needs a remote pid to address
stdin (E2B's `SendInput` does), queue early writes and flush them in order
once the start event lands:

```elixir
def handle_call({:write, data}, _from, %{remote_pid: nil} = state) do
  {:reply, :ok, %{state | pending_writes: state.pending_writes ++ [data]}}
end

def handle_call({:write, data}, _from, state) do
  {:reply, send_input(state, data), state}
end
```

Give `write/2` a call timeout that clears the slowest thing inside it. The
E2B adapter's stdin write is an HTTP request with a 15-second receive
timeout, so the GenServer call allows 25; with `GenServer.call`'s default
5 seconds, a slow but working write would be misreported as
`{:error, :closed}`, and the provider adapter would retire the session over
a transport that was fine.

## Kill must reach the process, not just the stream

This is the sharpest edge in the contract. On most sandbox vendors,
dropping the output stream does **not** stop the remote command. E2B's envd
detaches it deliberately. A CLI that keeps running after the orchestrator
loses track of it keeps consuming tokens until the sandbox TTL reaps it.

So `kill/1` sends a real signal through the vendor API, best-effort, and
then reports `:killed`:

```elixir
def handle_call(:kill, _from, state) do
  state = signal_remote(state)   # SIGKILL via the vendor API
  {:stop, :normal, :ok, deliver_exit(state, :killed)}
end
```

And `terminate/2` is the catch-all for every path that did not go through
`kill/1` — a dropped stream, an HTTP rejection, a protocol error, the owner
dying:

```elixir
@impl GenServer
def terminate(_reason, state) do
  if not state.remote_exited?, do: signal_remote(state)
  if state.stream_task, do: Task.shutdown(state.stream_task, :brutal_kill)
  :ok
end
```

Keep two flags rather than one: whether the *owner has been told* the exec
exited, and whether the *remote command is over*. Conflating them leaks a
running CLI — a stream can drop (owner told, exit delivered) while the
command is still alive and billing.

The exactly-once exit delivery falls out of the first flag:

```elixir
defp deliver_exit(%{exited?: true} = state, _reason), do: state

defp deliver_exit(state, reason) do
  send(state.owner, {:exec_exit, self(), reason})
  %{state | exited?: true}
end
```

Finally, make `kill/1` survive a wedged GenServer, the same escalation
`Exec.Local` makes:

```elixir
def kill(handle) when is_pid(handle) do
  GenServer.call(handle, :kill, @call_timeout)
catch
  :exit, {:timeout, _call} ->
    Process.exit(handle, :kill)
    :ok

  :exit, _reason ->
    :ok
end
```

`:kill` is untrappable, so the process and its stream die even when the
mailbox is stuck. The remote command is then bounded by the sandbox TTL,
which is the backstop the whole design leans on. Set one.

## The owner monitor

The contract requires monitoring `owner` and releasing resources if it
dies. For a remote implementation, "releasing resources" includes the
remote command:

```elixir
def handle_info({:DOWN, ref, :process, _pid, _reason}, %{owner_monitor: ref} = state) do
  {:stop, :normal, %{signal_remote(state) | exited?: true}}
end
```

Nobody is left to report to, so no exit message is sent; but the command
would keep running (and billing) without the signal.

## Redaction

The spec's `env` carries the session's credentials (`ANTHROPIC_API_KEY` and
friends), and your `opts` likely carry a sandbox token. Both land in your
GenServer state, and crash reports render state with Erlang `~p`
formatting, which bypasses the `Inspect` protocol. Scrub both paths:

```elixir
@impl GenServer
def format_status(status) do
  Map.new(status, fn
    {:state, %__MODULE__{} = state} -> {:state, redact(state)}
    other -> other
  end)
end

def redact(%__MODULE__{} = state) do
  %{state | spec: Map.put(state.spec, :env, Map.new(state.spec.env, fn {k, _} -> {k, "[REDACTED]"} end))}
end

defimpl Inspect do
  def inspect(state, opts) do
    state |> MyApp.Sandbox.Exec.redact() |> Inspect.Any.inspect(opts)
  end
end
```

This mirrors what the provider adapters do with their own state; see
[Credentials in logs](configuration.md#credentials-in-logs).

## Wiring it into the providers

Pi and Codex take the implementation directly:

```elixir
provider_options: %{
  auth: :inherit,
  exec: {MyApp.Sandbox.Exec, sandbox: sandbox}
}
```

Claude selects it through the `claude_code` adapter option:

```elixir
provider_options: %{
  auth: :inherit,
  adapter: {AgentHarness.Providers.Claude.Adapter.Exec,
            exec: {MyApp.Sandbox.Exec, sandbox: sandbox}}
}
```

All three require `auth: :inherit`, so the sandbox needs credentials: either
API keys in the session `env` (they travel in the spec) or provider state
baked into the sandbox template.

## Testing an implementation

Unit-test the contract mechanics against a stub of your vendor API: the
exactly-once exit, the pending-write flush order, kill-then-exit, owner
death, and the stderr modes. The message shapes make this natural — a test
process can be the owner and assert on `{:exec_data, ...}` and
`{:exec_exit, ...}` directly.

Then validate end to end with a real session. The cheapest full-stack check
runs a provider through your implementation with a trivial prompt:

```elixir
{:ok, session} =
  AgentHarness.start_session(:pi,
    cwd: "/workspace",
    provider_options: %{auth: :inherit, exec: {MyApp.Sandbox.Exec, sandbox: sandbox}}
  )

{:ok, turn} = AgentHarness.start_turn(session, "Reply with exactly OK and nothing else.")
{:ok, %{status: :completed}} = AgentHarness.await(turn, timeout: 120_000)
```

Tag anything that reaches a real sandbox or a real model `:live`, following
the conventions in [Testing](testing.md). AgentHarness's own exec live
tests (`*_exec_live_test.exs` under `test/agent_harness/providers/`) are
the reference shape: they run each provider through `Exec.Local`, and your
implementation should pass the same scenario with only the exec tuple
changed.
