# Architecture and concurrency

AgentHarness models coding-agent conversations as OTP processes. Its goal is
to make lifecycle, interaction, and streaming predictable without pretending
stateful agent sessions are interchangeable pooled workers.

## Supervision tree

The application starts:

```text
AgentHarness.Supervisor (:rest_for_one)
├── AgentHarness.Store.Memory
├── AgentHarness.SessionRegistry
├── AgentHarness.SessionSupervisor
├── AgentHarness.ProviderSupervisor
└── AgentHarness.RunnerSupervisor
```

Responsibilities:

- **Store.Memory** owns default ephemeral snapshots and event journals.
- **SessionRegistry** maps stable session IDs to live SessionServer PIDs.
- **SessionSupervisor** dynamically supervises logical SessionServers.
- **ProviderSupervisor** dynamically supervises provider runtime processes.
- **RunnerSupervisor** owns temporary stream-consumer tasks.

The root uses `:rest_for_one` because downstream sessions must not continue
against a newly empty Memory Store or Registry after one of those foundational
processes fails.

Session and provider children are currently temporary. Supervision supplies
ownership, isolation, shutdown, and cleanup, but v0.1 does not automatically
restart and rehydrate a crashed coding session.

## One session, one conversation

`start_session/2` creates one SessionServer with:

- one stable AgentHarness session ID;
- one provider and provider session/thread ID;
- one working directory and immutable session configuration;
- at most one active turn;
- pending requests;
- event sequence and subscription state;
- a provider runtime handle.

The public `%AgentHarness.SessionRef{}` contains no PID. Calls resolve it through
the Registry:

```text
SessionRef ID → Registry → SessionServer
```

Provider adapters receive an authenticated sink containing the SessionServer
PID and a private reference. The SessionServer ignores events bearing an old
sink reference or a noncurrent turn ID, preventing a replaced or delayed
provider runtime from corrupting current state.

## Provider runtime ownership

Provider adapters hide their concrete process model:

- Codex uses an app-server connection and a thread.
- Claude uses a bidirectional CLI session plus a temporary task that consumes
  each turn's stream.

Provider runtimes live under `ProviderSupervisor`; streaming tasks live under
`RunnerSupervisor`. The logical SessionServer does not execute a blocking CLI
read in its own callbacks.

Provider stream readers send normalized messages through
`AgentHarness.Provider.Sink`. SessionServer assigns the final event sequence,
persists the event, updates its replay buffer, and delivers it to subscribers.

## Why there is no Poolboy

A coding-agent session is not a fungible resource:

- it owns conversation history;
- it may have a different repository and working tree;
- it may use different MCP servers and skills;
- it may have pending questions or permissions;
- it carries a provider session/thread ID;
- a checkout could last minutes or hours.

Checking such a process out of a generic worker pool would obscure the very
identity and lifecycle AgentHarness needs to preserve.

AgentHarness therefore does not use Poolboy or NimblePool. It also does not
impose a mandatory global scheduler. The caller decides which sessions to
create and when to start their turns.

## What concurrency means

Within one session, commands are serialized by a GenServer and only one turn
may be active.

Across sessions, turns are independent:

```elixir
sessions
|> Task.async_stream(
  fn session ->
    with {:ok, turn} <- AgentHarness.start_turn(session, "Run the assigned work") do
      AgentHarness.await(turn, timeout: 600_000)
    end
  end,
  max_concurrency: 8,
  timeout: :infinity
)
|> Enum.to_list()
```

The `max_concurrency` above is caller policy, not an AgentHarness requirement.
Without it, starting turns on many sessions asks all providers to run
concurrently.

There is intentionally no hard-coded limit such as ten. The practical limit
depends on:

- provider plan and rate limits;
- process memory and file descriptors;
- command and MCP subprocesses;
- model/subagent fan-out inside each CLI;
- repository size and filesystem activity;
- network and CPU capacity.

Provider rate limits and resource failures surface as provider events or
terminal failures.

## Logical sessions versus resident CLIs

In the current v0.1 implementation, `start_session/2` opens the provider runtime
immediately. Creating 1,000 sessions can therefore create a very large number
of resident CLI/app-server processes even if they have no active turn.

The BEAM can hold 1,000 lightweight GenServers comfortably; 1,000 coding-agent
CLI trees are a different resource problem. Each CLI may start commands, MCP
servers, language servers, hooks, or subagents.

For large orchestration workloads today:

- create sessions when work is ready;
- stop idle sessions once their next-step latency no longer matters;
- retain provider session IDs in Store and resume explicitly later;
- bound caller-side turn startup with `Task.async_stream/3`, Broadway, Oban, or
  your own job coordinator;
- group work into fewer conversational sessions when that preserves intent;
- monitor OS resources and provider rate-limit events.

Future passivation or admission-control components can be added without
changing public SessionRef/Turn IDs, but they are not part of v0.1.

The default Memory Store is another large-fleet limit. It serializes all Store
operations through one process and keeps full event history until an aggregate
is deleted. Each live SessionServer also retains its turn and request maps for
the life of that logical session. For a 1,000-session orchestrator, use a
durable/partitioned Store, expire or compact high-volume deltas, stop idle
provider runtimes, and put admission control in the caller.

## Workspace concurrency

AgentHarness does not lock working directories. Two sessions pointed at the
same writable checkout can modify the same files concurrently, run conflicting
commands, or observe half-completed changes.

The orchestrator should choose a policy:

- only one writable turn per checkout;
- one Git worktree per session/task;
- isolated containers or sandboxes;
- explicitly read-only concurrent sessions.

This matters well before BEAM process limits.

## Event and subscriber concurrency

Provider readers continuously drain CLI output into the SessionServer mailbox.
The SessionServer delivers live events with ordinary `send/2`; it does not wait
for subscriber acknowledgement.

Consequences:

- provider output is not blocked by a slow consumer;
- ordering is preserved per session;
- subscribers need their own backpressure/buffering policy;
- high-frequency deltas can grow a slow subscriber's mailbox.

Use a dedicated process to coalesce deltas before forwarding them to LiveView,
PubSub, a database, or another slower system. Semantic events—requests,
responses, tool completion, usage, and terminal events—should not be discarded.

## Failure semantics

AgentHarness favors explicit uncertainty over false success:

- a provider terminal message marks successful or failed completion;
- a missing terminal record is a failure;
- a provider runner crash fails the active turn and tears down that provider
  transport rather than risking reuse while an upstream turn may still run;
- transport loss makes the session unavailable;
- Store write failure crashes the SessionServer rather than publishing an
  unpersisted lifecycle event;
- cancellation acceptance is not terminal completion.

It does not automatically retry turns. Coding turns may already have modified
files or triggered external side effects before a failure. Retry policy belongs
to the orchestrator, which can inspect the workspace and stored event history.

## Custom providers

New adapters implement `AgentHarness.Provider`:

```elixir
@callback open_session(SessionConfig.t(), Provider.Sink.t()) ::
  {:ok, handle, session_info} | {:error, term}

@callback start_turn(handle, Turn.t(), input, keyword()) ::
  {:ok, provider_turn_ref} | {:error, term}

@callback respond(handle, provider_request_ref, Response.t()) ::
  :ok | {:error, term}

@callback cancel(handle, provider_turn_ref) ::
  :ok | {:error, term}

@callback close_session(handle) :: :ok
@callback capabilities(handle) :: Capabilities.t()
```

Use a provider module directly:

```elixir
AgentHarness.start_session(MyApp.LocalAgentProvider, cwd: "/work/project")
```

The module must be loaded and implement the provider entry points. Provider
processes should run under `AgentHarness.ProviderSupervisor`, acknowledge
commands quickly, and emit asynchronous events through the supplied Sink.

Preserve original provider payloads in `raw`; normalize stable semantics into
AgentHarness event and request fields.

## Single-node scope

The built-in Registry, supervisors, and Memory Store are node-local. Stable IDs
make a future distributed router possible, but v0.1 does not provide:

- distributed Registry ownership;
- cluster-wide capacity leases;
- remote workspace placement;
- automatic failover/resume;
- cross-node event delivery.

Add those concerns in the larger orchestrator instead of starting the same
logical session ID independently on several nodes.
