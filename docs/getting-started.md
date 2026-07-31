# Getting started

AgentHarness turns a locally installed coding-agent CLI into an OTP-managed
session. A session is a conversation with a stable ID; a turn is one request
within that conversation.

This guide covers the provider-neutral public API. See
[Provider configuration](configuration.md) for Codex- and Claude-specific
settings.

## Start a session

```elixir
{:ok, session} =
  AgentHarness.start_session(:codex,
    cwd: "/absolute/path/to/project",
    model: "your-model",
    system_prompt: "Work carefully and run relevant tests.",
    approval_policy: :on_request,
    sandbox: :workspace_write,
    metadata: %{project: "my-project"}
  )
```

The return value is a stable `%AgentHarness.SessionRef{}` containing a provider
and generated ID. It intentionally contains no PID. AgentHarness locates the
live process through its Registry, allowing process ownership to remain an
implementation detail.

You can provide your own ID:

```elixir
{:ok, session} =
  AgentHarness.start_session(:codex,
    id: "issue-123-implementation",
    cwd: "/absolute/path/to/project"
  )
```

IDs must be unique among live sessions. Reusing a live ID returns
`{:error, :session_already_exists}`.

With the default Store, a stopped session's history remains retained and its
logical ID cannot be reused; reopening it returns
`{:error, :session_id_already_used}`. Use a new AgentHarness ID to wrap a saved
provider conversation, or explicitly delete the old Store aggregate first.
With `store: false`, an ID can be reused after its live process has stopped.

`start_session/2` opens the provider runtime before it returns. Executable
discovery, invalid configuration, and initialization failures usually surface
at session startup. Providers can still report account, quota, model, or tool
errors during a turn.

### Common session options

| Option               | Purpose                                                           |
| -------------------- | ----------------------------------------------------------------- |
| `:id`                | Caller-selected logical session ID                                |
| `:cwd`               | Working directory visible to the coding agent                     |
| `:model`             | Provider model name                                               |
| `:system_prompt`     | Codex developer instructions or Claude system prompt              |
| `:approval_policy`   | Provider-native approval policy; maps to Claude `permission_mode` |
| `:sandbox`           | Provider-native sandbox configuration                             |
| `:mcp_servers`       | Map of session-scoped MCP servers                                 |
| `:skills`            | Skill paths or descriptors                                        |
| `:env`               | Environment overrides for the provider process                    |
| `:provider_options`  | Provider-specific escape hatch                                    |
| `:metadata`          | Caller-owned orchestration metadata                               |
| `:event_buffer_size` | Positive in-memory replay-buffer size; default `1_000`            |
| `:store`             | `{store_module, owner}` or `false`                                |

Do not put secrets in `:metadata`. The default Store persists metadata as part
of the session snapshot. Provider environment values are not included in that
snapshot, although the configuration fingerprint reflects the configuration.

## Inspect a session

```elixir
%{
  session: ^session,
  status: :idle,
  provider_session_id: provider_id,
  current_turn: nil,
  pending_requests: []
} = AgentHarness.status(session)

capabilities = AgentHarness.capabilities(session)
```

Session status is one of the live lifecycle states such as `:idle`, `:running`,
`:awaiting_input`, `:cancelling`, or `:unavailable`. After a session stops,
operations on its handle return `{:error, :session_not_found}`.

Capabilities distinguish `:native`, `:emulated`, `:experimental`, and
`:unsupported` support. Check them when writing provider-agnostic orchestration:

```elixir
case AgentHarness.capabilities(session).questions do
  support when support in [:native, :emulated] -> :can_ask
  _ -> :cannot_ask
end
```

## Start a turn

```elixir
{:ok, turn} =
  AgentHarness.start_turn(
    session,
    "Implement the feature, run tests, and report what changed.",
    metadata: %{work_item: "issue-123"}
  )
```

The returned `%AgentHarness.Turn{}` contains a stable turn ID, the session ID,
the input, and its initial running state.

Only one turn may be active in a session. Starting another before the first has
a terminal event returns:

```elixir
{:error, {:turn_in_progress, current_turn}}
```

Different sessions can run turns concurrently. AgentHarness does not impose a
global queue or capacity limit.

Turn options reserve `:id` and `:metadata` for the core. Remaining options are
sent to the provider adapter. Codex accepts its documented per-turn options;
Claude currently uses `:timeout` and `:filter` for its stream. Claude filters
only affect nonterminal messages; AgentHarness always retains the result needed
to finish the turn.

Claude currently accepts string turn input. Codex accepts a string or a list
of structured input maps.

## Subscribe to events

Subscribe to a whole session:

```elixir
{:ok, subscription} = AgentHarness.subscribe(session, from: :latest)
```

Or only one turn:

```elixir
{:ok, subscription} = AgentHarness.subscribe(turn, from: :start)
```

Events arrive in the subscriber's mailbox:

```elixir
receive do
  {:agent_harness, subscription_ref, %AgentHarness.Event{} = event}
  when subscription_ref == subscription.ref ->
    IO.inspect(event)
end
```

The `:from` option controls replay:

| Value                | Meaning                                                   |
| -------------------- | --------------------------------------------------------- |
| `:latest`            | Only future events; the default for `subscribe/2`         |
| `:start`             | Replay retained events, then continue live                |
| `{:after, sequence}` | Replay events with a greater sequence, then continue live |

Each event has a monotonically increasing `seq` within its session. A
turn-scoped subscription filters out session events and events belonging to
other turns.

By default the calling process receives events. To deliver to another process:

```elixir
{:ok, subscription} =
  AgentHarness.subscribe(turn, pid: consumer_pid, from: :latest)
```

AgentHarness monitors subscriber processes and removes subscriptions when the
subscriber exits. Explicitly unsubscribe when a live consumer is finished:

```elixir
:ok = AgentHarness.unsubscribe(subscription)
```

Delivery uses ordinary BEAM messages. A slow subscriber can accumulate mailbox
messages, particularly when token deltas are enabled. Put UI broadcasting or
expensive event handling in a dedicated consumer process.

## Consume an Elixir stream

`stream/2` wraps a turn subscription in an `Enumerable`:

```elixir
{:ok, stream} = AgentHarness.stream(turn, from: :start, timeout: 30_000)

stream
|> Stream.each(fn
  %AgentHarness.Event{type: :message_delta, data: %{text: text}} ->
    IO.write(text)

  _event ->
    :ok
end)
|> Enum.to_list()
```

The stream includes the terminal event and then ends. It is replay-safe, so it
can be created after a fast turn has already completed, provided the event is
still available from the configured Store or local buffer.

The stream must be consumed by the same process that called `stream/2` because
that process owns the subscription mailbox. `:timeout` is the maximum wait for
the next event; an idle stream raises `AgentHarness.StreamTimeoutError`. If the
SessionServer exits while the consumer waits, the stream raises
`AgentHarness.SessionDownError`.

`stream/2` creates its subscription before returning the lazy Enumerable. Do
not create and then discard an unconsumed stream: its subscription remains
until the calling process exits. Enumerating to completion, halting an
enumeration normally, or raising while enumerating runs the stream cleanup.

## Wait for completion

```elixir
case AgentHarness.await(turn, timeout: 120_000) do
  {:ok, %{status: :completed, result: result}} ->
    {:completed, result}

  {:error, %AgentHarness.Event{type: :turn_failed} = event} ->
    {:failed, event.data}

  {:error, %AgentHarness.Event{type: type} = event}
  when type in [:turn_cancelled, :turn_interrupted] ->
    {:stopped, event}

  {:error, :timeout} ->
    :timed_out

  {:error, {:session_down, reason}} ->
    {:session_lost, reason}
end
```

`await/2` uses replay plus a live subscription, avoiding the usual race between
checking state and subscribing. Its timeout is an overall deadline. Timing out
does not stop the turn; call `cancel/1` if that is your desired policy.

## Answer questions and approvals

Providers surface blocking interaction as `%AgentHarness.Request{}` values in
`:request_created` events:

```elixir
alias AgentHarness.{Event, Request, Response}

receive do
  {:agent_harness, ref,
   %Event{
     type: :request_created,
     data: %Request{kind: :question} = request
   }}
  when ref == subscription.ref ->
    AgentHarness.respond(request, Response.answer("Postgres"))
end
```

For multiple questions, answer with a map keyed by normalized question ID:

```elixir
answers =
  Map.new(request.questions, fn question ->
    case question.id do
      "database" -> {question.id, "Postgres"}
      "region" -> {question.id, "us-east-1"}
    end
  end)

:ok = AgentHarness.respond(request, Response.answer(answers))
```

Approval decisions are explicit:

```elixir
AgentHarness.respond(request, Response.approve())
AgentHarness.respond(request, Response.approve(scope: :session))
AgentHarness.respond(request, Response.deny("Network access is not allowed"))
AgentHarness.respond(request, Response.cancel("Stop this operation"))
```

Approval scopes are provider capabilities. Codex request choices expose
session scope for file and structured-permission protocols. Command approvals
follow the provider's `availableDecisions`; a command that omits
`acceptForSession` rejects that scope. Claude currently rejects session-scoped
approval instead of silently reducing it to one-shot approval.

The SessionServer serializes responses. The first accepted response wins;
subsequent attempts return `{:error, :already_resolved}`. A request can expire
when its turn ends, its provider resolves it independently, or its configured
question timeout elapses; responding afterward returns
`{:error, :request_expired}`.

Inspect `request.kind`, `request.choices`, `request.questions`,
`request.schema`, and `request.metadata` before selecting a response. Treat
`request.provider_ref` as opaque.

## Cancel a turn

```elixir
:ok = AgentHarness.cancel(turn)
```

Cancellation is asynchronous. `:ok` means the adapter accepted or scheduled
the cancellation request, not that the CLI acknowledged it or the turn has
stopped. This distinction matters for a lazy Codex turn whose upstream IDs
have not arrived yet. Watch the turn subscription or call `await/2` for
`:turn_cancelled`, `:turn_interrupted`, or another provider terminal result.

Repeated cancellation while the session is cancelling is idempotent. Trying to
cancel an inactive turn returns `{:error, :turn_not_active}`.

## Stop a session

An idle session stops cleanly:

```elixir
:ok = AgentHarness.stop_session(session)
```

Stopping an active session without an explicit policy returns:

```elixir
{:error, :turn_in_progress}
```

To cancel the current turn and close immediately:

```elixir
:ok = AgentHarness.stop_session(session, force: true)
```

Forced stop records cancellation and closure events, expires pending requests,
and closes the provider runtime. Stored history is retained; stopping a session
does not purge it.
