# AgentHarness

AgentHarness is an Elixir library for running locally installed coding agents
as supervised OTP sessions. It currently supports Codex CLI and Claude Code,
normalizes their streaming output, and gives callers one interface for turns,
questions, approvals, cancellation, completion, and event replay.

AgentHarness is deliberately **bring your own CLI**. It does not proxy
credentials, create API keys, or log in on your behalf. The Codex and Claude
processes run on your machine with the accounts already configured in their
official CLIs.

> AgentHarness is an early v0.1 library. Its core lifecycle is tested, but the
> public API and provider event vocabulary may still evolve.

## What it provides

- One supervised `GenServer` per logical agent session
- Codex app-server and Claude Code streaming adapters
- Ordered `%AgentHarness.Event{}` values with normalized data and raw SDK payloads
- Session- or turn-scoped subscriptions
- Replay-capable Elixir streams and race-free terminal `await/2`
- Native session monitors and live/stored session inventory for orchestrators
- Structured questions, approvals, and MCP elicitation
- Per-session MCP servers, skills, model, workspace, sandbox, and provider options
- An in-memory event and lifecycle store with a behaviour for durable adapters
- Stable session and turn handles that contain IDs rather than PIDs
- Telemetry spans and events for commands, session/turn lifetimes, requests, and Store writes

Each call to `start_session/2` opens a provider runtime, each session allows one
active turn, and different sessions may run at the same time. Capacity can be
bounded through supervisor configuration or caller-side admission control. See
[Architecture and concurrency](docs/architecture.md) before creating a large
number of resident sessions.

## Prerequisites

This repository requires Elixir 1.19 and is tested with OTP 28.

Provider-side JSON helpers use Elixir 1.19's built-in `JSON` module; this
library does not declare its own JSON codec dependency. Provider SDKs may still
bring their own transitive codecs.

Install and authenticate the CLIs you intend to use:

```console
$ codex --version
$ codex login

$ claude --version
$ claude
```

By default, AgentHarness requires the CLIs' saved ChatGPT or claude.ai login and
rejects known API, cloud-provider, and custom-routing overrides. To
intentionally use another authentication route, set
`provider_options: %{auth: :inherit}`. See
[Billing and authentication](docs/billing-and-authentication.md) for the
provider-policy boundary and current official links.

## Installation

Until the package is published, use a path dependency:

```elixir
def deps do
  [
    {:agent_harness, path: "../agent_harness"}
  ]
end
```

Then fetch and compile dependencies:

```console
$ mix deps.get
$ mix compile
```

AgentHarness starts its Registry, supervisors, and default in-memory store with
your application.

## Quick start

```elixir
alias AgentHarness.Event

{:ok, session} =
  AgentHarness.start_session(:codex,
    cwd: "/absolute/path/to/project",
    approval_policy: :never,
    sandbox: :workspace_write
  )

{:ok, turn} =
  AgentHarness.start_turn(
    session,
    "Read the project, fix the failing test, and explain the change."
  )

{:ok, subscription} = AgentHarness.subscribe(turn, from: :start)

receive_events = fn receive_events ->
  receive do
    {:agent_harness, ref, %Event{type: :message_delta, data: %{text: text}}}
    when ref == subscription.ref ->
      IO.write(text)
      receive_events.(receive_events)

    {:agent_harness, ref, %Event{type: :turn_completed} = event}
    when ref == subscription.ref ->
      event

    {:agent_harness, ref, %Event{type: type} = event}
    when ref == subscription.ref and
           type in [:turn_failed, :turn_cancelled, :turn_interrupted] ->
      event

    {:agent_harness, ref, %Event{}}
    when ref == subscription.ref ->
      receive_events.(receive_events)
  end
end

terminal_event = receive_events.(receive_events)
AgentHarness.unsubscribe(subscription)
AgentHarness.stop_session(session)
```

For a simpler blocking workflow:

```elixir
{:ok, turn} = AgentHarness.start_turn(session, "Summarize this project.")

case AgentHarness.await(turn, timeout: 120_000) do
  {:ok, %{status: :completed, result: result}} ->
    IO.inspect(result)

  {:error, %AgentHarness.Event{} = terminal_event} ->
    IO.inspect(terminal_event, label: "agent did not complete")

  {:error, :timeout} ->
    AgentHarness.cancel(turn)
end
```

## Responding to questions and approvals

Questions and permission checks arrive as `:request_created` events:

```elixir
alias AgentHarness.{Event, Request, Response}

receive do
  {:agent_harness, ref,
   %Event{
     type: :request_created,
     data: %Request{kind: :question} = request
   }}
  when ref == subscription.ref ->
    # A scalar works for one question. For multiple questions, pass a map
    # keyed by each entry's `id` from request.questions.
    :ok = AgentHarness.respond(request, Response.answer("Postgres"))

  {:agent_harness, ref,
   %Event{
     type: :request_created,
     data: %Request{kind: kind} = request
   }}
  when ref == subscription.ref and
         kind in [:command_approval, :file_change_approval, :permission] ->
    :ok = AgentHarness.respond(request, Response.approve(scope: :once))
end
```

A request can be answered only once. Pending requests become expired when
their turn ends.

## Per-session MCP and skills

Both providers accept session-scoped MCP configuration:

```elixir
mcp_servers = %{
  "project-tools" => %{
    command: "npx",
    args: ["-y", "@example/project-mcp", "/absolute/path/to/project"]
  }
}

skills = [
  %{name: "release", path: "/absolute/path/to/release/SKILL.md"}
]

{:ok, session} =
  AgentHarness.start_session(:claude,
    cwd: "/absolute/path/to/project",
    mcp_servers: mcp_servers,
    skills: skills,
    provider_options: %{
      auth: :subscription,
      allowed_tools: ["Read", "Grep", "Glob", "Skill"]
    }
  )
```

The providers materialize these settings differently. Claude loads standalone
skills through a temporary session plugin; Codex sends explicit skill items
with each turn. Details and provider-specific options are in
[Provider configuration](docs/configuration.md).

## Documentation

- [Getting started and public API](docs/getting-started.md)
- [Provider configuration, MCP, skills, and authentication](docs/configuration.md)
- [Driving AgentHarness from a GenServer](docs/genserver-integration.md)
- [Lifecycle, events, requests, and persistence](docs/lifecycle-and-events.md)
- [Supervision, concurrency, and scaling](docs/architecture.md)
- [Billing and authentication](docs/billing-and-authentication.md)
- [Tests and live CLI checks](docs/testing.md)

## Development

Normal tests use Mox-backed provider clients and do not consume Codex or Claude
quota:

```console
$ mix precommit
```

Live CLI tests are tagged and excluded by default:

```console
$ mix test --include live
```

Live tests invoke your authenticated CLI and may consume subscription or API
usage. Read [Testing](docs/testing.md) before enabling them.

## License

AgentHarness is available under the [MIT License](LICENSE).
