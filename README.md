# AgentHarness

AgentHarness is an Elixir library for running coding agents as supervised OTP
sessions. It currently supports Codex CLI, Claude Code, and Pi, normalizes
their streaming output, and gives callers one interface for turns, questions,
approvals, cancellation, completion, and event replay.

AgentHarness is deliberately **bring your own CLI**. It does not proxy
credentials, create API keys, or log in on your behalf. By default the
provider processes run on your machine with the accounts already configured in
their official CLIs. Each provider can also run its CLI in another execution
environment (a remote sandbox, an SSH channel) through the
`AgentHarness.Exec` behaviour while the protocol stays on your node; see
[Remote execution](#remote-execution).

> AgentHarness is an early v0.x library. Its core lifecycle is tested, but the
> public API and provider event vocabulary may still evolve.

## What it provides

- One supervised `GenServer` per logical agent session
- Codex app-server, Claude Code, and Pi RPC streaming adapters
- Ordered `%AgentHarness.Event{}` values with normalized data and raw SDK payloads
- Session- or turn-scoped subscriptions
- Replay-capable Elixir streams and race-free terminal `await/2`
- Native session monitors and live/stored session inventory for orchestrators
- Structured questions, approvals, and MCP elicitation
- Per-session MCP servers, skills, model, workspace, sandbox, and provider options
- A byte-level `AgentHarness.Exec` behaviour for running any provider's CLI in
  another execution environment, with a port-backed local default
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

Pi is optional and only needed for `:pi` sessions. It is an npm package that
requires Node 22.19 or newer:

```console
$ npm install -g --ignore-scripts @earendil-works/pi-coding-agent
$ pi --version
$ pi          # then /login to authenticate a provider
```

By default, AgentHarness requires the CLIs' saved subscription login (ChatGPT,
claude.ai, or a pi `/login` provider) and rejects known API, cloud-provider, and
custom-routing overrides. To
intentionally use another authentication route, set
`provider_options: %{auth: :inherit}`. See
[Billing and authentication](docs/billing-and-authentication.md) for the
provider-policy boundary and current official links.

## Installation

Add AgentHarness to your dependencies:

```elixir
def deps do
  [
    {:agent_harness, "~> 0.3.0"}
  ]
end
```

For local development against a checkout, use `path: "../agent_harness"`
instead.

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

Codex and Claude accept session-scoped MCP configuration. Pi has no MCP
support and rejects a session that configures it:

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

## Remote execution

Each provider can run its CLI through an `AgentHarness.Exec` implementation
instead of a local port. The provider protocol (streaming, questions,
approvals, cancellation) stays in your application's node; only the CLI
process moves. `AgentHarness.Exec.Local` is the default and reproduces local
behavior, and an application-provided exec module can place the process in a
remote sandbox:

```elixir
# Pi and Codex
{:ok, session} =
  AgentHarness.start_session(:codex,
    cwd: "/workspace",
    provider_options: %{
      auth: :inherit,
      exec: {MyApp.SandboxExec, sandbox: sandbox}
    }
  )

# Claude selects it through the claude_code adapter option
{:ok, session} =
  AgentHarness.start_session(:claude,
    cwd: "/workspace",
    provider_options: %{
      auth: :inherit,
      adapter: {AgentHarness.Providers.Claude.Adapter.Exec,
                exec: {MyApp.SandboxExec, sandbox: sandbox}}
    }
  )
```

Remote execution requires `auth: :inherit`: the fail-closed `:subscription`
mode verifies local CLI state, which says nothing about the environment the
exec would run the process in. The exec contract is byte-level and
protocol-free (spawn with argv/env/cwd, stream output, write stdin,
force-kill), so one implementation covers all three providers. Per-provider
details are in [Provider configuration](docs/configuration.md), and
[Writing an Exec implementation](docs/writing-an-exec-implementation.md)
walks through building a sandbox adapter, using E2B as the example.

## Documentation

- [Getting started and public API](docs/getting-started.md)
- [Provider configuration, MCP, skills, and authentication](docs/configuration.md)
- [Driving AgentHarness from a GenServer](docs/genserver-integration.md)
- [Lifecycle, events, requests, and persistence](docs/lifecycle-and-events.md)
- [Supervision, concurrency, and scaling](docs/architecture.md)
- [Writing an Exec implementation (remote sandboxes)](docs/writing-an-exec-implementation.md)
- [Billing and authentication](docs/billing-and-authentication.md)
- [Tests and live CLI checks](docs/testing.md)

## Development

Normal tests use Mox-backed provider clients and do not consume provider
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
