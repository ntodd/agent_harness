# Testing

AgentHarness separates deterministic protocol/lifecycle tests from live CLI
tests that consume real provider usage.

## Toolchain

The repository pins Erlang, Elixir, and Node with mise. Erlang and Elixir are
required for the library and its deterministic suite. Node is development
parity for Node-based CLI and MCP commands; the core Elixir suite does not
invoke Node:

```console
$ mise install
$ mise exec -- elixir --version
```

Run the normal suite:

```console
$ mise exec -- mix test
```

Run the complete local precommit gate:

```console
$ mise exec -- mix precommit
```

`precommit` checks formatting, compiles with warnings as errors, runs tests, and
runs Credo in strict mode.

## Deterministic tests

Normal tests do not invoke authenticated coding agents. Provider boundaries are
Mox behaviours:

```text
AgentHarness.Provider
AgentHarness.Providers.Codex.Client
AgentHarness.Providers.Claude.Client
```

`AgentHarness.Provider` is the public adapter contract. The two client
behaviours are internal seams used to test the built-in adapters and are hidden
from generated API documentation; applications should not depend on them as
public extension points.

The tests exercise:

- SessionServer lifecycle and Registry behavior;
- event ordering, replay, subscriptions, streams, and await;
- exactly-once request responses and expiration;
- cancellation and transport failure;
- provider-command request claims, duplicate responses, timeout-boundary result
  harvesting, uncertain retirement, and SessionServer responsiveness;
- Store ownership, idempotency, and aggregate invariants;
- Codex event normalization and response encoding;
- Claude message normalization and `can_use_tool` decisions;
- MCP, skills, environment, and provider option mapping.

This boundary keeps default tests fast, deterministic, and free of provider
charges while still exercising the AgentHarness processes around the client.

Mox calls made by supervised processes require explicit ownership allowances
or global Mox mode. Existing provider lifecycle tests use global mode and are
therefore `async: false`; pure value and Store tests remain asynchronous.

## Live tests

Live tests use ExUnit's `:live` tag and are excluded in `test/test_helper.exs`.
They run only when explicitly included:

```console
$ mise exec -- mix test --include live
```

To run only one provider's integrations:

```console
$ mise exec -- mix test \
    test/agent_harness/providers/claude_live_test.exs \
    --include live

$ mise exec -- mix test \
    test/agent_harness/providers/codex_live_test.exs \
    --include live
```

Before running it:

```console
$ claude --version
$ claude
```

The Claude tests use `provider_options: %{auth: :subscription}`, a temporary
working directory, small prompts, and long timeouts. Subscription mode refuses
to start if `config :claude_code, api_key: ...` is present, preventing the live
test from silently switching to API billing. The tests cover completion, an
`AskUserQuestion` round trip, and cancellation while Claude is blocked in
`AskUserQuestion`. They invoke the real model and consume your available Claude
usage.

Run the Codex tests only after:

```console
$ codex --version
$ codex login
```

They open the real app-server and run a read-only ephemeral turn in a temporary
directory. The turn still invokes a real model and consumes your available
Codex usage.

Live tests should always:

- use a temporary or disposable workspace;
- use bounded prompts and timeouts;
- stop sessions in `on_exit/1`;
- avoid destructive tools and external side effects;
- make quota consumption explicit in their module tag/documentation;
- never run in the default CI test command.

Provider output is nondeterministic. Assert protocol invariants and terminal
status rather than prose beyond a deliberately constrained response.

## Manual smoke test

For interactive debugging:

```console
$ mise exec -- iex -S mix
```

Then:

```elixir
{:ok, session} =
  AgentHarness.start_session(:codex,
    cwd: System.tmp_dir!(),
    approval_policy: :never,
    sandbox: :read_only,
    provider_options: %{thread_options: %{ephemeral: true}}
  )

{:ok, turn} =
  AgentHarness.start_turn(session, "Reply with exactly OK and nothing else.")

AgentHarness.await(turn, timeout: 120_000)
AgentHarness.stop_session(session, force: true)
```

Use Claude-native options for the corresponding smoke test:

```elixir
{:ok, session} =
  AgentHarness.start_session(:claude,
    cwd: System.tmp_dir!(),
    approval_policy: :default,
    provider_options: %{auth: :subscription}
  )
```

Then run the same turn/await/stop calls. Do not carry Codex-only
`thread_options.ephemeral` or `sandbox: :read_only` into the Claude example.

If session startup fails, first run the same CLI directly in the same shell and
working directory. Then inspect:

- executable discovery (`codex --version` or `claude --version`);
- CLI authentication;
- `CODEX_HOME`, `ANTHROPIC_API_KEY`, and OAuth-related environment values;
- MCP child command paths;
- skill/plugin paths;
- configured model and provider option compatibility.

## Adding provider tests

Put parsing and response translation behind the provider's client behaviour.
Test it with captured SDK structs or JSON-derived fixtures rather than shell
scripts that happen to print a happy path.

At minimum, a provider test suite should cover:

1. open failure and cleanup;
2. per-session configuration isolation;
3. partial text/progress output;
4. provider session ID discovery;
5. questions and every approval kind;
6. malformed responses;
7. successful, failed, interrupted, and missing terminal records;
8. cancellation while output or a request is pending;
9. transport/runner failure;
10. unknown events preserved through `Event.raw`.

Tag all real executable/model calls `:live`. A fake process-based transport can
be used for framing and shutdown behavior without authenticating or consuming
quota.
