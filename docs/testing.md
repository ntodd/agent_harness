# Testing

AgentHarness separates deterministic protocol/lifecycle tests from live CLI
tests that consume real provider usage.

## Test commands

Run the normal suite:

```console
$ mix test
```

Run the complete local precommit gate:

```console
$ mix precommit
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
$ mix test --include live
```

To run only one provider's integrations:

```console
$ mix test \
    test/agent_harness/providers/claude_live_test.exs \
    --include live

$ mix test \
    test/agent_harness/providers/codex_live_test.exs \
    --include live

$ mix test \
    test/agent_harness/providers/pi_live_test.exs \
    --include live
```

Each provider also has an exec live test
(`claude_exec_adapter_live_test.exs`, `codex_client_exec_live_test.exs`,
`pi_client_exec_live_test.exs`) that runs the real CLI through
`AgentHarness.Exec.Local`. These require `auth: :inherit`, so they use the
local environment's saved authentication or an exported API key.

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

Run the Pi tests only after installing the CLI, which needs Node 22.19 or
newer:

```console
$ npm install -g --ignore-scripts @earendil-works/pi-coding-agent
$ pi --version
```

Pi is bring-your-own-model, so the live tests run with `auth: :inherit` and
whichever API key is already exported. They default to `openai/gpt-4.1-nano`;
override with `PI_LIVE_MODEL`. Each test sets `agent_dir` to a temporary
directory, which points `PI_CODING_AGENT_DIR` away from your real `~/.pi`, so
live runs never read or write your own pi config, credentials, or sessions.
They cover session opening, a completed turn, token streaming, a real `read`
tool call, cancellation, resume from a persisted session file, and the two
rejection paths for features pi does not have.

Pi writes an `EPIPE` stack trace to stderr when the harness closes its port
mid-write. It is noise on teardown: pi appends session entries as they happen,
so the transcript on disk is complete, which the resume test exercises
directly.

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
$ iex -S mix
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

### Recorded protocol fixtures

`test/support/fixtures/pi` holds JSONL frames captured verbatim from a real
`pi --mode rpc` process. The normalizer and session tests replay them, so those
assertions track what pi actually emits rather than what its documentation
describes. Several adapter decisions came from differences between the two:
readiness needs a `get_state` probe because pi sends no startup frame,
`agent_settled` rather than `agent_end` is the terminal marker, and an `abort`
acknowledgement arrives after the run has already settled.

To refresh a fixture, run the corresponding scenario against a real pi process
and save its stdout. Only `message_update` frames should be decimated; every
lifecycle and control frame belongs in the file.

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
