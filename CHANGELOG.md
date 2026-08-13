# Changelog

All notable changes to AgentHarness will be documented in this file.

The project follows [Semantic Versioning](https://semver.org/). It has not yet
published a stable public API.

## Unreleased

### Added

- `AgentHarness.Exec`, a byte-level behaviour for running a command in some
  execution environment (spawn with argv/env/cwd, stream output, write stdin,
  force-kill), with `AgentHarness.Exec.Local` as the port-backed default.
  Remote execution backends (SSH, sandbox vendors) implement this behaviour
  outside the library.
- `AgentHarness.Providers.Claude.Adapter.Exec`, a `ClaudeCode.Adapter` that
  runs the Claude Code CLI through any `AgentHarness.Exec` implementation.
  The stream-json protocol, control handshake, and question/approval routing
  stay on the orchestrator while the CLI runs wherever the exec module puts
  it. Requires `auth: :inherit`; the spawn spec is remote-safe (explicit env,
  no local environment forwarding, `cwd` resolved where the CLI runs). SDK
  features that need filesystem access next to the CLI (history, plugin/skill
  materialization) stay orchestrator-local and are out of scope for this
  adapter. The adapter monitors pid exec handles, defers exec output that
  arrives before provisioning completes, force-kills the exec on disconnect,
  and redacts `api_key`/`env` from its own inspect output and crash reports.
- Credential redaction for `AgentHarness.SessionConfig`: the `Inspect`
  implementation keeps the top-level keys of `env`, `provider_options`, and
  `mcp_servers` and replaces every value with `"[REDACTED]"`, so a
  misconfigured session stays debuggable without exposing secrets.
  `SessionServer` and all three provider sessions (Claude, Codex, Pi)
  implement `format_status/1` so crash reports and `:sys.get_status/1`
  scrub the raw state term, including the prepared/resolved option
  containers that carry the merged session env and API keys (Pi's
  `--api-key` argv value among them). The scrub logic lives in one shared
  internal helper so the providers cannot drift. A Pi spawn failure reports
  a reduced reason instead of the raised term whose stacktrace carries the
  full argv and env.

## 0.2.0 - 2026-08-04

### Added

- Pi session adapter for `pi --mode rpc`, speaking pi's JSONL command and event
  protocol over stdio.
- Pi provider options for tool allow and deny lists, thinking level,
  extensions, resume, fork, ephemeral sessions, session storage, and
  `agent_dir` isolation.
- Questions from Pi, raised through its extension UI sub-protocol as `confirm`,
  `select`, `input`, and `editor` dialogs.
- Native Pi skills through `--skill`.
- Subscription auth for Pi, which rejects credential-shaped session `env`
  entries and confirms the selected provider holds an OAuth credential without
  reading the token.
- Pi capability reporting for `approvals`, `per_session_mcp`, and `steer`, all
  unsupported. A session that sets `mcp_servers`, `approval_policy`, or
  `sandbox` is rejected at `start_session/2` instead of opened with those
  settings dropped.
- Recorded `pi --mode rpc` fixtures that the normalizer and session tests
  replay, and live tests behind the `live` tag.

## 0.1.0 - 2026-07-31

### Added

- Supervised Codex CLI and Claude Code session adapters.
- Ordered session and turn event subscriptions, replay, streams, and await.
- Structured questions, approvals, MCP elicitation, cancellation, and terminal
  outcomes.
- Per-session MCP, skills, authentication, model, sandbox, and provider
  configuration.
- Store behaviour with an in-memory implementation, inventory, guarded purge,
  explicit ID replacement, and durability policy.
- Session monitoring and Telemetry lifecycle events for orchestrators.
- Configurable lifecycle deadlines and supervisor capacity limits.
- Owner-bound startup guardians for provider runtimes that ignore readiness
  cancellation.
- A configurable completed-turn cache that bounds hot Turn, terminal-event,
  and request retention while preserving Store-backed cold lookup.
- A bounded provider-command state machine with exactly-once response claims,
  local cancellation admission, and provider-command Telemetry spans.

### Changed

- Provider and session startup handshakes no longer serialize independent
  session creation.
- Provider opening and initial Store finalization have separate phase-aware
  deadlines; partial new aggregates are rolled back without burning their ID.
- Turn admission is asynchronous and preserves a stable turn handle across a
  local call timeout.
- Store failures now follow an explicit degrade or fail-stop policy.
- Turn replay is indexed by turn and reports when a completed turn's terminal
  replay is unavailable.
- Graceful application shutdown closes sessions before provider infrastructure.
- Provider-open and turn-admission tasks now share the owning SessionServer's
  lifecycle, and startup readiness uses an acknowledged caller handoff.
- Pending startup attempts remain explicitly marked in Store until the two-way
  readiness acknowledgement, so a hard kill cannot burn a logical session ID.

### Fixed

- General calls no longer report `:ok` when a SessionServer exits normally.
- Live Store deletion is rejected instead of crashing the active session.
- Consumers can monitor session death without blocking a GenServer callback.
- Invalid approval scopes are rejected at both construction and response
  boundaries.
- Provider-admission timeouts and crashes retire uncertain sessions instead of
  exposing them as reusable idle conversations.
- Provider response and cancellation callbacks no longer block the
  SessionServer; uncertain acknowledgement retires the session.
- Live session inventory reads Registry metadata without waiting on each
  SessionServer mailbox.
- Transport loss reported during provider opening can no longer be overwritten
  by a late successful return.
- Provider loss and terminal messages received during turn admission preserve
  their causal ordering.
- Graceful shutdown expires pending requests and records an interrupted turn
  before the final session-closed event.
- Generated Claude skill plugins are removed when startup is killed before
  ownership can transfer to the provider runtime.
