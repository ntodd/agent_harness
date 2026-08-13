# Provider configuration

`AgentHarness.start_session/2` accepts a common set of session options and a
`provider_options` escape hatch. Common options express intent; adapters map
them to the provider's native protocol.

```elixir
AgentHarness.start_session(provider,
  cwd: "/absolute/workspace",
  model: "provider-model-name",
  system_prompt: "Project-specific instructions",
  approval_policy: :on_request,
  sandbox: :workspace_write,
  mcp_servers: %{},
  skills: [],
  env: %{},
  provider_options: %{},
  event_buffer_size: 1_000,
  completed_turn_cache_size: 1_000,
  startup_timeout: 30_000,
  startup_finalization_timeout: 5_000,
  turn_start_timeout: 30_000,
  provider_command_timeout: 30_000,
  store_failure: :degrade
)
```

Provider values are not artificially reduced to one cross-provider enum. Both
adapters map the common `approval_policy` and `sandbox` fields, but each value
must use that provider's native shape. For Claude, `approval_policy` becomes
`permission_mode`, while `sandbox` uses the `claude_code` sandbox schema.

`startup_timeout` bounds Store reconciliation and provider opening for one
session. After the provider opens, `startup_finalization_timeout` separately
bounds the initial Store commit and each cleanup phase; it defaults to `5_000`
milliseconds. The public caller follows the phase transition, so one budget is
not silently consumed by another. Readiness ends with a bounded two-way caller
acknowledgement. Until the SessionServer processes that acknowledgement, Store
snapshots carry a pending startup-attempt marker. A partial or hard-killed
unacknowledged aggregate is therefore reclaimable on the next start even if its
last write had already reached `status: :idle`. A handshake that does not finish
within that phase budget returns `{:error, :session_start_ack_timeout}` and the
pending attempt remains safely reclaimable.

`turn_start_timeout` bounds the provider's asynchronous admission of a locally
accepted turn. These are per-session positive millisecond values. If turn
admission crosses its deadline, AgentHarness cannot know whether upstream work
began, so it fails the turn and retires that SessionServer instead of reusing
the conversation as idle.

`provider_command_timeout` is the SessionServer watchdog for `respond/2` and
the provider-facing part of `cancel/1`. It defaults to `30_000` milliseconds.
An uncertain command timeout retires the provider session so an answer or
cancellation that may already have crossed the transport boundary is never
treated as safely retryable.

`store_failure: :degrade` keeps a session alive after a write failure, switches
its status snapshot to `durability: {:degraded, failure}`, and publishes a
non-durable `:store_failed` event. Use `store_failure: :stop` when fail-stop
durability is required.

`event_buffer_size` bounds recent replay events. `completed_turn_cache_size`
bounds completed Turn values, terminal events, and request records held by the
live SessionServer; it defaults to the event-buffer size and may be zero or
`:infinity`. A configured Store remains the source for older cold lookups. The
built-in Memory Store retains its full journal until guarded purge, so use a
durable Store with an explicit retention policy for high-volume fleets.

Process-level limits and internal command deadlines are application settings:

```elixir
config :agent_harness,
  max_sessions: 100,
  max_provider_processes: 200,
  max_runner_tasks: 500,
  session_call_timeout: 30_000,
  provider_command_call_timeout: 60_000,
  codex_call_timeout: 25_000,
  codex_startup_call_timeout: 25_000,
  claude_call_timeout: 5_000,
  claude_interrupt_timeout: 3_000,
  claude_stop_timeout: 1_000,
  store_call_timeout: 5_000,
  provider_close_timeout: 5_000,
  provider_open_shutdown_grace: 250,
  session_shutdown_timeout: 60_000
```

The three `max_*` settings default to `:infinity`. Configure them before the
application starts. The default provider-command deadline order is outward:
the built-in transport call is shorter than `provider_command_timeout`, which
must be shorter than `provider_command_call_timeout`. This lets the adapter
report first, then the SessionServer retire uncertainty, before the public
caller can give up. The default turn-start watchdog is likewise longer than
either built-in provider call, and session shutdown has more time than provider
cleanup. A built-in provider call timeout is classified as uncertain and
retires the affected session.

`provider_open_shutdown_grace` is the non-negative number of milliseconds the
built-in opening-runtime guardian allows supervised cleanup before forcing that
runtime down when its SessionServer disappears during startup. It defaults to
`250`. `session_shutdown_timeout` defaults to `60_000` milliseconds so normal
session shutdown has room for bounded provider cleanup and final persistence.
`store_call_timeout` bounds calls to the built-in Memory Store; a custom Store
must enforce its own I/O deadline because its callback runs in the calling
process.

## Bring your own CLI and authentication

AgentHarness launches a local CLI and relies on credentials that CLI already
owns. It does not open login flows, accept passwords, store provider
credentials, or choose a paid plan for you. The selected executable and any
custom client module are trusted code boundaries.

Authenticate each CLI outside your Elixir application:

```console
$ codex login
$ claude
```

Then verify a small request directly in the CLI before debugging AgentHarness.

AgentHarness defaults to guarded `:subscription` mode. API keys, cloud
providers, custom endpoints, and other intentional routes require
`auth: :inherit`. Plan eligibility and billing are provider policy, not a
library capability; read [Billing and authentication](billing-and-authentication.md)
before unattended use.

### Credentials in logs

Session `env`, `provider_options`, and `mcp_servers` can carry credentials,
so `SessionConfig` redacts them when inspected: top-level keys stay visible
with `"[REDACTED]"` values. The `SessionServer` and each provider session
also scrub their raw state term via `format_status/1`, so the state
rendered in crash reports and `:sys.get_status/1` is display-safe even
under Erlang `~p` formatting, which bypasses the Inspect protocol.

One path stays open: supervisor progress and child-start reports include
the unredacted start arguments of a session process, config included.
Elixir's logger drops those SASL reports by default; leave
`handle_sasl_reports` off in any environment whose logs are shipped or
retained.

### Codex authentication and environment

Codex defaults to the fail-closed subscription mode:

```elixir
provider_options = %{auth: :subscription}
```

This mode uses the ChatGPT login in the selected Codex home. Before launching
the app-server, AgentHarness:

- clears API keys, endpoint variables, and OSS/custom-provider selectors;
- forces the SDK to no API key, the official base URL, and the OpenAI backend;
- validates the SDK's resolved model payload contains no environment or config
  overrides;
- pins every app-server thread to the built-in `openai` model provider;
- rejects model payloads, governed/remote execution surfaces, raw config
  overrides, custom provider backends, OSS providers, and thread profiles;
- inspects effective user, project, and system Codex configuration and rejects
  custom model providers, custom OpenAI base URLs, provider/profile tables, or
  an API-forced login method.

These checks prevent ambient or per-session provider routing from silently
switching the child to API billing. `auth: :inherit` is the explicit escape
hatch for those advanced configurations.

AgentHarness also inspects the selected file-backed Codex auth record before
launch. Stored `api_key` and `bedrock_api_key` modes are rejected. A keyring
store cannot currently be inspected by the Elixir SDK, so subscription mode
fails closed for that store as well. The validated `CODEX_HOME` is then set
explicitly in the child environment so an unvalidated inherited home cannot
take precedence.

To isolate a Codex home or account:

```elixir
{:ok, session} =
  AgentHarness.start_session(:codex,
    cwd: "/work/project",
    env: %{"CODEX_HOME" => "/work/codex-profile"}
  )
```

Environment entries are merged into the app-server process environment.
Subscription mode overwrites auth- and provider-routing entries after that
merge. Avoid putting credentials directly into logs, metadata, or exception
messages.

To intentionally preserve API credentials or another custom environment,
select:

```elixir
provider_options = %{auth: :inherit}
```

The only accepted Codex auth modes are `:subscription` and `:inherit`.
AgentHarness does not refresh ChatGPT tokens or generate upstream attestation
tokens itself. If an app-server asks the host for either operation, the active
turn fails and that provider connection is torn down rather than leaving an
unresolvable request pending.

### Claude authentication modes

Claude defaults to:

```elixir
provider_options = %{auth: :subscription}
```

This uses the globally installed `claude` executable and refuses to start
unless `claude auth status` reports a logged-in, first-party `claude.ai`
account. The check uses the selected executable, working directory, and
scrubbed child environment.

Subscription mode forcibly unsets known higher-precedence or alternate routing
configuration, including:

- `ANTHROPIC_API_KEY`, `ANTHROPIC_AUTH_TOKEN`, and `ANTHROPIC_BASE_URL`;
- custom auth headers and provider-specific base URLs;
- Bedrock, Mantle, Vertex, Foundry, and Claude Platform on AWS selectors;
- ambient `CLAUDE_CODE_OAUTH_TOKEN` and refresh-token overrides;
- host-managed credential-file, credential-variable, and provider-routing
  selectors.

It also fails session startup if either of these API-key sources is configured:

- `provider_options: %{api_key: "..."}`;
- `config :claude_code, api_key: "..."`.

Those checks matter because Claude Code's documented authentication precedence
places cloud providers, bearer tokens, API keys, and configured key helpers
ahead of the saved `/login` subscription. Remove the conflicting configuration
to use subscription mode, or select `auth: :inherit` when another credential is
intentional. See
[Claude Code authentication](https://code.claude.com/docs/en/team) and its
[environment-variable reference](https://code.claude.com/docs/en/env-vars).

Because CLI `settings`, `setting_sources`, arbitrary `extra_args`, or a custom
SDK adapter can install an `apiKeyHelper` or re-enable a credential source,
subscription mode rejects nonempty values for those provider options. It pins
the local Port adapter and supplies empty settings, source, and argument
values, overriding auth-sensitive `config :claude_code` defaults. Use the
common session options for sandbox, MCP, skills, model, and tools; select
`auth: :inherit` if custom Claude settings or adapters are intentional.

There is one boundary the library cannot override: Claude Code always applies
organization-managed policy, and managed settings have higher precedence than
CLI arguments. Server-managed settings, MDM policy, registry/plist policy, or
system `managed-settings.json` files can inject `env` or an `apiKeyHelper`
after AgentHarness's scrub. `claude auth status` does not reveal every such
route. Treat managed policy as trusted external configuration and verify it
with your administrator before relying on subscription-only billing. See
[Claude Code settings and precedence](https://code.claude.com/docs/en/settings)
and [SDK settings behavior](https://code.claude.com/docs/en/agent-sdk/claude-code-features).

To let the child process inherit your environment unchanged:

```elixir
provider_options = %{auth: :inherit}
```

Use `:inherit` only when intentional—for example, when an OAuth token or API
key is managed by your local environment. The only accepted auth modes are
`:subscription` and `:inherit`.

## MCP servers

MCP configuration belongs to the logical session:

```elixir
mcp_servers = %{
  "project-files" => %{
    command: "project-mcp",
    args: ["--root", "/work/project"],
    env: %{"LOG_LEVEL" => "warning"}
  },
  "internal-docs" => %{
    url: "https://docs.example.test/mcp"
  }
}

{:ok, session} =
  AgentHarness.start_session(:codex,
    cwd: "/work/project",
    mcp_servers: mcp_servers
  )
```

Use strings for stable MCP server names. Configuration values are provider
configuration, so the exact accepted fields depend on the installed CLI.

Codex inserts the map into the app-server thread's `config.mcp_servers`.
Nested atom keys are converted to strings. In `auth: :inherit` mode, existing
MCP entries supplied in Codex `thread_options.config` are merged, with common
`mcp_servers` entries taking precedence by server name. Subscription mode
rejects caller-supplied raw thread config and builds the MCP map itself.

Claude always passes an explicit inline MCP map and enables strict MCP mode,
including when the map is empty. This prevents SDK application defaults and
global CLI MCP servers from leaking into a session. To opt into global Claude
MCP configuration, set `provider_options: %{strict_mcp_config: false}`; doing so
weakens per-session isolation.

The underlying Claude SDK also accepts BEAM-native MCP server modules. That is
a Claude-specific extension; do not rely on it in provider-neutral code.

## Skills

The common `skills` option accepts paths or descriptors:

```elixir
skills = [
  "/work/skills/testing/SKILL.md",
  %{name: "release", path: "/work/skills/release/SKILL.md"}
]
```

### Codex skills

Codex normalizes each entry into an explicit skill input:

```elixir
%{type: :skill, name: "release", path: "/work/skills/release/SKILL.md"}
```

Skill inputs are prepended to every turn, and `skills_enabled` is enabled on
the thread. A Codex skill descriptor may set `enabled: false` to omit it.

Paths are sent to Codex as configured. Validate that they exist and are
readable before starting orchestration.

### Claude skills and plugins

Claude accepts three path forms:

- a plugin root containing `.claude-plugin/plugin.json`;
- a directory containing `SKILL.md`;
- a direct path to `SKILL.md`.

Existing plugin roots are loaded directly. Standalone skill directories are
copied, including supporting files, into one generated session plugin under
the system temporary directory. The generated plugin is removed when the
provider session closes.

The adapter adds the `Skill` tool to `allowed_tools` when it creates a plugin.
If you provide your own restrictive tool configuration, keep `Skill` allowed.

This translation is why Claude reports skill support as `:emulated` while
Codex reports it as `:native`.

## Codex options

Codex uses the `codex_sdk` app-server transport. A complete configuration may
look like:

```elixir
{:ok, session} =
  AgentHarness.start_session(:codex,
    cwd: "/work/project",
    model: "your-codex-model",
    system_prompt: "Use the project's conventions.",
    approval_policy: :on_request,
    sandbox: :workspace_write,
    provider_options: %{
      auth: :subscription,
      codex_path: "/opt/bin/codex",
      codex_options: %{},
      connect_options: %{
        init_timeout_ms: 10_000
      },
      thread_options: %{
        web_search_enabled: true,
        model_reasoning_summary: :concise
      },
      turn_options: %{
        effort: :medium
      }
    }
  )
```

Recognized layers:

| Key                    | Applied to                                          |
| ---------------------- | --------------------------------------------------- |
| `:auth`                | `:subscription` (default) or intentional `:inherit` |
| `:client`              | Trusted test/custom Codex client module             |
| `:codex_path`          | Executable override                                 |
| `:codex_options`       | `Codex.Options` construction                        |
| `:connect_options`     | App-server connection/initialization                |
| `:thread_options`      | New or resumed Codex thread                         |
| `:turn_options`        | Every turn in this AgentHarness session             |
| `:provider_session_id` | Resume a specific Codex thread                      |
| `:thread_id`           | Alias for `:provider_session_id`                    |
| `:resume`              | Alias for an exact thread ID                        |

String forms of documented option keys are canonicalized without creating
arbitrary atoms.

Common session values are used as defaults:

```text
cwd             → thread working_directory
model           → thread model
system_prompt   → thread developer_instructions
approval_policy → thread ask_for_approval
sandbox         → thread sandbox
```

Explicit safe `thread_options` override those defaults. Subscription mode
reserves provider-, profile-, and raw-config routing keys; use `auth: :inherit`
when those settings are intentional. Each call to `start_turn/3` may also
supply Codex turn options:

```elixir
AgentHarness.start_turn(session, "Analyze this failure",
  effort: :high,
  additional_context: "CI failed only on Linux",
  output_schema: schema
)
```

`id` and `metadata` remain AgentHarness options and are not sent to Codex.

### Resume a Codex conversation

Provider session IDs appear in `AgentHarness.status/1`, `:session_updated`
events, terminal result data, and the Store snapshot.

To create a new AgentHarness process around an existing Codex thread:

```elixir
{:ok, resumed} =
  AgentHarness.start_session(:codex,
    cwd: "/work/project",
    provider_options: %{provider_session_id: saved_thread_id}
  )
```

`:last` is deliberately rejected. In the SDK's app-server transport it does
not identify an exact thread and can silently start a new one. Resolve and
persist the concrete Codex thread ID instead. AgentHarness does not
automatically reconstruct stopped sessions from Store in v0.x.

### Codex structured input

String prompts are converted to text input. Codex also accepts a list of
structured input maps:

```elixir
AgentHarness.start_turn(session, [
  %{type: :text, text: "Review this image"},
  %{type: :local_image, path: "/work/screenshot.png"}
])
```

Configured skill items are prepended automatically.

## Claude options

Claude uses the `claude_code` Elixir SDK to drive the globally installed CLI
with a bidirectional stream:

```elixir
{:ok, session} =
  AgentHarness.start_session(:claude,
    cwd: "/work/project",
    model: "sonnet",
    system_prompt: "Use the project's conventions.",
    approval_policy: :default,
    sandbox: %{enabled: true},
    provider_options: %{
      auth: :subscription,
      allowed_tools: ["Read", "Grep", "Glob", "Bash(mix test:*)"],
      disallowed_tools: ["Bash(rm:*)"],
      max_turns: 20,
      readiness_timeout: 15_000,
      question_timeout: 300_000
    }
  )
```

AgentHarness consumes these internal keys:

| Key                   | Meaning                                                                |
| --------------------- | ---------------------------------------------------------------------- |
| `:auth`               | `:subscription` (default) or `:inherit`                                |
| `:auth_check_timeout` | Bound for `claude auth status`; default `5_000` ms                     |
| `:readiness_timeout`  | Time to wait for the CLI initialization handshake; default `10_000` ms |
| `:question_timeout`   | Time to wait for your response; default `:infinity`                    |
| `:client`             | Trusted test/custom client module                                      |

Other provider options are passed to `ClaudeCode.start_link/1` after common
fields are mapped, except that subscription mode rejects or replaces
auth-sensitive `api_key`, `settings`, `setting_sources`, `extra_args`, and
`adapter` values. Useful options include `allowed_tools`, `disallowed_tools`,
`permission_mode`, `max_turns`, `resume`, `fork_session`, `agents`,
`output_format`, and `plugins`. Refer to the installed `claude_code` dependency
and Claude Code version for the complete accepted set.

Unknown binary option names are rejected rather than converted to atoms.
Prefer atom-keyed options in application code.

Partial messages are enabled by default, producing character-level
`:message_delta`, `:thinking_delta`, and `:tool_input_delta` events. Set
`include_partial_messages: false` to reduce event volume; complete
assistant/tool messages and the terminal result remain available.

Claude maps common fields directly to Claude-native SDK options:

```elixir
approval_policy: :default,       # Claude `permission_mode`
sandbox: %{enabled: true}        # Claude SDK sandbox configuration
```

Consult the installed `claude_code` version for the sandbox schema. Do not use
dangerous permission-bypass modes unless the CLI is already contained by a
separate, trusted sandbox. Explicit `provider_options.permission_mode` or
`provider_options.sandbox` values override the corresponding common field.

Session startup waits for Claude's CLI initialization handshake before emitting
`:session_ready`. `readiness_timeout` bounds that wait. The handshake verifies
transport initialization, not whether a later model request will pass account,
quota, or model checks; those errors can still arrive during a turn.

The subscription auth check is a separate, no-model CLI command and does not
consume inference usage. Its timeout is controlled by `auth_check_timeout`.

Claude turn filters are applied by AgentHarness after reading the SDK stream.
They can suppress nonterminal provider messages, but the terminal
`ResultMessage` is always retained so a successful turn cannot become a
`:missing_result` failure.

Claude currently supports one-shot approvals. A
`Response.approve(scope: :session)` returns
`{:error, {:unsupported_approval_scope, :session}}` rather than silently
downgrading the requested scope.

### Question timeout

Claude's `can_use_tool` callback waits while your Elixir application resolves a
question or permission request. With the default `:infinity`, the Claude turn
can remain blocked indefinitely.

Set a positive millisecond timeout to fail closed:

```elixir
provider_options = %{question_timeout: 60_000}
```

When it expires, AgentHarness denies the tool and interrupts that request. This
is independent of `await/2` and stream timeouts.

### Resume or fork Claude

```elixir
{:ok, resumed} =
  AgentHarness.start_session(:claude,
    cwd: "/work/project",
    provider_options: %{
      auth: :subscription,
      resume: saved_claude_session_id
    }
  )

{:ok, forked} =
  AgentHarness.start_session(:claude,
    cwd: "/work/project",
    provider_options: %{
      auth: :subscription,
      resume: saved_claude_session_id,
      fork_session: true
    }
  )
```

AgentHarness records updated Claude session IDs, but v0.x does not automatically
recreate a stopped session from Store.

### Run the Claude CLI in another execution environment

`AgentHarness.Providers.Claude.Adapter.Exec` is a `ClaudeCode.Adapter` that
spawns the CLI through an `AgentHarness.Exec` implementation instead of a
local port. The stream-json protocol, control handshake, and
question/approval routing stay in your application's node; only the CLI
process moves. `AgentHarness.Exec.Local` reproduces local behavior, and an
application-provided exec module can place the process in a remote sandbox.

```elixir
{:ok, session} =
  AgentHarness.start_session(:claude,
    cwd: "/workspace",
    env: %{"ANTHROPIC_API_KEY" => api_key},
    provider_options: %{
      auth: :inherit,
      adapter: {
        AgentHarness.Providers.Claude.Adapter.Exec,
        exec: {MyApp.SandboxExec, sandbox: sandbox},
        cli_path: "claude"
      }
    }
  )
```

The exec adapter requires `auth: :inherit` because `:subscription` pins the
local port adapter and verifies local CLI authentication. Its spawn spec is
remote-safe: `cwd` and `cli_path` resolve in the execution environment, and
the process environment is built only from the SDK variables, the session
`env`, and `api_key` — the orchestrator's environment is never forwarded.

Two limits to plan around. The adapter does not reconnect after the exec
reports exit; transport loss fails the turn and retires the session, matching
AgentHarness's provider-loss semantics. And SDK features that expect
filesystem access next to the CLI — session history and `skills:`/plugin
materialization — operate on the orchestrator's filesystem, so do not
configure `skills:` for a session whose CLI runs elsewhere; deliver
instructions through `system_prompt` instead.

## Pi options

Pi is driven through `pi --mode rpc`. It is the smallest of the supported
harnesses: four core tools, no permission system, and no MCP. A session that
sets `mcp_servers`, `approval_policy`, or `sandbox` is rejected at
`start_session/2` rather than opened with those settings quietly dropped.

```elixir
{:ok, session} =
  AgentHarness.start_session(:pi,
    cwd: "/absolute/path/to/project",
    model: "anthropic/claude-sonnet-5",
    provider_options: %{
      tools: ["read", "grep", "find", "ls"],
      thinking: "medium",
      agent_dir: "/absolute/path/to/isolated/pi-home"
    }
  )
```

| Option           | Meaning                                                            |
| ---------------- | ------------------------------------------------------------------ |
| `auth`           | `:subscription` (default) or `:inherit`                            |
| `api_key`        | Explicit key; only valid with `auth: :inherit`                     |
| `provider`       | Pi provider name when the model pattern does not carry one         |
| `executable`     | Path or name of the `pi` binary                                    |
| `tools`          | Allowlist of tool names                                            |
| `exclude_tools`  | Denylist of tool names                                             |
| `no_tools`       | Disable all tools                                                  |
| `thinking`       | `off`, `minimal`, `low`, `medium`, `high`, `xhigh`, or `max`       |
| `extensions`     | Extension file paths to load                                       |
| `resume`         | Session file path or partial session ID to continue                |
| `fork`           | Session file path or partial session ID to branch from             |
| `session`        | `false` for an ephemeral run that is never written to disk         |
| `session_dir`    | Directory for session storage and lookup                           |
| `agent_dir`      | Sets `PI_CODING_AGENT_DIR`, isolating config and sessions          |
| `name`           | Session display name                                               |
| `offline`        | Skip pi's startup network calls                                    |

By default the harness assigns its own session id with `--session-id`, so
`provider_session_id` matches the AgentHarness session id. Setting `resume`,
`fork`, or `session: false` hands that choice back to pi, and the id is read
from pi instead.

### Questions from Pi

Pi has no built-in question tool. Dialogs reach the harness through pi's
extension UI sub-protocol, so they appear only when a loaded extension calls
`ctx.ui.confirm`, `ctx.ui.select`, `ctx.ui.input`, or `ctx.ui.editor`. Each one
arrives as a `%Request{kind: :question}`:

- `confirm` carries `true`/`false` choices. Answer with `Response.approve/0`,
  `Response.deny/0`, or `Response.answer(boolean)`.
- `select` carries the extension's options as choices.
- `input` and `editor` are free-form; answer with `Response.answer(text)`.

`Response.cancel/0` dismisses any dialog. A dialog raised while no turn is
active is dismissed automatically, because an unanswered dialog blocks pi
indefinitely.

### Steering

Pi can accept messages while a turn is running, through its `steer` and
`follow_up` commands. AgentHarness allows one active turn per session and has
no steering entry point, so the adapter reports `steer: :unsupported` and
`start_turn/3` during a running turn returns `{:error, {:turn_in_progress, turn}}`.
The protocol support is in place for when a public API lands.

## Provider differences

| Concern                 | Codex                                                                                  | Claude                                     | Pi                                            |
| ----------------------- | -------------------------------------------------------------------------------------- | ------------------------------------------ | --------------------------------------------- |
| Transport               | Codex app-server via `codex_sdk`                                                       | Bidirectional Claude CLI via `claude_code` | `pi --mode rpc` JSONL over a port             |
| Turn input              | String or structured input list                                                        | String                                     | String, or a list of strings joined by newline |
| Token deltas            | Native                                                                                 | Native                                     | Native                                        |
| Questions               | Native app-server request                                                              | Native `can_use_tool` callback             | Extension UI dialogs, only when an extension asks |
| Approvals               | Command, file, permissions, MCP elicitation                                            | Tool permission callback                   | Unsupported; pi has no permission system      |
| Session-scoped approval | Advertised for file/permission protocols; command requests follow `availableDecisions` | Explicitly unsupported; returns an error   | Not applicable                                |
| Per-session MCP         | Native                                                                                 | Native                                     | Unsupported; a configured server is rejected  |
| Skills                  | Explicit native skill input                                                            | Session plugin generated when needed       | Native `--skill` per skill path               |
| Cancellation            | Turn interrupt, then drain the authoritative terminal event                            | CLI interrupt                              | `abort`, then the aborted stop reason         |
| Resume                  | Codex thread ID                                                                        | Claude session ID                          | Session file path or partial session ID       |
| Fork                    | Unsupported in the current adapter                                                     | Native via `resume` plus `fork_session`    | Native via `fork`                             |
| Approval/sandbox config | Common session fields                                                                  | Common fields with Claude-native values    | Rejected; pi does not sandbox                 |
| Steering                | Capability currently unsupported                                                       | Capability currently unsupported           | Supported by pi, not yet exposed by the harness |
| Terminal signal         | Codex terminal turn event                                                              | Claude `ResultMessage`                     | `agent_settled`, not `agent_end`              |

Provider-specific event data remains available in `Event.raw`. Write your
orchestrator against normalized lifecycle and request events, then inspect
`raw` only where a provider-specific feature is intentional.
