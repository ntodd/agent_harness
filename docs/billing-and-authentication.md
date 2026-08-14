# Billing and authentication

AgentHarness is bring-your-own-CLI infrastructure. It launches the Codex,
Claude Code, and Pi executables already installed on your machine and relies on
their saved authentication. It does not proxy credentials, convert a subscription
into an API key, or bypass provider usage limits.

## Can this use subscription access?

As of July 31, 2026, the providers' official guidance says:

- Codex CLI can be signed into with a ChatGPT account, and Codex usage is
  included with eligible ChatGPT plans. Limits and credit options vary by plan.
  Check [Using Codex with your ChatGPT plan](https://help.openai.com/en/articles/11369540-using-codex-with-your-chatgpt-plan)
  immediately before relying on unattended volume.
- Claude Code can be connected to a Claude Pro or Max subscription. Claude and
  Claude Code share plan limits, and API-credit usage is a separate billing
  route. Check [Claude Code setup](https://docs.anthropic.com/en/docs/claude-code/getting-started)
  and [Using Claude Code with your Pro or Max plan](https://support.anthropic.com/en/articles/11145838-using-claude-code-with-your-pro-or-max-plan).
- Pi is bring-your-own-model and not tied to one vendor. Its `/login` flow
  stores OAuth credentials for subscription providers, including Claude Pro and
  Max, ChatGPT Plus and Pro, and GitHub Copilot. Whether a given subscription
  permits use through a third-party harness is that provider's decision, so
  check the same pages above plus
  [GitHub Copilot's terms](https://docs.github.com/en/copilot/responsible-use-of-github-copilot-features)
  before relying on it.

That means a personal, local AgentHarness process can use the same CLI login
that works when you run the CLI yourself, without requiring AgentHarness to
send requests through the provider API. It does not mean usage is unlimited,
free, or guaranteed to remain covered by the same plan.

Provider pricing and policy change independently of this package. The date
above makes this statement auditable; the linked provider pages are the source
of truth.

## Authentication modes

All three built-in adapters default to:

```elixir
provider_options: %{auth: :subscription}
```

This is a fail-closed mode. The adapter removes or rejects known API keys,
custom endpoints, cloud-provider selectors, and auth-sensitive overrides so
ambient configuration cannot silently select a different billing route.

Use the escape hatch only when another route is intentional:

```elixir
provider_options: %{auth: :inherit}
```

`:inherit` permits the CLI/SDK to use API credentials, cloud providers, custom
endpoints, or managed environment configuration. Any resulting charges follow
that provider's normal terms.

Remote execution also requires `:inherit`. Subscription mode's checks inspect
local state (a saved login, the CLI's credential store, local config layers),
which says nothing about the environment an `AgentHarness.Exec`
implementation would run the CLI in, so every adapter rejects its remote
execution option under `:subscription` rather than trusting an environment it
cannot verify.

Pi enforces the same policy differently, because it is bring-your-own-model
rather than tied to one account. Under `:subscription` the adapter rejects an
explicit `api_key`, refuses session `env` entries that look like credentials
(anything ending in `_API_KEY`, plus the cloud and bearer-token variables pi
documents), and confirms with `pi auth print-bearer-token` that the selected
provider actually holds an OAuth credential. That command prints the token on
stdout, so the adapter inspects only its exit status and error marker and never
returns or logs the output.

## Trust boundary

The executable, provider SDK, account policy, organization-managed settings,
and any custom client module remain outside AgentHarness's control. Verify the
same command in the same shell and working directory before debugging the
Elixir layer.

This library is intended for a user managing their own local CLI sessions. It
is not an authentication design for a hosted multi-user service. Before
offering provider access to other users, choose the provider-supported
commercial authentication route and review current terms, including
[Claude Code legal and compliance](https://code.claude.com/docs/en/legal-and-compliance)
and the applicable OpenAI terms linked from the Codex plan article.
