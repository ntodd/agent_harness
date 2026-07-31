# Billing and authentication

AgentHarness is bring-your-own-CLI infrastructure. It launches the Codex and
Claude Code executables already installed on your machine and relies on their
saved authentication. It does not proxy credentials, convert a subscription
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

That means a personal, local AgentHarness process can use the same CLI login
that works when you run the CLI yourself, without requiring AgentHarness to
send requests through the provider API. It does not mean usage is unlimited,
free, or guaranteed to remain covered by the same plan.

Provider pricing and policy change independently of this package. The date
above makes this statement auditable; the linked provider pages are the source
of truth.

## Authentication modes

Both built-in adapters default to:

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
