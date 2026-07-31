defmodule AgentHarness.MixProject do
  use Mix.Project

  def project do
    [
      app: :agent_harness,
      name: "AgentHarness",
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: docs(),
      elixirc_paths: elixirc_paths(Mix.env()),
      aliases: aliases(),
      description: "Supervised Elixir sessions for local coding-agent harnesses"
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:crypto, :logger],
      mod: {AgentHarness.Application, []}
    ]
  end

  def cli do
    [preferred_envs: [precommit: :test]]
  end

  defp deps do
    [
      {:claude_code, "~> 0.36.5"},
      {:codex_sdk, "~> 0.18.1"},
      {:telemetry, "~> 1.3"},
      {:mox, "~> 1.2", only: :test},
      {:stream_data, "~> 1.2", only: [:dev, :test]},
      {:ex_doc, "~> 0.38", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  defp docs do
    guides = [
      "docs/getting-started.md",
      "docs/configuration.md",
      "docs/genserver-integration.md",
      "docs/lifecycle-and-events.md",
      "docs/architecture.md",
      "docs/billing-and-authentication.md",
      "docs/testing.md"
    ]

    [
      main: "readme",
      extras: ["README.md", "CHANGELOG.md" | guides],
      groups_for_extras: [Guides: guides]
    ]
  end

  defp aliases do
    [
      precommit: [
        "format --check-formatted",
        "compile --warnings-as-errors",
        "test",
        "credo --strict"
      ]
    ]
  end
end
