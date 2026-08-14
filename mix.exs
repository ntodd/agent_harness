defmodule AgentHarness.MixProject do
  use Mix.Project

  @version "0.3.0"
  @source_url "https://github.com/ntodd/agent_harness"

  def project do
    [
      app: :agent_harness,
      name: "AgentHarness",
      version: @version,
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: docs(),
      package: package(),
      elixirc_paths: elixirc_paths(Mix.env()),
      aliases: aliases(),
      description: "Supervised Elixir sessions for coding-agent harnesses",
      source_url: @source_url,
      homepage_url: @source_url
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
      {:codex_sdk, "~> 0.19.0"},
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
      "docs/writing-an-exec-implementation.md",
      "docs/billing-and-authentication.md",
      "docs/testing.md"
    ]

    [
      main: "readme",
      extras: ["README.md", "CHANGELOG.md", "LICENSE" | guides],
      groups_for_extras: [Guides: guides, Project: ["CHANGELOG.md", "LICENSE"]],
      before_closing_body_tag: &mermaid_renderer/1,
      source_ref: "v#{@version}",
      source_url: @source_url
    ]
  end

  defp mermaid_renderer(:html) do
    """
    <script defer src="https://cdn.jsdelivr.net/npm/mermaid@11.16.0/dist/mermaid.min.js"></script>
    <script>
      let mermaidInitialized = false;

      window.addEventListener("exdoc:loaded", () => {
        if (!mermaidInitialized) {
          mermaid.initialize({
            startOnLoad: false,
            theme: document.body.className.includes("dark") ? "dark" : "default"
          });
          mermaidInitialized = true;
        }

        let mermaidId = 0;
        for (const codeEl of document.querySelectorAll("pre code.mermaid")) {
          const preEl = codeEl.parentElement;
          const graphDefinition = codeEl.textContent;
          const graphEl = document.createElement("div");
          const graphId = "mermaid-graph-" + mermaidId++;

          mermaid.render(graphId, graphDefinition).then(({svg, bindFunctions}) => {
            graphEl.innerHTML = svg;
            bindFunctions?.(graphEl);
            preEl.insertAdjacentElement("afterend", graphEl);
            preEl.remove();
          });
        }
      });
    </script>
    """
  end

  defp mermaid_renderer(:epub), do: ""

  defp package do
    [
      files: ["lib", "docs", "mix.exs", "README.md", "CHANGELOG.md", "LICENSE"],
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url}
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
