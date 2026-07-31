defmodule AgentHarness.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: AgentHarness.SessionRegistry},
      {DynamicSupervisor, strategy: :one_for_one, name: AgentHarness.SessionSupervisor},
      {Task.Supervisor, name: AgentHarness.RunnerSupervisor}
    ]

    opts = [strategy: :one_for_one, name: AgentHarness.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
