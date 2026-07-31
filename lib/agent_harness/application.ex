defmodule AgentHarness.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {AgentHarness.Store.Memory, name: AgentHarness.Store.Memory},
      {Registry, keys: :unique, name: AgentHarness.SessionRegistry},
      {DynamicSupervisor, strategy: :one_for_one, name: AgentHarness.SessionSupervisor},
      {DynamicSupervisor, strategy: :one_for_one, name: AgentHarness.ProviderSupervisor},
      {Task.Supervisor, name: AgentHarness.RunnerSupervisor}
    ]

    opts = [strategy: :rest_for_one, name: AgentHarness.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
