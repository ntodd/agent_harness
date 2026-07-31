defmodule AgentHarness.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    max_sessions = Application.get_env(:agent_harness, :max_sessions, :infinity)

    max_provider_processes =
      Application.get_env(:agent_harness, :max_provider_processes, :infinity)

    max_runner_tasks = Application.get_env(:agent_harness, :max_runner_tasks, :infinity)

    children = [
      {AgentHarness.Store.Memory, name: AgentHarness.Store.Memory},
      {Registry, keys: :unique, name: AgentHarness.SessionRegistry},
      {Task.Supervisor, name: AgentHarness.RunnerSupervisor, max_children: max_runner_tasks},
      {DynamicSupervisor,
       strategy: :one_for_one,
       name: AgentHarness.ProviderSupervisor,
       max_children: max_provider_processes},
      {DynamicSupervisor,
       strategy: :one_for_one, name: AgentHarness.SessionSupervisor, max_children: max_sessions}
    ]

    opts = [strategy: :rest_for_one, name: AgentHarness.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
