ExUnit.start(exclude: [:live])

Mox.defmock(AgentHarness.ExecMock, for: AgentHarness.Exec)
