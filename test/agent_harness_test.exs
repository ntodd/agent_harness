defmodule AgentHarnessTest do
  use ExUnit.Case
  doctest AgentHarness

  test "greets the world" do
    assert AgentHarness.hello() == :world
  end
end
