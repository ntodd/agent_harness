defmodule AgentHarness.EventBufferTest do
  use ExUnit.Case, async: true

  alias AgentHarness.EventBuffer

  test "keeps events in order and evicts the oldest event at capacity" do
    buffer =
      EventBuffer.new(2)
      |> EventBuffer.push(%{seq: 1})
      |> EventBuffer.push(%{seq: 2})
      |> EventBuffer.push(%{seq: 3})

    assert EventBuffer.to_list(buffer) == [%{seq: 2}, %{seq: 3}]
  end

  test "reads from an inclusive sequence cursor" do
    buffer =
      EventBuffer.new(5)
      |> EventBuffer.push(%{seq: 1})
      |> EventBuffer.push(%{seq: 2})
      |> EventBuffer.push(%{seq: 3})

    assert EventBuffer.from(buffer, 2) == [%{seq: 2}, %{seq: 3}]
    assert EventBuffer.from(buffer, :latest) == []
    assert EventBuffer.from(buffer, :start) == [%{seq: 1}, %{seq: 2}, %{seq: 3}]
  end

  test "requires a positive capacity" do
    assert_raise ArgumentError, ~r/capacity/, fn -> EventBuffer.new(0) end
  end
end
