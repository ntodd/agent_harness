defmodule AgentHarness.Provider.SinkTest do
  use ExUnit.Case, async: true

  alias AgentHarness.Provider.Sink

  test "terminal event names are reserved for finish/5" do
    sink = Sink.new(self())

    for type <- [:turn_completed, :turn_failed, :turn_cancelled, :turn_interrupted] do
      assert {:error, :reserved_event_type} = Sink.emit(sink, "turn-1", type)
    end

    refute_receive {:agent_harness_provider, _ref, _message}
  end
end
