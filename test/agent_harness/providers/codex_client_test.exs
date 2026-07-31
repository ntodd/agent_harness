defmodule AgentHarness.Providers.Codex.ClientTest do
  use ExUnit.Case, async: true

  alias AgentHarness.Providers.Codex.Client

  test "constructing a raw turn stream does not allocate linked queue/control processes" do
    {:ok, options} = Codex.Options.new(%{api_key: false})

    {:ok, thread} =
      Codex.start_thread(options, %{transport: {:app_server, self()}})

    {:links, links_before} = Process.info(self(), :links)

    assert {:ok, stream} =
             Client.SDK.run_streamed(thread, [%{type: :text, text: "hello"}], %{})

    assert is_function(stream, 2)
    assert Enumerable.impl_for(stream)
    assert Client.SDK.raw_events(stream) == stream
    assert :ok = Client.SDK.cancel_stream(stream, :immediate)

    {:links, links_after} = Process.info(self(), :links)
    assert MapSet.new(links_after) == MapSet.new(links_before)
  end
end
