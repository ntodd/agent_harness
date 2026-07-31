defmodule AgentHarness.Providers.Codex.ConnectionProxyTest do
  use ExUnit.Case, async: true

  alias AgentHarness.Providers.Codex.ConnectionProxy
  alias Codex.AppServer.Connection
  alias Codex.AppServer.NotificationAdapter
  alias Codex.Events

  defmodule FakeConnection do
    use GenServer

    def start_link(owner), do: GenServer.start_link(__MODULE__, owner)

    def notify(pid, method, params),
      do: GenServer.cast(pid, {:notify, method, params})

    def peer_request(pid, id, method, params),
      do: GenServer.cast(pid, {:peer_request, id, method, params})

    @impl true
    def init(owner),
      do: {:ok, %{owner: owner, subscribers: MapSet.new(), pending_request: nil}}

    @impl true
    def handle_call(:await_ready, _from, state), do: {:reply, :ok, state}

    def handle_call({:subscribe, pid, _opts}, _from, state) do
      {:reply, :ok, %{state | subscribers: MapSet.put(state.subscribers, pid)}}
    end

    def handle_call({:unsubscribe, pid}, _from, state) do
      {:reply, :ok, %{state | subscribers: MapSet.delete(state.subscribers, pid)}}
    end

    def handle_call({:request, "request/with-peer", params, _timeout}, from, state) do
      send(state.owner, {:request_waiting_for_peer, params})
      {:noreply, %{state | pending_request: from}}
    end

    def handle_call({:request, method, params, _timeout}, _from, state) do
      send(state.owner, {:request, method, params})
      {:reply, {:ok, %{"accepted" => true}}, state}
    end

    def handle_call({:respond, id, result}, _from, state) do
      send(state.owner, {:response, id, result})
      {:reply, :ok, state}
    end

    def handle_call({:respond_error, id, code, message, data}, _from, state) do
      send(state.owner, {:response_error, id, code, message, data})
      {:reply, :ok, state}
    end

    @impl true
    def handle_cast({:notify, method, params}, state) do
      Enum.each(state.subscribers, &send(&1, {:codex_notification, method, params}))
      {:noreply, state}
    end

    def handle_cast({:peer_request, id, method, params}, state) do
      Enum.each(state.subscribers, &send(&1, {:codex_request, id, method, params}))
      {:noreply, state}
    end

    def handle_cast(:complete_request, state) do
      GenServer.reply(state.pending_request, {:ok, %{"completed" => true}})
      {:noreply, %{state | pending_request: nil}}
    end
  end

  test "repairs malformed activeFlags before the SDK notification adapter sees them" do
    {:ok, real} = FakeConnection.start_link(self())
    {:ok, proxy} = start_proxy(real)
    assert :ok = Connection.subscribe(proxy, thread_id: "thread-1")

    params = %{
      "threadId" => "thread-1",
      "status" => %{"type" => "active", "activeFlags" => ""}
    }

    FakeConnection.notify(real, "thread/status/changed", params)

    assert_receive {:codex_notification, "thread/status/changed", repaired}
    assert get_in(repaired, ["status", "activeFlags"]) == []

    assert {:ok, %Events.ThreadStatusChanged{status: status}} =
             NotificationAdapter.to_event("thread/status/changed", repaired)

    assert status == %{type: :active, active_flags: []}
    assert :ok = ConnectionProxy.disconnect(proxy)
  end

  test "preserves valid flags, request/response traffic, filtering, and peer requests" do
    {:ok, real} = FakeConnection.start_link(self())
    {:ok, proxy} = start_proxy(real)
    assert :ok = Connection.subscribe(proxy, thread_id: "thread-1")

    valid = %{
      "threadId" => "thread-1",
      "status" => %{"type" => "active", "activeFlags" => ["waitingOnApproval"]}
    }

    FakeConnection.notify(real, "thread/status/changed", valid)
    assert_receive {:codex_notification, "thread/status/changed", ^valid}

    FakeConnection.notify(real, "turn/started", %{"threadId" => "other"})
    refute_receive {:codex_notification, "turn/started", _}

    assert {:ok, %{"accepted" => true}} =
             Connection.request(proxy, "turn/interrupt", %{"turnId" => "turn-1"})

    assert_receive {:request, "turn/interrupt", %{"turnId" => "turn-1"}}

    assert :ok = Connection.respond(proxy, 17, %{"decision" => "accept"})
    assert_receive {:response, 17, %{"decision" => "accept"}}

    FakeConnection.peer_request(real, 22, "item/tool/call", %{"threadId" => "thread-1"})

    assert_receive {:codex_request, 22, "item/tool/call", %{"threadId" => "thread-1"}}
    assert :ok = ConnectionProxy.disconnect(proxy)
  end

  test "disconnect tears down the wrapped connection" do
    {:ok, real} = FakeConnection.start_link(self())
    real_monitor = Process.monitor(real)
    {:ok, proxy} = start_proxy(real)

    assert :ok = ConnectionProxy.disconnect(proxy)
    assert_receive {:DOWN, ^real_monitor, :process, ^real, :normal}
    refute Process.alive?(proxy)
  end

  test "an outgoing request does not block an interleaved peer request" do
    {:ok, real} = FakeConnection.start_link(self())
    {:ok, proxy} = start_proxy(real)
    assert :ok = Connection.subscribe(proxy, thread_id: "thread-1")

    request_task =
      Task.async(fn ->
        Connection.request(proxy, "request/with-peer", %{"phase" => "start"})
      end)

    assert_receive {:request_waiting_for_peer, %{"phase" => "start"}}

    FakeConnection.peer_request(real, 91, "item/tool/call", %{"threadId" => "thread-1"})

    assert_receive {:codex_request, 91, "item/tool/call", %{"threadId" => "thread-1"}}

    GenServer.cast(real, :complete_request)
    assert Task.await(request_task) == {:ok, %{"completed" => true}}
    assert :ok = ConnectionProxy.disconnect(proxy)
  end

  test "intercepts unresolvable peer requests for the owner even with a turn subscriber" do
    {:ok, real} = FakeConnection.start_link(self())
    {:ok, proxy} = start_proxy(real)
    assert :ok = Connection.subscribe(proxy, thread_id: "thread-1")

    params = %{"reason" => "expired"}

    FakeConnection.peer_request(
      real,
      101,
      "account/chatgptAuthTokens/refresh",
      params
    )

    assert_receive {:codex_unresolvable_request, 101, "account/chatgptAuthTokens/refresh",
                    ^params}

    refute_receive {:codex_request, 101, _, _}
    assert :ok = ConnectionProxy.disconnect(proxy)
  end

  defp start_proxy(real) do
    ConnectionProxy.start_link(real,
      disconnect: fn pid ->
        GenServer.stop(pid, :normal)
        :ok
      end
    )
  end
end
