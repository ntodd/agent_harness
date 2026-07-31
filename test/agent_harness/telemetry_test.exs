defmodule AgentHarness.TelemetryTest do
  use ExUnit.Case, async: false

  import Mox

  alias AgentHarness.{Provider, ProviderMock}

  setup :set_mox_global
  setup :verify_on_exit!

  test "emits lifecycle and Store telemetry without prompt contents" do
    test_pid = self()
    handler_id = {__MODULE__, make_ref()}

    events = [
      [:agent_harness, :session, :start],
      [:agent_harness, :session, :stop],
      [:agent_harness, :turn, :start],
      [:agent_harness, :turn, :stop],
      [:agent_harness, :store, :write, :stop]
    ]

    :ok =
      :telemetry.attach_many(handler_id, events, &__MODULE__.handle_event/4, test_pid)

    on_exit(fn -> :telemetry.detach(handler_id) end)

    expect(ProviderMock, :open_session, fn _config, sink ->
      send(test_pid, {:sink, sink})
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :start_turn, fn :provider_handle, _turn, "secret prompt", [] ->
      {:ok, "provider-turn"}
    end)

    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    {:ok, session} = AgentHarness.start_session(:test, provider_module: ProviderMock)
    assert_receive {:sink, sink}
    {:ok, turn} = AgentHarness.start_turn(session, "secret prompt")
    Provider.Sink.finish(sink, turn.id, :completed)
    assert {:ok, %{status: :completed}} = AgentHarness.await(turn, timeout: 1_000)
    assert :ok = AgentHarness.stop_session(session)

    assert_receive {:telemetry, [:agent_harness, :session, :start], _, %{provider: :test}}
    assert_receive {:telemetry, [:agent_harness, :session, :stop], %{duration: _}, _}

    assert_receive {:telemetry, [:agent_harness, :turn, :start], _,
                    %{session_id: session_id, turn_id: turn_id}}

    assert session_id == session.id
    assert turn_id == turn.id

    assert_receive {:telemetry, [:agent_harness, :turn, :stop], %{duration: _},
                    %{status: :completed}}

    assert_receive {:telemetry, [:agent_harness, :store, :write, :stop], _, store_metadata}
    refute inspect(store_metadata) =~ "secret prompt"
  end

  def handle_event(event, measurements, metadata, pid) do
    send(pid, {:telemetry, event, measurements, metadata})
  end
end
