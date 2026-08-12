defmodule AgentHarness.SessionServerRedactionTest do
  use ExUnit.Case, async: false

  import Mox

  alias AgentHarness.ProviderMock

  setup :set_mox_global
  setup :verify_on_exit!

  @secret "sk-ant-api03-super-secret-value"

  test ":sys.get_status output never contains session env values" do
    expect(ProviderMock, :open_session, fn _config, _sink ->
      {:ok, :provider_handle, %{}}
    end)

    expect(ProviderMock, :close_session, fn :provider_handle -> :ok end)

    assert {:ok, session} =
             AgentHarness.start_session(:test,
               provider_module: ProviderMock,
               env: %{"ANTHROPIC_API_KEY" => @secret},
               provider_options: %{api_key: @secret}
             )

    pid = AgentHarness.whereis(session.id)
    status = :sys.get_status(pid)

    rendered = inspect(status, limit: :infinity, printable_limit: :infinity)

    refute rendered =~ @secret

    # format_status/1 must scrub the state term itself, not rely on the
    # Inspect protocol: crash reports can render state with Erlang ~p
    # formatting, which bypasses Inspect implementations.
    config = find_formatted_config(status)

    assert config.env == %{"ANTHROPIC_API_KEY" => "[REDACTED]"}
    refute inspect(config.provider_options, limit: :infinity) =~ @secret

    assert :ok = AgentHarness.stop_session(session)
  end

  defp find_formatted_config(term) do
    find_config(term) ||
      flunk("no SessionConfig found in :sys.get_status output")
  end

  defp find_config(%AgentHarness.SessionConfig{} = config), do: config

  defp find_config(%AgentHarness.SessionServer.State{config: config}), do: config

  defp find_config(list) when is_list(list), do: Enum.find_value(list, &find_config/1)

  defp find_config(tuple) when is_tuple(tuple),
    do: tuple |> Tuple.to_list() |> Enum.find_value(&find_config/1)

  defp find_config(_other), do: nil
end
