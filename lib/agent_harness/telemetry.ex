defmodule AgentHarness.Telemetry do
  @moduledoc false

  @prefix [:agent_harness]

  @spec start([atom()], map()) :: integer()
  def start(name, metadata \\ %{}) do
    :telemetry.execute(
      @prefix ++ name ++ [:start],
      %{system_time: System.system_time()},
      metadata
    )

    System.monotonic_time()
  end

  @spec stop([atom()], integer(), map()) :: :ok
  def stop(name, started_at, metadata \\ %{}) do
    :telemetry.execute(
      @prefix ++ name ++ [:stop],
      %{duration: System.monotonic_time() - started_at},
      metadata
    )
  end

  @spec event([atom()], map(), map()) :: :ok
  def event(name, measurements \\ %{}, metadata \\ %{}) do
    :telemetry.execute(@prefix ++ name, measurements, metadata)
  end

  @spec span([atom()], map(), (-> result)) :: result when result: term()
  def span(name, metadata, function) when is_function(function, 0) do
    started_at = start(name, metadata)

    try do
      result = function.()
      stop(name, started_at, Map.put(metadata, :result, outcome(result)))
      result
    rescue
      error ->
        exception(name, started_at, metadata, :error, error, __STACKTRACE__)
        reraise(error, __STACKTRACE__)
    catch
      kind, reason ->
        exception(name, started_at, metadata, kind, reason, __STACKTRACE__)
        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  defp exception(name, started_at, metadata, kind, reason, stacktrace) do
    :telemetry.execute(
      @prefix ++ name ++ [:exception],
      %{duration: System.monotonic_time() - started_at},
      Map.merge(metadata, %{kind: kind, reason: reason, stacktrace: stacktrace})
    )
  end

  defp outcome({:ok, _value}), do: :ok
  defp outcome(:ok), do: :ok
  defp outcome({:error, _reason}), do: :error
  defp outcome(_other), do: :ok
end
