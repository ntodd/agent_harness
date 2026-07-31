defmodule AgentHarness.Providers.Claude.Client do
  @moduledoc false

  @type session :: pid()

  @callback verify_subscription_auth(keyword(), pos_integer()) :: :ok | {:error, term()}
  @callback start_link(keyword()) :: GenServer.on_start()
  @callback await_ready(session(), pos_integer()) :: :ok | {:error, term()}
  @callback stream(session(), String.t(), keyword()) :: Enumerable.t()
  @callback session_id(session()) :: String.t() | nil
  @callback interrupt(session()) :: :ok | {:error, term()}
  @callback stop(session()) :: :ok
end

defmodule AgentHarness.Providers.Claude.Client.Default do
  @moduledoc false

  @behaviour AgentHarness.Providers.Claude.Client

  alias ClaudeCode.Adapter.Port.Resolver

  @impl true
  def verify_subscription_auth(opts, timeout) do
    case bounded_call(fn -> auth_status(opts) end, timeout) do
      {:ok, {output, 0}} ->
        verify_auth_status(output)

      {:ok, {_output, exit_status}} when is_integer(exit_status) ->
        {:error, {:claude_auth_status_failed, exit_status}}

      {:ok, {:error, reason}} ->
        {:error, {:claude_auth_status_failed, reason}}

      {:error, reason} ->
        {:error, {:claude_auth_status_failed, reason}}

      :timeout ->
        {:error, :claude_auth_status_timeout}
    end
  end

  @impl true
  def start_link(opts), do: ClaudeCode.start_link(opts)

  @impl true
  def await_ready(session, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    await_ready_until(session, deadline)
  end

  @impl true
  def stream(session, prompt, opts), do: ClaudeCode.stream(session, prompt, opts)

  @impl true
  def session_id(session), do: ClaudeCode.Session.session_id(session)

  @impl true
  def interrupt(session), do: ClaudeCode.Session.interrupt(session)

  @impl true
  def stop(session), do: ClaudeCode.stop(session)

  defp await_ready_until(session, deadline) do
    case readiness_status(session, deadline) do
      :ready ->
        :ok

      {:error, reason} ->
        {:error, {:claude_readiness_failed, reason}}

      {:timeout, status} ->
        {:error, {:claude_readiness_timeout, status}}

      {:pending, status} ->
        remaining = deadline - System.monotonic_time(:millisecond)

        if remaining > 0 do
          Process.sleep(min(25, remaining))
          await_ready_until(session, deadline)
        else
          {:error, {:claude_readiness_timeout, status}}
        end
    end
  end

  defp readiness_status(session, deadline) do
    case bounded_call(
           fn -> ClaudeCode.Session.server_info(session) end,
           remaining(deadline)
         ) do
      {:ok, {:ok, info}} when not is_nil(info) ->
        :ready

      {:ok, {:ok, nil}} ->
        health_status(session, deadline)

      {:ok, {:error, reason}} ->
        {:error, reason}

      {:ok, other} ->
        {:error, {:unexpected_server_info, other}}

      {:error, reason} ->
        {:error, {:session_down, reason}}

      :timeout ->
        {:timeout, :server_info}
    end
  end

  defp health_status(session, deadline) do
    case bounded_call(fn -> ClaudeCode.Session.health(session) end, remaining(deadline)) do
      {:ok, :healthy} -> {:pending, :initializing}
      {:ok, {:unhealthy, :provisioning}} -> {:pending, :provisioning}
      {:ok, {:unhealthy, reason}} -> {:error, reason}
      {:ok, {:error, reason}} -> {:error, reason}
      {:ok, other} -> {:error, {:unexpected_health, other}}
      {:error, reason} -> {:error, {:session_down, reason}}
      :timeout -> {:timeout, :health}
    end
  end

  defp auth_status(opts) do
    case Resolver.find_binary(opts) do
      {:ok, path} ->
        System.cmd(
          path,
          ["auth", "status"],
          command_options(opts)
        )

      {:error, reason} ->
        {:error, {:cli_not_found, reason}}
    end
  end

  defp command_options(opts) do
    options = [
      stderr_to_stdout: true,
      env:
        opts
        |> Keyword.get(:env, %{})
        |> Enum.map(fn
          {key, false} -> {to_string(key), nil}
          {key, value} -> {to_string(key), to_string(value)}
        end)
    ]

    case Keyword.get(opts, :cwd) do
      nil -> options
      cwd -> Keyword.put(options, :cd, cwd)
    end
  end

  defp verify_auth_status(output) do
    case JSON.decode(output) do
      {:ok,
       %{
         "loggedIn" => true,
         "authMethod" => "claude.ai",
         "apiProvider" => "firstParty"
       }} ->
        :ok

      {:ok, status} when is_map(status) ->
        {:error,
         {:subscription_auth_required,
          Map.take(status, ["loggedIn", "authMethod", "apiProvider", "subscriptionType"])}}

      {:ok, _other} ->
        {:error, :invalid_claude_auth_status}

      {:error, _reason} ->
        {:error, :invalid_claude_auth_status}
    end
  end

  defp bounded_call(_function, timeout) when timeout <= 0, do: :timeout

  defp bounded_call(function, timeout) do
    task =
      Task.Supervisor.async_nolink(AgentHarness.RunnerSupervisor, function)

    case Task.yield(task, timeout) do
      {:ok, result} ->
        {:ok, result}

      {:exit, reason} ->
        {:error, reason}

      nil ->
        _ = Task.shutdown(task, :brutal_kill)
        :timeout
    end
  catch
    :exit, reason -> {:error, reason}
  end

  defp remaining(deadline) do
    max(deadline - System.monotonic_time(:millisecond), 0)
  end
end
