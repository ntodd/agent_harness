defmodule AgentHarness.Providers.Pi.ClientPortTest do
  use ExUnit.Case, async: true

  alias AgentHarness.Providers.Pi.Client

  @secret "sk-pi-spawn-secret"

  # A file that exists but is not executable passes resolve_executable and
  # makes Port.open raise. The raised error's stacktrace frame carries the
  # full port options — argv and env included — so the reported reason must
  # be reduced before it can reach a crash report or the caller.
  test "a spawn failure reports a reason without argv or env" do
    path =
      Path.join(
        System.tmp_dir!(),
        "agent-harness-not-executable-#{System.unique_integer([:positive])}"
      )

    File.write!(path, "#!/bin/sh\n")
    File.chmod!(path, 0o644)
    on_exit(fn -> File.rm(path) end)

    prepared = %{
      executable: path,
      args: ["--mode", "rpc", "--api-key", @secret],
      env: [{~c"OPENROUTER_API_KEY", String.to_charlist(@secret)}],
      cwd: nil
    }

    assert {:error, {:spawn_failed, reason}} = Client.Port.open(prepared, self())

    rendered =
      ~c"~p"
      |> :io_lib.format([reason])
      |> IO.iodata_to_binary()

    refute rendered =~ @secret
  end
end
