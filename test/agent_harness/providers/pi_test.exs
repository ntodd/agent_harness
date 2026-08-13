defmodule AgentHarness.Providers.PiTest do
  @moduledoc """
  Session-level tests for the Pi adapter.

  The mocked transport replays RPC frames captured from a real
  `pi --mode rpc` process, so the lifecycle assertions here reflect the
  ordering pi actually produces rather than an idealized protocol.
  """

  use ExUnit.Case, async: false

  import Mox

  alias AgentHarness.{Capabilities, Response, SessionConfig, SessionRef, Turn}
  alias AgentHarness.Provider.Sink
  alias AgentHarness.Providers.Pi, as: PiProvider
  alias AgentHarness.Providers.Pi.ClientMock

  setup :set_mox_global
  setup :verify_on_exit!

  @fixture_dir Path.expand("../../support/fixtures/pi", __DIR__)

  setup do
    session = SessionRef.new(:pi, id: "session-pi-1")
    sink = Sink.new(self())
    %{session: session, sink: sink}
  end

  defp fixture(name) do
    @fixture_dir
    |> Path.join(name)
    |> File.stream!()
    |> Enum.flat_map(fn line ->
      case String.trim(line) do
        "" -> []
        json -> [JSON.decode!(json)]
      end
    end)
  end

  defp config(session, opts \\ []) do
    provider_options =
      opts
      |> Keyword.get(:provider_options, %{})
      |> Map.merge(%{client: ClientMock, auth: :inherit})

    SessionConfig.new(session, Keyword.put(opts, :provider_options, provider_options))
  end

  # A transport stub that records outbound frames and lets a test push frames
  # back to the session, exactly as the real port process would.
  defp stub_transport(opts \\ []) do
    test = self()
    ready_id = Keyword.get(opts, :ready_id, "ah-0")
    session_id = Keyword.get(opts, :session_id, "pi-session-abc")

    transport = spawn_link(fn -> Process.sleep(:infinity) end)

    ClientMock
    |> stub(:open, fn _prepared, owner ->
      send(test, {:transport_owner, owner})
      {:ok, transport}
    end)
    |> stub(:close, fn _handle -> :ok end)
    |> stub(:send_frame, fn _handle, frame ->
      send(test, {:sent, frame})

      if frame["type"] == "get_state" and frame["id"] == ready_id do
        send(test, {:ready_probe, frame["id"]})
      end

      :ok
    end)

    %{transport: transport, session_id: session_id}
  end

  defp await_owner do
    receive do
      {:transport_owner, owner} -> owner
    after
      1_000 -> flunk("transport was never opened")
    end
  end

  defp push(owner, transport, frame) do
    send(owner, {:pi_frame, transport, frame})
  end

  defp ack_readiness(owner, transport, session_id) do
    receive do
      {:ready_probe, id} ->
        push(owner, transport, %{
          "type" => "response",
          "id" => id,
          "command" => "get_state",
          "success" => true,
          "data" => %{"sessionId" => session_id}
        })
    after
      1_000 -> flunk("session never probed readiness")
    end
  end

  defp sent_frames do
    receive do
      {:sent, frame} -> [frame | sent_frames()]
    after
      50 -> []
    end
  end

  describe "capabilities" do
    test "reports pi's actual feature set" do
      assert %Capabilities{
               token_streaming: :native,
               questions: :native,
               approvals: :unsupported,
               cancel: :native,
               steer: :unsupported,
               resume: :native,
               fork: :native,
               per_session_mcp: :unsupported,
               skills: :native
             } = PiProvider.capabilities(self())
    end
  end

  describe "open_session/2" do
    test "establishes readiness with a get_state probe and adopts pi's session id",
         %{session: session, sink: sink} do
      %{transport: transport, session_id: session_id} = stub_transport()

      task = Task.async(fn -> PiProvider.open_session(config(session), sink) end)

      owner = await_owner()
      ack_readiness(owner, transport, session_id)

      assert {:ok, server, %{provider_session_id: ^session_id}} = Task.await(task, 2_000)
      assert is_pid(server)

      PiProvider.close_session(server)
    end

    test "refuses a session configured with MCP servers", %{session: session, sink: sink} do
      assert {:error, {:unsupported, :per_session_mcp}} =
               PiProvider.open_session(
                 config(session, mcp_servers: %{"docs" => %{command: "docs-mcp"}}),
                 sink
               )
    end

    test ":sys.get_status on a live session never leaks credentials", %{
      session: session,
      sink: sink
    } do
      secret = "sk-pi-live-secret"
      %{transport: transport, session_id: session_id} = stub_transport()

      config =
        config(session,
          env: %{"OPENROUTER_API_KEY" => secret},
          provider_options: %{api_key: secret}
        )

      task = Task.async(fn -> PiProvider.open_session(config, sink) end)

      owner = await_owner()
      ack_readiness(owner, transport, session_id)

      assert {:ok, server, _info} = Task.await(task, 2_000)

      # Crash reports render the state with Erlang ~p formatting, which
      # bypasses the Inspect protocol, so the raw term must be scrubbed.
      rendered =
        ~c"~p"
        |> :io_lib.format([:sys.get_status(server)])
        |> IO.iodata_to_binary()

      refute rendered =~ secret

      PiProvider.close_session(server)
    end

    test "verifies subscription auth before spawning pi", %{session: session, sink: sink} do
      test = self()

      ClientMock
      |> expect(:verify_subscription_auth, fn _prepared, _timeout ->
        send(test, :auth_checked)
        {:error, {:subscription_auth_required, :no_oauth_credential}}
      end)
      |> stub(:open, fn _prepared, _owner -> flunk("pi must not start without auth") end)

      config =
        SessionConfig.new(session,
          provider_options: %{client: ClientMock, auth: :subscription}
        )

      assert {:error, {:subscription_auth_required, :no_oauth_credential}} =
               PiProvider.open_session(config, sink)

      assert_received :auth_checked
    end
  end

  describe "turn lifecycle" do
    setup %{session: session, sink: sink} do
      %{transport: transport, session_id: session_id} = stub_transport()
      task = Task.async(fn -> PiProvider.open_session(config(session), sink) end)
      owner = await_owner()
      ack_readiness(owner, transport, session_id)
      {:ok, server, _info} = Task.await(task, 2_000)
      _drained = sent_frames()

      on_exit(fn -> PiProvider.close_session(server) end)
      %{server: server, owner: owner, transport: transport}
    end

    test "a prompt is acknowledged before events start flowing", %{
      server: server,
      owner: owner,
      transport: transport,
      session: session
    } do
      turn = Turn.new(session.id, "say hi", id: "turn-1")
      task = Task.async(fn -> PiProvider.start_turn(server, turn, "say hi", []) end)

      prompt = await_frame(fn frame -> frame["type"] == "prompt" end)
      assert prompt["message"] == "say hi"

      push(owner, transport, %{
        "type" => "response",
        "id" => prompt["id"],
        "command" => "prompt",
        "success" => true
      })

      assert {:ok, provider_turn_ref} = Task.await(task, 2_000)
      assert is_reference(provider_turn_ref)
    end

    test "a rejected prompt is a definite failure, not an uncertain start", %{
      server: server,
      owner: owner,
      transport: transport,
      session: session
    } do
      turn = Turn.new(session.id, "hi", id: "turn-2")
      task = Task.async(fn -> PiProvider.start_turn(server, turn, "hi", []) end)

      prompt = await_frame(fn frame -> frame["type"] == "prompt" end)

      push(owner, transport, %{
        "type" => "response",
        "id" => prompt["id"],
        "command" => "prompt",
        "success" => false,
        "error" => "No API key found for the selected model."
      })

      assert {:error, {:pi_prompt_rejected, "No API key found for the selected model."}} =
               Task.await(task, 2_000)
    end

    test "replaying a recorded text turn streams deltas and finishes as completed", %{
      server: server,
      owner: owner,
      transport: transport,
      session: session
    } do
      start_turn!(server, owner, transport, session, "turn-3")

      Enum.each(fixture("text_turn.jsonl"), fn frame ->
        unless frame["type"] == "response", do: push(owner, transport, frame)
      end)

      assert collect_delta_text("turn-3") == "pong"

      assert_receive {:agent_harness_provider, _ref,
                      {:finish, "turn-3", :completed, _result, _raw}},
                     2_000
    end

    test "an aborted run finishes as cancelled even though the ack trails the settle", %{
      server: server,
      owner: owner,
      transport: transport,
      session: session
    } do
      start_turn!(server, owner, transport, session, "turn-4")

      Enum.each(fixture("abort_midstream.jsonl"), fn frame ->
        unless frame["type"] == "response", do: push(owner, transport, frame)
      end)

      assert_receive {:agent_harness_provider, _ref,
                      {:finish, "turn-4", :cancelled, _result, _raw}},
                     2_000
    end

    test "cancel sends an abort for the active turn", %{
      server: server,
      owner: owner,
      transport: transport,
      session: session
    } do
      ref = start_turn!(server, owner, transport, session, "turn-5")

      assert :ok = PiProvider.cancel(server, ref)
      assert await_frame(fn frame -> frame["type"] == "abort" end)
    end

    test "cancel rejects a turn reference the session does not own", %{server: server} do
      assert {:error, :unknown_turn} = PiProvider.cancel(server, make_ref())
    end

    test "a second turn is refused while one is active", %{
      server: server,
      owner: owner,
      transport: transport,
      session: session
    } do
      start_turn!(server, owner, transport, session, "turn-6")

      turn = Turn.new(session.id, "again", id: "turn-7")
      assert {:error, :turn_already_active} = PiProvider.start_turn(server, turn, "again", [])
    end
  end

  describe "extension UI dialogs" do
    setup %{session: session, sink: sink} do
      %{transport: transport, session_id: session_id} = stub_transport()
      task = Task.async(fn -> PiProvider.open_session(config(session), sink) end)
      owner = await_owner()
      ack_readiness(owner, transport, session_id)
      {:ok, server, _info} = Task.await(task, 2_000)
      _drained = sent_frames()

      on_exit(fn -> PiProvider.close_session(server) end)
      %{server: server, owner: owner, transport: transport}
    end

    test "a confirm dialog becomes a request the caller can approve", %{
      server: server,
      owner: owner,
      transport: transport,
      session: session
    } do
      start_turn!(server, owner, transport, session, "turn-8")

      push(owner, transport, %{
        "type" => "extension_ui_request",
        "id" => "dialog-1",
        "method" => "confirm",
        "title" => "Allow bash?",
        "message" => "{\"command\":\"echo hi\"}"
      })

      assert_receive {:agent_harness_provider, _ref,
                      {:request, "turn-8", "dialog-1", attrs, _raw}},
                     2_000

      assert attrs[:kind] == :question

      assert :ok = PiProvider.respond(server, "dialog-1", Response.approve())

      answer = await_frame(fn frame -> frame["type"] == "extension_ui_response" end)

      assert answer == %{
               "type" => "extension_ui_response",
               "id" => "dialog-1",
               "confirmed" => true
             }
    end

    test "a dialog raised with no active turn is dismissed so pi is not left blocked", %{
      owner: owner,
      transport: transport
    } do
      push(owner, transport, %{
        "type" => "extension_ui_request",
        "id" => "orphan-1",
        "method" => "confirm",
        "title" => "Stuck?"
      })

      answer = await_frame(fn frame -> frame["type"] == "extension_ui_response" end)

      assert answer == %{
               "type" => "extension_ui_response",
               "id" => "orphan-1",
               "cancelled" => true
             }
    end

    test "responding to an unknown request is rejected", %{server: server} do
      assert {:error, :unknown_request} =
               PiProvider.respond(server, "nope", Response.approve())
    end
  end

  describe "transport loss" do
    test "reports the provider going away", %{session: session, sink: sink} do
      %{transport: transport, session_id: session_id} = stub_transport()
      task = Task.async(fn -> PiProvider.open_session(config(session), sink) end)
      owner = await_owner()
      ack_readiness(owner, transport, session_id)
      {:ok, _server, _info} = Task.await(task, 2_000)

      send(owner, {:pi_down, transport, {:exit_status, 1}})

      assert_receive {:agent_harness_provider, _ref,
                      {:transport_down, {:pi_transport_down, {:exit_status, 1}}}},
                     2_000
    end
  end

  defp start_turn!(server, owner, transport, session, turn_id) do
    turn = Turn.new(session.id, "work", id: turn_id)
    task = Task.async(fn -> PiProvider.start_turn(server, turn, "work", []) end)
    prompt = await_frame(fn frame -> frame["type"] == "prompt" end)

    push(owner, transport, %{
      "type" => "response",
      "id" => prompt["id"],
      "command" => "prompt",
      "success" => true
    })

    {:ok, ref} = Task.await(task, 2_000)
    ref
  end

  defp await_frame(predicate, deadline \\ 2_000) do
    receive do
      {:sent, frame} ->
        if predicate.(frame), do: frame, else: await_frame(predicate, deadline)
    after
      deadline -> flunk("expected frame never sent")
    end
  end

  defp collect_delta_text(turn_id, acc \\ "") do
    receive do
      {:agent_harness_provider, _ref, {:event, ^turn_id, :message_delta, %{text: text}, _raw}} ->
        collect_delta_text(turn_id, acc <> text)
    after
      500 -> acc
    end
  end
end
