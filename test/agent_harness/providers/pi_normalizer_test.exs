defmodule AgentHarness.Providers.PiNormalizerTest do
  @moduledoc """
  Normalizer tests replay RPC frames recorded from a real `pi --mode rpc`
  process (pi 0.83.0). Fixtures live in `test/support/fixtures/pi` and are
  verbatim protocol output, so these assertions track what pi actually emits
  rather than what its docs describe.
  """

  use ExUnit.Case, async: true

  alias AgentHarness.Providers.Pi.Normalizer

  @fixture_dir Path.expand("../../support/fixtures/pi", __DIR__)

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

  defp normalize_all(name), do: Enum.map(fixture(name), &Normalizer.normalize/1)

  defp events(normalized, type) do
    for {:event, ^type, data} <- normalized, do: data
  end

  describe "text turns" do
    test "streams assistant text as message deltas" do
      normalized = normalize_all("text_turn.jsonl")

      assert normalized |> events(:message_delta) |> Enum.map_join(& &1.text) == "pong"
    end

    test "reports the completed assistant message with usage" do
      [completed] = normalize_all("text_turn.jsonl") |> events(:message_completed)

      assert completed.text == "pong"
      assert completed.usage["input"] == 383
      assert completed.usage["output"] == 2
    end

    test "a clean run stops as completed and then settles" do
      normalized = normalize_all("text_turn.jsonl")

      assert Enum.any?(normalized, &match?({:stopped, :completed, _data}, &1))
      assert List.last(normalized) == {:settle, %{}}
    end

    test "acknowledges the prompt command before any agent event" do
      [first | _rest] = normalize_all("text_turn.jsonl")

      assert {:response, "p1", %{command: "prompt", success: true}} = first
    end
  end

  describe "cancellation" do
    test "an aborted run stops as cancelled" do
      normalized = normalize_all("abort_midstream.jsonl")

      assert Enum.any?(normalized, &match?({:stopped, :cancelled, _data}, &1))
    end

    test "the abort acknowledgement arrives after the run settles" do
      normalized = normalize_all("abort_midstream.jsonl")

      settle_index = Enum.find_index(normalized, &match?({:settle, _data}, &1))

      ack_index =
        Enum.find_index(normalized, &match?({:response, _id, %{command: "abort"}}, &1))

      assert is_integer(settle_index)
      assert is_integer(ack_index)
      assert ack_index > settle_index
    end
  end

  describe "tool execution" do
    test "surfaces tool start, progress, and completion" do
      normalized = normalize_all("tool_turn.jsonl")

      assert [%{tool_name: "bash", args: %{"command" => "echo tool-probe-ok"}} | _] =
               events(normalized, :tool_started)

      assert [_ | _] = events(normalized, :tool_progress)

      assert [%{tool_name: "bash", error?: false} | _] = events(normalized, :tool_completed)
    end

    test "a blocked tool completes as an error carrying the reason" do
      [completed | _rest] = normalize_all("tool_denied.jsonl") |> events(:tool_completed)

      assert completed.error?

      assert completed.result["content"] == [
               %{"type" => "text", "text" => "Denied by AgentHarness probe"}
             ]
    end
  end

  describe "extension UI dialogs" do
    test "a confirm dialog becomes an answerable question request" do
      [request] =
        for {:request, id, attrs} <- normalize_all("extension_ui_dialogs.jsonl"),
            attrs[:metadata][:method] == "confirm",
            do: {id, attrs}

      {id, attrs} = request

      assert id == "ada9d8e0-8aff-45bb-9937-82d1b9d8dd48"
      assert attrs[:kind] == :question
      assert attrs[:prompt] =~ "Run dangerous command?"

      assert Enum.map(attrs[:choices], & &1.value) == [true, false]
    end

    test "a select dialog carries its options as choices" do
      [{_id, attrs}] =
        for {:request, id, attrs} <- normalize_all("extension_ui_dialogs.jsonl"),
            attrs[:metadata][:method] == "select",
            do: {id, attrs}

      assert Enum.map(attrs[:choices], & &1.value) == ["Postgres", "SQLite", "MySQL"]
    end

    test "a free-form input dialog has no fixed choices" do
      [{_id, attrs}] =
        for {:request, id, attrs} <- normalize_all("extension_ui_dialogs.jsonl"),
            attrs[:metadata][:method] == "input",
            do: {id, attrs}

      assert attrs[:choices] == []
      assert attrs[:metadata][:placeholder] == "my-app"
    end

    test "fire-and-forget notifications are events, not requests" do
      normalized = normalize_all("extension_ui_dialogs.jsonl")

      refute Enum.any?(normalized, fn
               {:request, _id, attrs} -> attrs[:metadata][:method] == "notify"
               _other -> false
             end)

      assert [%{level: "info"} | _] = events(normalized, :provider_notice)
    end
  end

  describe "queueing" do
    test "a queued follow-up is reported with its pending messages" do
      [queued | _rest] =
        normalize_all("midstream_prompt_rejected.jsonl") |> events(:queue_updated)

      assert queued.follow_up == ["queued via followUp"]
      assert queued.steering == []
    end

    test "a bare prompt during streaming is a failed command response" do
      failures =
        for {:response, id, %{success: false} = data} <-
              normalize_all("midstream_prompt_rejected.jsonl"),
            do: {id, data.error}

      assert [{"p2", error}] = failures
      assert error =~ "Agent is already processing"
    end
  end

  describe "protocol errors" do
    test "malformed input is reported as a parse failure with no request id" do
      [failure] =
        for {:response, id, %{command: "parse"} = data} <-
              normalize_all("protocol_errors.jsonl"),
            do: {id, data}

      assert {nil, %{success: false}} = failure
    end

    test "user bash output streams as command output" do
      [delta | _rest] = normalize_all("protocol_errors.jsonl") |> events(:command_output_delta)

      assert delta.text =~ "hello-from-pi"
      assert delta.item_id == "b1"
    end
  end

  describe "unknown frames" do
    test "are preserved rather than dropped" do
      assert {:event, :provider_event, %{type: "some_future_frame"}} =
               Normalizer.normalize(%{"type" => "some_future_frame", "extra" => 1})
    end
  end
end
