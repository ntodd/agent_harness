defmodule AgentHarness.Providers.PiProtocolTest do
  use ExUnit.Case, async: true

  alias AgentHarness.Providers.Pi.Protocol
  alias AgentHarness.Response

  describe "commands" do
    test "a prompt carries its correlation id" do
      assert %{"id" => "cmd-1", "type" => "prompt", "message" => "hello"} =
               Protocol.command("cmd-1", :prompt, %{message: "hello"})
    end

    test "steering and follow-up are distinct commands" do
      assert %{"type" => "steer", "message" => "left"} =
               Protocol.command("cmd-2", :steer, %{message: "left"})

      assert %{"type" => "follow_up", "message" => "later"} =
               Protocol.command("cmd-3", :follow_up, %{message: "later"})
    end

    test "abort and get_state take no payload" do
      assert %{"id" => "cmd-4", "type" => "abort"} == Protocol.command("cmd-4", :abort)
      assert %{"id" => "cmd-5", "type" => "get_state"} == Protocol.command("cmd-5", :get_state)
    end
  end

  describe "encode/2 for a confirm dialog" do
    @confirm %{method: "confirm"}

    test "approve confirms" do
      assert {:ok, %{"type" => "extension_ui_response", "id" => "d1", "confirmed" => true}} =
               Protocol.encode("d1", @confirm, Response.approve())
    end

    test "deny declines without cancelling" do
      assert {:ok, %{"id" => "d1", "confirmed" => false}} =
               Protocol.encode("d1", @confirm, Response.deny())
    end

    test "cancel dismisses the dialog" do
      assert {:ok, %{"id" => "d1", "cancelled" => true}} =
               Protocol.encode("d1", @confirm, Response.cancel())
    end

    test "a boolean answer is accepted directly" do
      assert {:ok, %{"confirmed" => true}} =
               Protocol.encode("d1", @confirm, Response.answer(true))

      assert {:ok, %{"confirmed" => false}} =
               Protocol.encode("d1", @confirm, Response.answer(false))
    end

    test "a non-boolean answer is rejected rather than coerced" do
      assert {:error, {:invalid_confirm_answer, "maybe"}} =
               Protocol.encode("d1", @confirm, Response.answer("maybe"))
    end
  end

  describe "encode/2 for a select dialog" do
    @select %{method: "select"}

    test "an answer becomes the selected option" do
      assert {:ok, %{"id" => "d2", "value" => "Postgres"}} =
               Protocol.encode("d2", @select, Response.answer("Postgres"))
    end

    test "cancel dismisses the dialog" do
      assert {:ok, %{"cancelled" => true}} =
               Protocol.encode("d2", @select, Response.cancel())
    end

    test "deny dismisses the dialog because pi has no decline for a select" do
      assert {:ok, %{"cancelled" => true}} =
               Protocol.encode("d2", @select, Response.deny())
    end
  end

  describe "encode/2 for text dialogs" do
    test "input takes the answer verbatim" do
      assert {:ok, %{"value" => "my-app"}} =
               Protocol.encode("d3", %{method: "input"}, Response.answer("my-app"))
    end

    test "editor takes multi-line text" do
      assert {:ok, %{"value" => "line 1\nline 2"}} =
               Protocol.encode("d4", %{method: "editor"}, Response.answer("line 1\nline 2"))
    end

    test "an approve without a value has nothing to submit" do
      assert {:error, {:unsupported_response, "input", :approve}} =
               Protocol.encode("d3", %{method: "input"}, Response.approve())
    end
  end

  describe "encode/2 guards" do
    test "refuses to answer a fire-and-forget method" do
      assert {:error, {:unanswerable_method, "notify"}} =
               Protocol.encode("d5", %{method: "notify"}, Response.approve())
    end

    test "refuses a request with no recorded method" do
      assert {:error, {:unanswerable_method, nil}} =
               Protocol.encode("d6", %{}, Response.approve())
    end
  end
end
