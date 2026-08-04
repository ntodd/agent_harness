defmodule AgentHarness.Providers.PiFramingTest do
  use ExUnit.Case, async: true

  alias AgentHarness.Providers.Pi.Framing

  describe "decode/2" do
    test "splits complete frames and keeps the remainder buffered" do
      assert {[~s({"a":1}), ~s({"b":2})], ~s({"c":)} =
               Framing.decode("", ~s({"a":1}\n{"b":2}\n{"c":))
    end

    test "reassembles a frame split across chunks" do
      {frames, buffer} = Framing.decode("", ~s({"a":))
      assert frames == []

      assert {[~s({"a":1})], ""} = Framing.decode(buffer, "1}\n")
    end

    test "strips a trailing carriage return" do
      assert {[~s({"a":1})], ""} = Framing.decode("", ~s({"a":1}\r\n))
    end

    test "skips blank lines" do
      assert {[~s({"a":1})], ""} = Framing.decode("", ~s(\n\n{"a":1}\n))
    end

    # Pi's RPC docs call this out: U+2028 and U+2029 are legal inside a JSON
    # string, and a reader that treats them as newlines corrupts the frame.
    test "does not split on Unicode line separators inside a JSON string" do
      text = "before\u2028after\u2029end"
      payload = ~s({"text":"#{text}"})

      assert {[frame], ""} = Framing.decode("", payload <> "\n")
      assert JSON.decode!(frame)["text"] == text
    end

    test "handles a frame larger than a typical read chunk" do
      big = String.duplicate("x", 200_000)
      payload = JSON.encode!(%{"text" => big})

      {frames, buffer} =
        payload
        |> String.split("", trim: true)
        |> Enum.chunk_every(9_973)
        |> Enum.map(&Enum.join/1)
        |> Kernel.++(["\n"])
        |> Enum.reduce({[], ""}, fn chunk, {acc, buf} ->
          {frames, buf} = Framing.decode(buf, chunk)
          {acc ++ frames, buf}
        end)

      assert buffer == ""
      assert [frame] = frames
      assert JSON.decode!(frame)["text"] == big
    end

    test "emits multiple frames delivered in one chunk in order" do
      chunk = Enum.map_join(1..5, "", &~s({"n":#{&1}}\n))

      assert {frames, ""} = Framing.decode("", chunk)
      assert Enum.map(frames, &JSON.decode!(&1)["n"]) == [1, 2, 3, 4, 5]
    end
  end
end
