defmodule AgentHarness.Providers.Pi.Framing do
  @moduledoc false

  # Pi's RPC transport is strict JSONL: LF is the only record delimiter. A
  # generic line reader is not protocol-compliant, because U+2028 and U+2029
  # are legal inside JSON strings and would split a frame in half. Splitting on
  # a raw "\n" byte avoids that, and frames arrive whole regardless of size.

  @spec decode(binary(), binary()) :: {[binary()], binary()}
  def decode(buffer, chunk) when is_binary(buffer) and is_binary(chunk) do
    split(buffer <> chunk, [])
  end

  defp split(buffer, acc) do
    case :binary.split(buffer, "\n") do
      [rest] ->
        {Enum.reverse(acc), rest}

      [line, rest] ->
        split(rest, prepend_frame(line, acc))
    end
  end

  defp prepend_frame(line, acc) do
    case trim_carriage_return(line) do
      "" -> acc
      frame -> [frame | acc]
    end
  end

  defp trim_carriage_return(line) do
    case :binary.last(line) do
      ?\r -> :binary.part(line, 0, byte_size(line) - 1)
      _other -> line
    end
  rescue
    ArgumentError -> line
  end
end
