defmodule AgentHarness.Internal.Redaction do
  @moduledoc false

  # Shared scrubbing for `SessionConfig.redact/1` and the provider
  # `format_status/1` callbacks, so the providers cannot drift apart in what
  # they consider display-safe. Crash reports render process state with
  # Erlang ~p formatting, which bypasses the Inspect protocol, so the raw
  # term itself must be scrubbed.

  @redacted "[REDACTED]"
  @redacted_charlist String.to_charlist(@redacted)

  @doc """
  The placeholder that replaces redacted values.
  """
  @spec redacted() :: String.t()
  def redacted, do: @redacted

  @doc """
  Keeps the keys of a map or keyword list and replaces every value with the
  redaction placeholder. Anything else, structs included, is replaced
  wholesale because its contents cannot be enumerated safely.
  """
  @spec redact_values(term()) :: term()
  def redact_values(map) when is_map(map) and not is_struct(map) do
    Map.new(map, fn {key, _value} -> {key, @redacted} end)
  end

  def redact_values(list) when is_list(list) do
    if Keyword.keyword?(list) do
      Enum.map(list, fn {key, _value} -> {key, @redacted} end)
    else
      @redacted
    end
  end

  def redact_values(_other), do: @redacted

  @doc """
  Keeps the variable names of an Erlang-style environment list
  (`[{charlist(), charlist()}]`) and replaces every value. Entries that are
  not pairs, or a term that is not a list, are replaced wholesale.
  """
  @spec redact_env_charlists(term()) :: term()
  def redact_env_charlists(env) when is_list(env) do
    Enum.map(env, fn
      {key, _value} -> {key, @redacted_charlist}
      _other -> @redacted
    end)
  end

  def redact_env_charlists(_other), do: @redacted

  @doc """
  Replaces the argv element that follows any of `sensitive_flags`. A term
  that is not a list is replaced wholesale.
  """
  @spec redact_argv(term(), [String.t()]) :: term()
  def redact_argv(args, sensitive_flags) when is_list(args) do
    scrub_argv(args, sensitive_flags)
  end

  def redact_argv(_other, _sensitive_flags), do: @redacted

  defp scrub_argv([], _sensitive_flags), do: []

  defp scrub_argv([arg | rest], sensitive_flags) do
    if arg in sensitive_flags do
      case rest do
        [_value | tail] -> [arg, @redacted | scrub_argv(tail, sensitive_flags)]
        [] -> [arg]
      end
    else
      [arg | scrub_argv(rest, sensitive_flags)]
    end
  end
end
