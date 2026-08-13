defmodule AgentHarness.Internal.RedactionTest do
  use ExUnit.Case, async: true

  alias AgentHarness.Internal.Redaction

  defmodule Carrier do
    defstruct [:api_key, :base_url]
  end

  describe "redact_values/1" do
    test "keeps map keys and replaces every value" do
      assert Redaction.redact_values(%{"auth" => "token", api_key: "secret"}) ==
               %{"auth" => "[REDACTED]", api_key: "[REDACTED]"}
    end

    test "keeps keyword list keys and replaces every value" do
      assert Redaction.redact_values(process_env: %{"KEY" => "secret"}, cwd: "/work") ==
               [process_env: "[REDACTED]", cwd: "[REDACTED]"]
    end

    test "replaces structs wholesale" do
      assert Redaction.redact_values(%Carrier{api_key: "secret"}) == "[REDACTED]"
    end

    test "replaces non-keyword lists wholesale" do
      assert Redaction.redact_values(["--api-key", "secret"]) == "[REDACTED]"
    end

    test "replaces other terms wholesale" do
      assert Redaction.redact_values("secret") == "[REDACTED]"
      assert Redaction.redact_values(nil) == "[REDACTED]"
    end

    test "keeps an empty map and an empty list" do
      assert Redaction.redact_values(%{}) == %{}
      assert Redaction.redact_values([]) == []
    end
  end

  describe "redact_env_charlists/1" do
    test "keeps variable names and replaces charlist values" do
      env = [{~c"OPENROUTER_API_KEY", ~c"secret"}, {~c"HOME", ~c"/home/user"}]

      assert Redaction.redact_env_charlists(env) == [
               {~c"OPENROUTER_API_KEY", ~c"[REDACTED]"},
               {~c"HOME", ~c"[REDACTED]"}
             ]
    end

    test "replaces entries that are not pairs" do
      assert Redaction.redact_env_charlists([~c"stray-secret"]) == ["[REDACTED]"]
    end

    test "replaces non-list values wholesale" do
      assert Redaction.redact_env_charlists(%{"KEY" => "secret"}) == "[REDACTED]"
    end
  end

  describe "redact_argv/2" do
    test "replaces the value following a sensitive flag" do
      args = ["--mode", "rpc", "--api-key", "secret", "--model", "pi-1"]

      assert Redaction.redact_argv(args, ["--api-key"]) ==
               ["--mode", "rpc", "--api-key", "[REDACTED]", "--model", "pi-1"]
    end

    test "replaces every occurrence" do
      args = ["--api-key", "one", "--api-key", "two"]

      assert Redaction.redact_argv(args, ["--api-key"]) ==
               ["--api-key", "[REDACTED]", "--api-key", "[REDACTED]"]
    end

    test "handles a trailing flag with no value" do
      assert Redaction.redact_argv(["--mode", "rpc", "--api-key"], ["--api-key"]) ==
               ["--mode", "rpc", "--api-key"]
    end

    test "leaves argv without sensitive flags untouched" do
      args = ["--mode", "rpc", "--session-id", "s-1"]

      assert Redaction.redact_argv(args, ["--api-key"]) == args
    end

    test "replaces non-list values wholesale" do
      assert Redaction.redact_argv("--api-key secret", ["--api-key"]) == "[REDACTED]"
    end
  end
end
