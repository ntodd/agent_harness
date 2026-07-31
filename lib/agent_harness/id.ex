defmodule AgentHarness.ID do
  @moduledoc false

  @spec generate() :: String.t()
  def generate do
    16
    |> :crypto.strong_rand_bytes()
    |> Base.url_encode64(padding: false)
  end
end
