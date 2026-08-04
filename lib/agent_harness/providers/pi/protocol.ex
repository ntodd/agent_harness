defmodule AgentHarness.Providers.Pi.Protocol do
  @moduledoc false

  alias AgentHarness.Providers.Pi.Normalizer
  alias AgentHarness.Response

  @type command :: :prompt | :steer | :follow_up | :abort | :get_state | :new_session

  @spec command(String.t(), command(), map()) :: map()
  def command(id, type, payload \\ %{}) when is_binary(id) and is_atom(type) do
    payload
    |> Map.new(fn {key, value} -> {camel_key(key), value} end)
    |> Map.merge(%{"id" => id, "type" => Atom.to_string(type)})
  end

  @doc """
  Encodes a harness `Response` as an `extension_ui_response` frame.

  Only blocking dialog methods can be answered. Answering a fire-and-forget
  method would leave pi with no pending request to resolve, so it is refused
  here rather than written to the port.
  """
  @spec encode(String.t(), map(), Response.t()) :: {:ok, map()} | {:error, term()}
  def encode(id, metadata, %Response{} = response) do
    method = Map.get(metadata, :method, Map.get(metadata, "method"))

    if is_binary(method) and Normalizer.dialog_method?(method) do
      encode_dialog(id, method, response)
    else
      {:error, {:unanswerable_method, method}}
    end
  end

  defp encode_dialog(id, "confirm", %Response{action: action})
       when action in [:approve, :deny] do
    {:ok, confirmed(id, action == :approve)}
  end

  defp encode_dialog(id, "confirm", %Response{action: :answer, value: value})
       when is_boolean(value) do
    {:ok, confirmed(id, value)}
  end

  defp encode_dialog(_id, "confirm", %Response{action: :answer, value: value}) do
    {:error, {:invalid_confirm_answer, value}}
  end

  defp encode_dialog(id, method, %Response{action: :answer, value: value})
       when method in ["select", "input", "editor"] do
    {:ok, envelope(id, %{"value" => value})}
  end

  # A select or text dialog has no "declined" shape in pi. Denying is a refusal
  # to supply a value, which is exactly what dismissal means to the extension.
  defp encode_dialog(id, method, %Response{action: action})
       when method in ["select", "input", "editor"] and action in [:deny, :cancel] do
    {:ok, cancelled(id)}
  end

  defp encode_dialog(id, "confirm", %Response{action: :cancel}), do: {:ok, cancelled(id)}

  defp encode_dialog(_id, method, %Response{action: action}) do
    {:error, {:unsupported_response, method, action}}
  end

  defp confirmed(id, value), do: envelope(id, %{"confirmed" => value})
  defp cancelled(id), do: envelope(id, %{"cancelled" => true})

  defp envelope(id, fields) do
    Map.merge(fields, %{"type" => "extension_ui_response", "id" => id})
  end

  defp camel_key(:follow_up), do: "followUp"
  defp camel_key(:streaming_behavior), do: "streamingBehavior"
  defp camel_key(:parent_session), do: "parentSession"
  defp camel_key(:session_path), do: "sessionPath"
  defp camel_key(:entry_id), do: "entryId"
  defp camel_key(key) when is_atom(key), do: Atom.to_string(key)
  defp camel_key(key), do: to_string(key)
end
