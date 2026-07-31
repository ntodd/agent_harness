defmodule AgentHarness.Provider.Sink do
  @moduledoc """
  Authenticated mailbox destination used by a provider adapter.

  The private reference prevents events from a replaced provider process from
  being accepted by the logical session.
  """

  @enforce_keys [:pid, :ref]
  defstruct [:pid, :ref]

  @type t :: %__MODULE__{pid: pid(), ref: reference()}
  @terminal_event_types [:turn_completed, :turn_failed, :turn_cancelled, :turn_interrupted]

  @spec new(pid()) :: t()
  def new(pid) when is_pid(pid), do: %__MODULE__{pid: pid, ref: make_ref()}

  @spec emit(t(), String.t(), atom(), term(), term()) ::
          :ok | {:error, :reserved_event_type}
  def emit(sink, turn_id, type, data \\ %{}, raw \\ nil)

  def emit(%__MODULE__{}, _turn_id, type, _data, _raw)
      when type in @terminal_event_types do
    {:error, :reserved_event_type}
  end

  def emit(%__MODULE__{} = sink, turn_id, type, data, raw)
      when is_binary(turn_id) and is_atom(type) do
    deliver(sink, {:event, turn_id, type, data, raw})
  end

  @spec request(t(), String.t(), term(), keyword(), term()) :: :ok
  def request(%__MODULE__{} = sink, turn_id, provider_ref, attrs, raw \\ nil)
      when is_binary(turn_id) and is_list(attrs) do
    deliver(sink, {:request, turn_id, provider_ref, attrs, raw})
  end

  @doc """
  Expires a pending request that the provider resolved or withdrew itself.
  """
  @spec expire_request(t(), String.t(), term(), term(), term()) :: :ok
  def expire_request(%__MODULE__{} = sink, turn_id, provider_ref, reason, raw \\ nil)
      when is_binary(turn_id) do
    deliver(sink, {:expire_request, turn_id, provider_ref, reason, raw})
  end

  @spec finish(t(), String.t(), :completed | :failed | :cancelled | :interrupted, term(), term()) ::
          :ok
  def finish(%__MODULE__{} = sink, turn_id, status, result \\ %{}, raw \\ nil)
      when is_binary(turn_id) and status in [:completed, :failed, :cancelled, :interrupted] do
    deliver(sink, {:finish, turn_id, status, result, raw})
  end

  @spec session_updated(t(), map()) :: :ok
  def session_updated(%__MODULE__{} = sink, attrs) when is_map(attrs) do
    deliver(sink, {:session_updated, attrs})
  end

  @spec transport_down(t(), term()) :: :ok
  def transport_down(%__MODULE__{} = sink, reason) do
    deliver(sink, {:transport_down, reason})
  end

  defp deliver(%__MODULE__{pid: pid, ref: ref}, message) do
    send(pid, {:agent_harness_provider, ref, message})
    :ok
  end
end
