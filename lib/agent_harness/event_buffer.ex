defmodule AgentHarness.EventBuffer do
  @moduledoc false

  @enforce_keys [:capacity]
  defstruct capacity: nil, size: 0, queue: :queue.new()

  @type t :: %__MODULE__{
          capacity: pos_integer(),
          size: non_neg_integer(),
          queue: :queue.queue()
        }

  @spec new(pos_integer()) :: t()
  def new(capacity) when is_integer(capacity) and capacity > 0 do
    %__MODULE__{capacity: capacity}
  end

  def new(_capacity) do
    raise ArgumentError, "event buffer capacity must be a positive integer"
  end

  @spec push(t(), term()) :: t()
  def push(%__MODULE__{size: size, capacity: capacity} = buffer, event)
      when size < capacity do
    %{buffer | queue: :queue.in(event, buffer.queue), size: size + 1}
  end

  def push(%__MODULE__{} = buffer, event) do
    {{:value, _oldest}, queue} = :queue.out(buffer.queue)
    %{buffer | queue: :queue.in(event, queue)}
  end

  @spec to_list(t()) :: [term()]
  def to_list(%__MODULE__{queue: queue}), do: :queue.to_list(queue)

  @spec from(t(), :start | :latest | non_neg_integer()) :: [term()]
  def from(%__MODULE__{}, :latest), do: []
  def from(%__MODULE__{} = buffer, :start), do: to_list(buffer)

  def from(%__MODULE__{} = buffer, seq) when is_integer(seq) and seq >= 0 do
    Enum.drop_while(to_list(buffer), &(&1.seq < seq))
  end
end
