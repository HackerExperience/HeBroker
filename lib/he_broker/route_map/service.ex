defmodule HeBroker.RouteMap.Service do

  alias HeBroker.Request

  @type t :: %__MODULE__{cast: cast | nil, call: call | nil, pool: pool}

  @opaque pool :: :queue.queue(pid) | [pid]

  @type call_return :: :noreply | {:reply, term}
  @type cast_return :: :noreply

  @type cast :: ((consumer :: pid, RouteMap.topic, message :: any, Request.t) -> cast_return)
  @type call :: ((consumer :: pid, RouteMap.topic, message :: any, Request.t) -> call_return)

  defstruct [:call, :cast, :pool]

  @spec pool_out(pool) :: pid
  def pool_out(pool) do
    Enum.random(pool)
  end

  @spec pool_in(pool, pid) :: pool
  def pool_in(pool, pid) do
    [pid| pool]
  end

  @spec pool_new(pid) :: pool
  def pool_new(pid) do
    [pid]
  end

  @spec pool_delete(pool, pid) :: pool
  def pool_delete(pool, pid) do
    pool -- [pid]
  end

  @spec pool_member?(pool, pid) :: boolean
  def pool_member?(pool, pid) do
    pid in pool
  end

  @spec pool_empty?(pool) :: boolean
  def pool_empty?([]),
    do: true
  def pool_empty?(_),
    do: false
end