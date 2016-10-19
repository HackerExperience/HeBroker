defmodule HeBroker.RouteMap.Service do

  @opaque t :: %__MODULE__{}

  defstruct [:call, :cast, :pool]
end