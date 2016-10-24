defmodule HeBroker.Request.Headers do

  @type t :: %__MODULE__{
    app_id: any,
    correlation_id: any,
    reply_to: pid | port | nil
  }

  defstruct [:app_id, :correlation_id, :reply_to]

  @doc false
  def new,
    do: %__MODULE__{}

  @doc false
  def new(headers = %__MODULE__{}),
    do: headers
  def new(headers),
    do: struct(__MODULE__, headers)
end