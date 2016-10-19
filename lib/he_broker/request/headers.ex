defmodule HeBroker.Request.Headers do

  @type t :: %__MODULE__{
    app_id: any,
    correlation_id: any,
    reply_to: pid | port | nil,
    start_time: DateTime.t
  }

  defstruct [:app_id, :correlation_id, :reply_to, :start_time]

  @doc false
  def new do
    %__MODULE__{
      start_time: DateTime.utc_now()
    }
  end

  def new(headers) do
    struct(__MODULE__, headers)
  end
end