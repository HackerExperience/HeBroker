defmodule HeBroker.Request.Reply do

  alias HeBroker.Request

  @type t :: %__MODULE__{
    request: Request.t,
    reply: term,
    reply_time: DateTime.t
  }

  defstruct [:request, :reply, :reply_time]

  @doc false
  def new(request, reply) do
    %__MODULE__{
      request: request,
      reply: reply,
      reply_time: DateTime.utc_now()
    }
  end
end