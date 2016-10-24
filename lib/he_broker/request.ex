defmodule HeBroker.Request do

  alias HeBroker.Request.Dont
  alias HeBroker.Request.Just
  alias HeBroker.Request.Headers

  @debug? Application.get_env(:hebroker, :debug, Mix.env in [:test, :dev])
  @trace? @debug? || Application.get_env(:hebroker, :trace, true)
  @processor @trace? && Just || Dont

  @type t :: %__MODULE__{
    message_id: reference,
    trace: [String.t],
    headers: Headers.t
  }

  defstruct \
    message_id: nil,
    trace: [],
    headers: %Headers{}

  @doc false
  defdelegate bounce(request, topic),
    to: @processor

  @doc false
  def init,
    do: @processor.init(%__MODULE__{})

  @doc false
  def init(nil, nil),
    do: init()
  def init(request, nil),
    do: request
  def init(nil, headers),
    do: %__MODULE__{init()| headers: Headers.new(headers)}
  def init(request, headers),
    do: %__MODULE__{request| headers: Headers.new(headers)}


  defmodule Dont do
    def bounce(request, _),
      do: request

    def init(request),
      do: request
  end


  defmodule Just do
    def bounce(request, topic),
      do: %{request| trace: [topic| request.trace], message_id: make_ref()}

    def init(request),
      do: %{request| message_id: make_ref()}
  end
end