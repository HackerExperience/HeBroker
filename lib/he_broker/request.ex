defmodule HeBroker.Request do

  alias HeBroker.Request.Headers

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
  def bounce(request = %__MODULE__{}, topic) do
    %{request| trace: [topic| request.trace], message_id: make_ref()}
  end

  @doc false
  def init do
    %__MODULE__{}
  end

  @doc false
  def init(nil, nil) do
    init()
  end

  def init(nil, headers) do
    %__MODULE__{headers: Headers.new(headers)}
  end

  def init(request, nil) do
    request
  end

  def init(request, headers = %Headers{}) do
    %__MODULE__{request| headers: headers}
  end
end