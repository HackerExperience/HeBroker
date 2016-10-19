defmodule HeBroker.Request.TimeoutError do
  @moduledoc """
  Raised when the request takes longer than it's timeout to return a response
  """

  defexception [:message, :request]

  def exception(opts) do
    request = Keyword.fetch!(opts, :request)

    message = """
    Request #{inspect request.message_id} exceeded timeout
    """

    %__MODULE__{message: message, request: request}
  end
end

defmodule HeBroker.InvalidCallbackError do
  defexception [:message]

  def exception(_) do
    %__MODULE__{message: "Broker received an invalid callback or no callback"}
  end
end