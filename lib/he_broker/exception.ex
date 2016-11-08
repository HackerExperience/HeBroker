defmodule HeBroker.Request.TimeoutError do
  @moduledoc """
  Raised when the request takes longer than it's timeout to return a response
  """

  defexception [:message, :request]

  def exception(opts) when is_list(opts) do
    %__MODULE__{message: "request exceeded timeout", request: opts[:request]}
  end

  def exception(_) do
    %__MODULE__{message: "request exceeded timeout"}
  end
end

defmodule HeBroker.InvalidCallbackError do
  defexception [:message]

  def exception(_) do
    %__MODULE__{message: "Broker received an invalid callback or no callback"}
  end
end