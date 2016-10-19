defmodule HeBroker.Publisher do

  alias HeBroker.Request
  alias HeBroker.Request.Reply
  alias HeBroker.Request.OngoingRequest

  @type broker :: pid | atom
  @type topic :: String.t
  @type message :: term
  @type params :: [
    {:timeout, non_neg_integer | :infinity},
    {:request, Request.t},
    {:headers, Keyword.t}]

  @default_timeout 15_000

  @spec cast(broker, topic, message, params) :: Request.t
  @doc """
  Sends `message` to all services subscribed to `topic`

  `params` can include `headers` and/or a `request`
  """
  def cast(broker, topic, message, params \\ []) do
    request =
      params
      |> Keyword.get(:request)
      |> Request.init(Keyword.get(params, :headers))
      |> Request.bounce(topic)

    broker
    |> HeBroker.Broker.cast_callbacks(topic)
    |> List.wrap()
    |> Enum.each(fn callback ->
      spawn fn ->
        callback.(message, request)
      end
    end)

    request
  end

  @spec call(broker, topic, message, params) :: Reply.t
  @doc """
  Sends `message` to all applications subscribed to `topic` using their defined
  call functions expecting a return

  Raises `HeBroker.Request.TimeoutError` if no reply is returned within
  `timeout` (this also means that the process might be blocked forever if
  timeout is `:infinity` and no reply is returned)

  In case more than one service reply to the request, only the first reply is
  returned
  """
  def call(broker, topic, message, params \\ []) do
    broker
    |> async(topic, message, params)
    |> await(Keyword.get(params, :timeout, @default_timeout))
  end

  @spec async(broker, topic, message, params) :: OngoingRequest.t
  @doc """
  Starts an asynchronous request to the services subscribed to `topic`.

  Use `yield/2` or `await/2` to receive the return value
  """
  def async(broker, topic, message, params \\ []) do
    request =
      params
      |> Keyword.get(:request)
      |> Request.init(Keyword.get(params, :headers))
      |> Request.bounce(topic)

    broker
    |> HeBroker.Broker.call_callbacks(topic)
    |> List.wrap()
    |> OngoingRequest.start(message, request)
  end

  @spec await(OngoingRequest.t, non_neg_integer | :infinity) :: Reply.t
  @doc """
  Waits `timeout` for the `request` to reply

  If no reply is received before `timeout`, `HeBroker.Request.TimeoutError`
  is raised
  """
  defdelegate await(request, timeout \\ @default_timeout),
    to: OngoingRequest

  @spec yield(OngoingRequest.t, non_neg_integer) :: {:ok, Reply.t} | nil
  @doc """
  Waits `timeout` for the `request` to return a reply

  If no reply is returned before `timeout`, nil is returned
  """
  defdelegate yield(request, timeout \\ @default_timeout),
    to: OngoingRequest

  @spec ignore(OngoingRequest.t) :: Reply.t
  @doc """
  Ignores the possible return from an ongoing async request

  This function is provided as a safe-guard for exceptional cases. It's use is
  discouraged

  NOTE: Don't use this function for a request that has already `yield/2` or
  been `await/2`ed for, otherwise the current function will be blocked forever
  since it assume the monitor to the target task is still open
  """
  defdelegate ignore(request),
    to: OngoingRequest, as: :shutdown
end