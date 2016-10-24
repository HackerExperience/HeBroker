defmodule HeBroker.Publisher do

  alias HeBroker.Request
  alias HeBroker.RouteMap
  alias HeBroker.Request.Reply
  alias HeBroker.Request.OngoingRequest
  alias HeBroker.Pry

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
      |> Pry.pry_origin_request()

    broker
    |> HeBroker.Broker.cast_callbacks(topic)
    |> Pry.pry_maybe_message_lost(topic, message, request)
    |> Enum.each(fn partial ->
      spawn fn ->
        {_request, callback} = prepare_callback(partial, topic, message, request)
        callback.()
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
      |> Pry.pry_origin_request()

    broker
    |> HeBroker.Broker.call_callbacks(topic)
    |> Pry.pry_maybe_message_lost(topic, message, request)
    |> OngoingRequest.start(message, request, topic)
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

  @spec prepare_callback(RouteMap.partial, topic, message, Request.t) :: {Request.t, (() -> RouteMap.callback_return)}
  @doc false
  def prepare_callback(partial, topic, message, request) do
    dest_request =
      request
      |> Request.bounce(topic)
      |> Pry.pry_sent(request, topic, message)

    callback = fn ->
      partial.(topic, message, dest_request)
    end

    {dest_request, callback}
  end
end