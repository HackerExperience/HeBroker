defmodule HeBroker.Publisher do

  use GenServer
  alias HeBroker.RouteMap

  @type publisher :: pid
  @type topic :: RouteMap.topic

  @spec subscribe(publisher, [topic] | topic) :: publisher
  @doc """
  Subscribe the publisher to a new topic or list of `topics`
  """
  def subscribe(publisher, topics) do
    GenServer.cast(publisher, {:subscribe, topics})

    publisher
  end

  @spec start_link([topic] | topic) :: publisher
  @doc """
  Starts the publisher middleware
  """
  def start_link(topics \\ []) do
    {:ok, pid} = GenServer.start_link(__MODULE__, topics)
    pid
  end

  @spec cast(publisher, topic, message :: any) :: publisher
  @doc """
  Sends `message` to all applications subscribed to `topic` using their defined
  cast functions
  """
  def cast(publisher, topic, message) do
    publisher
    |> GenServer.call({:cast, topic})
    |> Enum.each(
      fn {fun, pid} ->
        fun.(pid, topic, message)
    end)

    publisher
  end

  @spec call(publisher, topic, message :: any, timeout :: non_neg_integer | :infinity) :: :noreply | {:reply, any}
  @doc """
  Sends `message` to all applications subscribed to `topic` using their defined
  call functions expecting a return

  Note 1: since the _call_ functions are defined by their own consumer, they might
  ignore the timeout
  Note 2: The timeout is applied individually to every call function and thus may
  be much higher than the defined timeout
  Note 3: Those functions are usually blocking
  Note 4: If more than one `{:reply, value}` tuples are received, only the first
  will be returned
  """
  def call(pid, topic, message, timeout \\ 5_000) do
    pid
    |> GenServer.call({:call, topic})
    |> Enum.reduce(:noreply, fn
      {fun, pid}, :noreply ->
        case fun.(pid, topic, message, timeout) do
          {:reply, value} ->
            {:reply, value}
          :noreply ->
            :noreply
        end
      {fun, pid}, acc ->
        fun.(pid, topic, message, timeout)
        acc
    end)
  end

  def consumer_up(pid, topic, app, cast_fun, call_fun, consumer),
    do: GenServer.cast(pid, {:consumer_up, topic, app, cast_fun, call_fun, consumer})

  @spec consumer_down(publisher, [{topic, app :: atom}], consumer :: pid) :: :ok
  @doc false
  def consumer_down(pid, topics_apps, consumer),
    do: GenServer.cast(pid, {:consumer_down, topics_apps, consumer})

  def lookup(publisher, topic) do
    GenServer.call(publisher, {:lookup, topic})
  end

  @doc false
  def init(topics) do
    routes = topics |> List.wrap() |> HeBroker.publisher_subscribe()

    {:ok, {List.wrap(topics), routes}}
  end

  @doc false
  def handle_call({:call, topic}, caller, s = {t, r}) do
    case RouteMap.topic_call(r, topic) do
      {:ok, {calls, routes}} ->
        {:reply, calls, {t, routes}}
      {:error, :not_found} ->
        if topic in t do
          {:reply, [], s}
        else
          handle_call({:call, topic}, caller, do_subscribe(s, topic))
        end
    end
  end

  def handle_call({:cast, topic}, caller, s = {t, r}) do
    case RouteMap.topic_cast(r, topic) do
      {:ok, {casts, routes}} ->
        {:reply, casts, {t, routes}}
      {:error, :not_found} ->
        if topic in t do
          {:reply, [], s}
        else
          handle_call({:cast, topic}, caller, do_subscribe(s, topic))
        end
    end
  end

  def handle_call({:lookup, topic}, caller, s = {t, r}) do
    {:reply, Enum.member?(t, topic), s}
  end


  @doc false
  def handle_cast({:subscribe, topics}, s) do
    {:noreply, do_subscribe(s, topics)}
  end

  def handle_cast({:consumer_up, topic, app, cast_fun, call_fun, pid}, {t, r}) do
    {:noreply, {t, RouteMap.upsert_topic(r, topic, app, pid, cast_fun, call_fun)}}
  end

  def handle_cast({:consumer_down, topics_app, consumer}, {t, r}) do
    {:noreply, {t, RouteMap.remove_consumer(r, topics_app, consumer)}}
  end

  @spec do_subscribe({[topic], RouteMap.t}, topic | [topic]) :: {[topic], RouteMap.t}
  defp do_subscribe({topics, routes}, topic) do
    {List.wrap(topic) ++ topics, RouteMap.merge(routes, HeBroker.publisher_subscribe(topic))}
  end
end
