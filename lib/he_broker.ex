defmodule HeBroker do

  alias HeBroker.RouteMap
  alias HeBroker.ConsumerMonitor
  alias HeBroker.PublisherMonitor

  @type topic :: RouteMap.topic

  @spec start_link() :: GenServer.start
  def start_link,
    do: GenServer.start_link(__MODULE__, [], name: HeBroker)

  @spec publisher_subscribe([topic]) :: RouteMap.routes
  def publisher_subscribe(topics),
    do: GenServer.call(HeBroker, {:subscribe, :publisher, List.wrap(topics), self()})

  @spec consumer_subscribe(atom, topic, [cast: RouteMap.cast_fun, call: RouteMap.call_fun]) :: :ok
  def consumer_subscribe(app, topic, fun) do
    fun2 =
      fun
      |> Keyword.put_new(:cast, fn _, _, _ -> :ok end)
      |> Keyword.put_new(:call, fn _, _, _, _ -> :noreply end)

    GenServer.cast(HeBroker, {:subscribe, :consumer, app, topic, fun2, self()})
  end

  @spec consumer_down([{topic, atom}], pid) :: :ok
  @doc false
  def consumer_down(topics_app, server),
    do: GenServer.cast(HeBroker, {:consumer_down, topics_app, server})

  @doc false
  # REVIEW: use `else` as soon as elixir 1.3 is released
  def init(_) do
    with \
      {:ok, cm} <- ConsumerMonitor.start_link(),
      {:ok, pm} <- PublisherMonitor.start_link(),
      routes = RouteMap.new()
    do
      {:ok, {routes, cm, pm}}
    end
    |> case do
      {:ok, state} -> {:ok, state}
      _ -> {:stop, "Error starting broker managers"}
    end
  end

  @doc false
  def handle_cast({:subscribe, :consumer, app, topic, funs, pid}, {routes, cm, pm}) do
    cast = Keyword.fetch!(funs, :cast)
    call = Keyword.fetch!(funs, :call)

    ConsumerMonitor.subscribe(cm, pid, app, topic)
    PublisherMonitor.consumer_up(pm, topic, app, cast, call, pid)

    # Updates the routemap to include the new consumer at the specified topic
    new_routes = RouteMap.upsert_topic(routes, topic, app, pid, cast, call)

    {:noreply, {new_routes, cm, pm}}
  end

  def handle_cast({:consumer_down, topics_app, server}, {r, cm, pm}) do
    PublisherMonitor.consumer_down(pm, topics_app, server)

    {:noreply, {RouteMap.remove_consumer(r, topics_app, server), cm, pm}}
  end

  @doc false
  def handle_call({:subscribe, :publisher, topics, pid}, _caller, {routes, cm, pm}) do
    PublisherMonitor.subscribe(pm, pid, topics)

    {:reply, Map.take(routes, topics), {routes, cm, pm}}
  end
end