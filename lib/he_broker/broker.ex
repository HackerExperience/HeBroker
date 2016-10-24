defmodule HeBroker.Broker do
  @moduledoc false

  use GenServer

  alias HeBroker.RouteMap

  @type topic :: String.t
  @type consumer_callbacks :: [cast: RouteMap.cast_fun, call: RouteMap.call_fun]

  @typep t :: %__MODULE__{}

  defstruct \
    routes: %{},
    consumers: %{by_topic: %{}, by_pid: %{}, monitors: %{}}

  @spec start_link() :: GenServer.start
  def start_link,
    do: GenServer.start_link(__MODULE__, [])

  @spec start_link(atom) :: GenServer.start
  def start_link(name),
    do: GenServer.start_link(__MODULE__, [], name: name)

  @spec subscribe(pid | atom, topic, consumer_callbacks) :: :ok
  @doc false
  def subscribe(broker, topic, callbacks) when is_binary(topic) do
    cast = Keyword.get(callbacks, :cast)
    call = Keyword.get(callbacks, :call)

    if \
      (is_nil(cast) and is_nil(call))
      or (not is_nil(cast) and not is_function(cast, 4))
      or (not is_nil(call) and not is_function(call, 4))
    do
      raise HeBroker.InvalidCallbackError
    end

    GenServer.cast(broker, {:subscribe, :consumer, topic, callbacks, self()})
  end

  @spec subscribed?(pid | atom, pid) :: boolean
  def subscribed?(broker, pid),
    do: GenServer.call(broker, {:subscribed?, pid})

  @spec subscribed?(pid | atom, pid, topic) :: boolean
  def subscribed?(broker, pid, topic),
    do: GenServer.call(broker, {:subscribed?, topic, pid})

  @spec count_services_on_topic(pid | atom, topic) :: non_neg_integer
  def count_services_on_topic(broker, topic),
    do: GenServer.call(broker, {:count, :topic, topic})

  @spec cast_callbacks(pid | atom, topic) :: [RouteMap.partial]
  @doc """
  Returns the callbacks from the consumers subscribed on `topic` on the `broker`
  """
  def cast_callbacks(broker, topic),
    do: callbacks(broker, topic, :cast)

  @spec call_callbacks(pid | atom, topic) :: [RouteMap.partial]
  @doc """
  Returns the callbacks from the consumers subscribed on `topic` on the `broker`
  """
  def call_callbacks(broker, topic),
    do: callbacks(broker, topic, :call)

  @spec callbacks(pid | atom, topic, :cast | :call) :: [RouteMap.partial]
  defp callbacks(broker, topic, type) do
    broker
    |> GenServer.call({:topic, topic})
    |> List.wrap()
    |> Enum.map(&RouteMap.callback(&1, type))
    |> Enum.reject(&is_nil/1)
  end

  @doc false
  def init(_),
    do: {:ok, %__MODULE__{}}

  @spec handle_call({:topic, topic}, {pid, term}, t) :: {:reply, [RouteMap.service], t}
  @spec handle_call({:subscribed?, pid}, {pid, term}, t) :: {:reply, boolean, t}
  @spec handle_call({:subscribed?, topic, pid}, {pid, term}, t) :: {:reply, boolean, t}
  @spec handle_call({:count, :topic, topic}, {pid, term}, t) :: {:reply, non_neg_integer, t}
  @doc false
  def handle_call({:topic, topic}, _caller, state) do
    {routes, services} = RouteMap.services_on_topic(state.routes, topic)

    {:reply, services, %{state| routes: routes}}
  end

  def handle_call({:subscribed?, pid}, _caller, state) do
    {:reply, Map.has_key?(state.consumers.by_pid, pid), state}
  end

  def handle_call({:subscribed?, topic, pid}, _caller, state) do
    topics_for_pid = Map.get(state.consumers.by_pid, pid)
    subscribed? = topics_for_pid && MapSet.member?(topics_for_pid, topic) || false

    {:reply, subscribed?, state}
  end

  def handle_call({:count, :topic, topic}, _caller, state) do
    # REVIEW: maybe move the count part to the client side
    count = Map.get(state.routes, topic, []) |> Enum.count()

    {:reply, count, state}
  end

  @spec handle_cast({:subscribe, :consumer, topic, consumer_callbacks, pid}, t) :: {:noreply, t}
  @doc false
  def handle_cast({:subscribe, :consumer, topic, callbacks, pid}, state) do
    cast = Keyword.get(callbacks , :cast)
    call = Keyword.get(callbacks , :call)

    consumers = monitor_consumer(state.consumers, topic, pid)

    routes = RouteMap.upsert_topic(state.routes, topic, pid, cast, call)

    {:noreply, %{state| consumers: consumers, routes: routes}}
  end

  @doc false
  def handle_info({:DOWN, _ref, _mod, pid, _reason}, state) do
    topics = Map.fetch!(state.consumers.by_pid, pid)
    routes = Enum.reduce(topics, state.routes, &RouteMap.remove_consumer(&2, &1, pid))
    consumers = remove_consumer_from_topics(state.consumers, topics, pid)

    {:noreply, %{state| consumers: consumers, routes: routes}}
  end

  @spec monitor_consumer(%{}, topic, pid) :: %{}
  defp monitor_consumer(consumers, topic, pid) do
    monitor_process = fn -> Process.monitor(pid) end
    by_pid_update = fn map ->
      Map.update(map, pid, MapSet.new([topic]), &MapSet.put(&1, topic))
    end
    by_topic_update = fn map ->
      Map.update(map, topic, MapSet.new([pid]), &MapSet.put(&1, pid))
    end

    consumers
    |> Map.update!(:by_pid, by_pid_update)
    |> Map.update!(:by_topic, by_topic_update)
    |> Map.update!(:monitors, &(Map.put_new_lazy(&1, pid, monitor_process)))
  end

  @spec remove_consumer_from_topics(%{}, [topic], pid) :: %{}
  defp remove_consumer_from_topics(consumers, topics, pid) do
    topics_as_set = MapSet.new(topics)

    consumed_topics = Map.fetch!(consumers.by_pid, pid)

    by_pid = if MapSet.equal?(consumed_topics, topics_as_set) do
      Map.delete(consumers.by_pid, pid)
    else
      Map.put(consumers.by_pid, pid, MapSet.difference(consumed_topics, topics_as_set))
    end

    by_topic =
      topics
      |> Enum.reduce(consumers.by_topic, fn topic, acc ->
        topic_consumers = Map.fetch!(acc, topic)
        updated_consumers = MapSet.delete(topic_consumers, pid)

        if MapSet.size(updated_consumers) == 0,
          do: Map.delete(acc, topic),
          else: Map.put(acc, topic, updated_consumers)
      end)

    monitors = if Map.has_key?(by_pid, pid) do
      consumers.monitors
    else
      consumers.monitors |> Map.fetch!(pid) |> Process.demonitor()
      Map.delete(consumers.monitors, pid)
    end

    %{consumers| by_pid: by_pid, by_topic: by_topic, monitors: monitors}
  end
end