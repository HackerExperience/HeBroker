defmodule HeBroker.Broker do
  @moduledoc false

  use GenServer

  alias HeBroker.RouteMap

  @type topic :: String.t
  @type consumer_callbacks :: [cast: RouteMap.cast_fun, call: RouteMap.call_fun]

  @typep t :: %__MODULE__{}

  defstruct [:routes, :consumers]

  @spec start_link() :: GenServer.start
  def start_link,
    do: start_link([])

  @spec start_link(atom | [term]) :: GenServer.start
  def start_link(params) when is_list(params),
    do: GenServer.start_link(__MODULE__, params)
  def start_link(name) when is_atom(name),
    do: start_link(name, [])

  def start_link(name, params),
    do: GenServer.start_link(__MODULE__, [{:name, name}| params], name: name)

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
  def subscribed?(broker, pid, topic) do
    broker
    |> GenServer.call(:consumers_by_topic)
    |> :ets.lookup(topic)
    |> case do
      [{^topic, services}] ->
        MapSet.member?(services, pid)
      _ ->
        false
    end
  end

  @spec count_services_on_topic(pid | atom, topic) :: non_neg_integer
  def count_services_on_topic(broker, topic) do
    broker
    |> GenServer.call(:consumers_by_topic)
    |> :ets.lookup(topic)
    |> case do
      [{^topic, services}] ->
        MapSet.size(services)
      _ ->
        0
    end
  end

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
    |> GenServer.call(:routes)
    |> RouteMap.services_on_topic(topic)
    |> Enum.map(&RouteMap.callback(&1, type))
    |> Enum.reject(&is_nil/1)
  end

  @doc false
  def init(route_options) do
    consumers_by_pid = :ets.new(:hebroker, [])
    consumers_by_topic = :ets.new(:hebroker, [])
    routes = RouteMap.new(route_options)

    {:ok, %__MODULE__{routes: routes, consumers: %{by_pid: consumers_by_pid, by_topic: consumers_by_topic}}}
  end

  @spec handle_call({:subscribed?, pid}, {pid, term}, t) :: {:reply, boolean, t}
  @spec handle_call({:subscribed?, topic, pid}, {pid, term}, t) :: {:reply, boolean, t}
  @doc false
  def handle_call({:subscribed?, pid}, _caller, state),
    do: {:reply, :ets.member(state.consumers.by_pid, pid), state}
  def handle_call(:routes, _caller, state),
    do: {:reply, state.routes, state}
  def handle_call(:consumers_by_topic, _caller, state),
    do: {:reply, state.consumers.by_topic, state}
  def handle_call(_, _, state),
    do: {:noreply, state}

  @spec handle_cast({:subscribe, :consumer, topic, consumer_callbacks, pid}, t) :: {:noreply, t}
  @doc false
  def handle_cast({:subscribe, :consumer, topic, callbacks, pid}, state) do
    cast = Keyword.get(callbacks, :cast)
    call = Keyword.get(callbacks, :call)

    monitor_consumer(state.consumers, topic, pid)

    RouteMap.upsert_topic(state.routes, topic, pid, cast, call)

    {:noreply, state}
  end

  def handle_cast(_, state),
    do: {:noreply, state}

  @doc false
  def handle_info({:DOWN, ref, _mod, pid, _reason}, state) do
    by_topic = state.consumers.by_topic
    by_pid = state.consumers.by_pid

    case :ets.lookup(by_pid, pid) do
      [{^pid, ^ref, topics}] ->
        Enum.each(topics, fn topic ->
          RouteMap.remove_consumer(state.routes, topic, pid)

          case :ets.lookup(by_topic, topic) do
            [{^topic, services}] ->
              s2 = MapSet.delete(services, pid)
              if 0 === MapSet.size(s2) do
                :ets.delete(by_topic, topic)
              else
                :ets.insert(by_topic, {topic, s2})
              end
            _ ->
              :ok
          end
        end)

        :ets.delete(by_pid, pid)
      _ ->
        :ok
    end

    {:noreply, state}
  end

  @spec monitor_consumer(%{by_pid: RouteMap.t, by_topic: RouteMap.t}, topic, pid) :: no_return
  defp monitor_consumer(%{by_pid: by_pid, by_topic: by_topic}, topic, pid) do
    case :ets.lookup(by_pid, pid) do
      [{^pid, ref, topics}] ->
        :ets.insert(by_pid, {pid, ref, MapSet.put(topics, topic)})
      [] ->
        topics = MapSet.new([topic])
        ref = Process.monitor(pid)

        :ets.insert(by_pid, {pid, ref, topics})
    end

    case :ets.lookup(by_topic, topic) do
      [{^topic, services}] ->
        :ets.insert(by_topic, {topic, MapSet.put(services, pid)})
      [] ->
        :ets.insert(by_topic, {topic, MapSet.new([pid])})
    end
  end
end