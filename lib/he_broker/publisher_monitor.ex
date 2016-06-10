defmodule HeBroker.PublisherMonitor do
  
  alias HeBroker.Publisher
  alias HeBroker.RouteMap

  @type topic :: RouteMap.topic

  @spec start_link() :: GenServer.on_start
  def start_link,
    do: GenServer.start(__MODULE__, [], [])

  @spec subscribe(pid, pid, [topic]) :: :ok
  def subscribe(pid, publisher, topics),
    do: GenServer.cast(pid, {:subscribe, publisher, topics})

  def consumer_up(pid, topic, app, cast, call, consumer),
    do: GenServer.cast(pid, {:consumer_up, topic, app, cast, call, consumer})

  @spec consumer_down(pid, [{topic, app :: atom}], pid) :: :ok
  def consumer_down(pid, topics_apps, consumer),
    do: GenServer.cast(pid, {:consumer_down, topics_apps, consumer})

  @doc false
  def init(_),
    do: {:ok, {%{}, %{}}}

  @doc false
  def handle_cast({:subscribe, publisher, topics}, {pt, tp}) do
    unless Map.has_key?(pt, publisher),
      do: Process.monitor(publisher)

    t = MapSet.new(topics)
    pt2 = Map.update(pt, publisher, t, &MapSet.union(&1, t))

    tp2 = 
      Enum.reduce(topics, tp, fn
        t, acc ->
          Map.update(acc, t, MapSet.new([publisher]), &MapSet.put(&1, publisher))
      end)

    {:noreply, {pt2, tp2}}
  end

  def handle_cast({:consumer_down, topics_apps, consumer}, s = {_, tp}) do
    topics_apps
    |> Enum.reduce(MapSet.new(), fn {topic, _}, acc -> MapSet.union(acc, Map.get(tp, topic, MapSet.new())) end)
    |> Enum.each(&Publisher.consumer_down(&1, topics_apps, consumer))

    {:noreply, s}
  end

  def handle_cast({:consumer_up, topic, app, cast, call, pid}, s = {_, tp}) do
    tp
    |> Map.get(topic, [])
    |> Enum.each(&Publisher.consumer_up(&1, topic, app, cast, call, pid))

    {:noreply, s}
  end

  @doc false
  def handle_info({:DOWN, _ref, _mod, pid, _reason}, {pt, tp}) do
    {topics, pt2} = Map.pop(pt, pid)
    tp2 = Enum.reduce(topics, tp, fn
      t, acc ->
        if MapSet.size(Map.fetch!(acc, t)) === 1,
          do: Map.delete(acc, t),
          else: Map.update!(acc, t, &MapSet.delete(&1, pid))
    end)

    {:noreply, {pt2, tp2}}
  end
end