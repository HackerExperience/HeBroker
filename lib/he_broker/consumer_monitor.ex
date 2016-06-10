defmodule HeBroker.ConsumerMonitor do

  alias HeBroker.RouteMap

  @type topic :: RouteMap.topic

  @spec start_link() :: GenServer.on_start
  def start_link,
    do: GenServer.start_link(__MODULE__, [], [])

  @spec subscribe(pid, pid, atom, topic) :: :ok
  def subscribe(pid, consumer, app, topic),
    do: GenServer.cast(pid, {:subscribe, consumer, app, topic})

  @doc false
  def init(_),
    do: {:ok, %{}}

  @doc false
  def handle_cast({:subscribe, consumer, app, topic}, c) do
    unless Map.has_key?(c, consumer),
      do: Process.monitor(consumer)

    {:noreply, Map.update(c, consumer, [{topic, app}], &([{topic, app}|&1]))}
  end

  @doc false
  def handle_info({:DOWN, _ref, _mod, pid, _reason}, c) do
    {topics_apps, c2} = Map.pop(c, pid)
    HeBroker.consumer_down(topics_apps, pid)

    {:noreply, c2}
  end
end