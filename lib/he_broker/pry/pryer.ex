defmodule HeBroker.Pry.Pryer do

  alias HeBroker.Request
  alias HeBroker.Pry.MessageSent

  def start_link,
    do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  def annotate_origin_request(request = %Request{}) do
    GenServer.cast(__MODULE__, {:annotate, request, self()})
  end

  def annotate_sent(message = %MessageSent{}) do
    GenServer.cast(__MODULE__, {:annotate, message, self()})
  end

  def graph do
    GenServer.call(__MODULE__, :graph)
  end

  def init(_) do
    {:ok, %{graph: :digraph.new(), origin_requests: %{}}}
  end

  def handle_cast({:annotate, message = %MessageSent{}, publisher}, state) do
    origin = Map.get(state.origin_requests, publisher)

    :digraph.add_vertex(state.graph, message.request.message_id, message)

    unless is_nil(origin.message_id) do
      :digraph.add_edge(state.graph, origin.message_id, message.request.message_id)
    end

    {:noreply, Map.update!(state, :origin_requests, &Map.delete(&1, publisher))}
  end

  def handle_cast({:annotate, request = %Request{}, publisher}, state) do
    {:noreply, Map.update!(state, :origin_requests, &Map.put(&1, publisher, request))}
  end

  def handle_call(:graph, _, state) do
    {:reply, state.graph, state}
  end
end