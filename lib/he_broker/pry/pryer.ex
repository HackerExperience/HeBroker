defmodule HeBroker.Pry.Pryer do
  @moduledoc """
  Stores a graph with the flow of requests
  """

  alias HeBroker.Request
  alias HeBroker.Pry.MessageSent
  alias HeBroker.Pry.MessageLost

  def start_link,
    do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  def annotate_origin_request(request = %Request{}),
    do: GenServer.cast(__MODULE__, {:annotate, :origin, request})

  def annotate_sent(original = %Request{}, sent = %MessageSent{}),
    do: GenServer.cast(__MODULE__, {:annotate, :sent, original, sent})

  def annotate_lost(original = %Request{}, lost = %MessageLost{}),
    do: GenServer.cast(__MODULE__, {:annotate, :lost, original, lost})

  def graph,
    do: GenServer.call(__MODULE__, :graph)

  @doc false
  def init(_) do
    {:ok, %{graph: :digraph.new([:acyclic])}}
  end

  @doc false
  def handle_cast({:annotate, :origin, request}, state) do
    unless :digraph.vertex(state.graph, request.message_id) do
      :digraph.add_vertex(state.graph, request.message_id, request)
    end

    {:noreply, state}
  end

  def handle_cast({:annotate, :sent, original, sent}, state) do
    :digraph.add_vertex(state.graph, sent.request.message_id, sent)

    :digraph.add_edge(state.graph, original.message_id, sent.request.message_id)

    {:noreply, state}
  end

  def handle_cast({:annotate, :lost, original, lost_message}, state) do
    vertex_identifier = make_ref()

    :digraph.add_vertex(state.graph, vertex_identifier, lost_message)

    :digraph.add_edge(state.graph, original.message_id, vertex_identifier)

    {:noreply, state}
  end

  @doc false
  def handle_call(:graph, _, state) do
    {:reply, state.graph, state}
  end
end