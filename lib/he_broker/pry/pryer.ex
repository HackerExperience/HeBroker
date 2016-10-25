defmodule HeBroker.Pry.Pryer do
  @moduledoc """
  Stores a graph with the flow of requests
  """

  alias HeBroker.Request
  alias HeBroker.Pry.MessageRelayed
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
    # Right now we are using the directed graph as a magical polytree
    g = :digraph.new([:acyclic])

    :digraph.add_vertex(g, :mu)

    {:ok, %{graph: g}}
  end

  @doc false
  def handle_cast({:annotate, :origin, request}, state) do
    case :digraph.vertex(state.graph, request.message_id) do
      {_, m = %MessageSent{}} ->
        # If the request is already in the graph, let's hack it to describe that
        # the message is going to be relayed somewhere else
        :digraph.add_vertex(state.graph, request.message_id, Map.put(m, :__struct__, MessageRelayed))
        # :digraph.add_vertex(state.graph, request.message_id, struct(MessageRelayed, Map.from_struct(m)))
      false ->
        # If the request doesn't exists in the graph, it is the root of a new
        # chain reaction
        :digraph.add_vertex(state.graph, request.message_id, request)

        # Links all the roots of tree to a common root of everything
        :digraph.add_edge(state.graph, :mu, request.message_id)
      _ ->
        # The request is already on the graph and already has caused expansion on
        # the tree
        :ok
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