defmodule HeBroker.Pry do

  alias HeBroker.Request, warn: false
  alias HeBroker.Pry.Pryer
  alias HeBroker.Pry.Just
  alias HeBroker.Pry.Dont

  @active? Application.get_env(:hebroker, :debug, Mix.env in [:test, :dev])

  @doc """
  Annotates on the Pryer that a message was received
  """
  def pry_origin_request(request) do
    if @active? do
      Just.pry_origin_request(request)
    else
      Dont.pry_origin_request(request)
    end
  end

  @doc """
  Annotates on the Pryer that a message is being sent
  """
  def pry_sent(callbacks, method, broker, topic, message, request) do
    if @active? do
      Just.pry_sent(callbacks, method, broker, topic, message, request)
    else
      Dont.pry_sent(callbacks, method, broker, topic, message, request)
    end
  end

  @spec topics(Request.t, Keyword.t) :: [String.t]
  @doc """
  Returns an unsorted list of topics that the derived from the original request

  `:unique` can be passed as a `opts` to remove repeated topics

  Note that this will include the topic where the original message was sent to
  """
  def topics(%Request{message_id: mid}, opts \\ []) do
    g = Pryer.graph

    topics =
      :digraph_utils.reachable([mid], g)
      |> Enum.map(&(:digraph.vertex(g, &1) |> elem(1)))
      |> Enum.map(&Map.fetch!(&1, :topic))

    if Keyword.get(opts, :unique, false) do
      Enum.uniq(topics)
    else
      topics
    end
  end

  @spec messages_sent(Request.t, Keyword.t) :: non_neg_integer
  def messages_sent(%Request{message_id: mid}, opts \\ []) do
    g = Pryer.graph

    vertices = if Keyword.get(opts, :immediate, false) do
      :digraph.out_neighbours(g, mid)
    else
      :digraph_utils.reachable([mid], g)
    end

    vertices
    |> Enum.map(&(:digraph.vertex(g, &1) |> elem(1)))
    |> Enum.map(&Map.fetch!(&1, :consumer_count))
    |> Enum.reduce(&(&1 + &2))
  end

  defmodule Dont do
    @moduledoc false

    def pry_origin_request(request) do
      request
    end

    def pry_sent(callbacks, _, _, _, _, _) do
      callbacks
    end
  end

  defmodule Just do
    @moduledoc false

    alias HeBroker.Pry.Pryer
    alias HeBroker.Pry.MessageSent

    def pry_origin_request(request) do
      Pryer.annotate_origin_request(request)

      request
    end

    def pry_sent(callbacks, method, broker, topic, message, request) do
      consumers = Enum.count(callbacks)

      message = %MessageSent{
        broker: broker,
        topic: topic,
        message: message,
        method: method,
        request: request,
        publisher: self(),
        consumer_count: consumers}

      Pryer.annotate_sent(message)

      callbacks
    end
  end
end