defmodule HeBroker.Pry do

  alias HeBroker.Request, warn: false
  alias HeBroker.Pry.Pryer
  alias HeBroker.Pry.MessageSent

  @active? Application.get_env(:hebroker, :debug, Mix.env in [:test, :dev])

  defmacro pry_origin_request(request) do
    if @active? do
      quote do
        Pryer.annotate_origin_request(unquote(request))

        unquote(request)
      end
    else
      request
    end
  end

  defmacro pry_sent(callbacks, method, broker, topic, message, request) do
    if @active? do
      quote do
        callbacks = unquote(callbacks)
        consumers = Enum.count(callbacks)

        message = %MessageSent{
          broker: unquote(broker),
          topic: unquote(topic),
          message: unquote(message),
          method: unquote(method),
          request: unquote(request),
          publisher: self(),
          consumer_count: consumers}

        Pryer.annotate_sent(message)

        callbacks
      end
    else
      callbacks
    end
  end

  if @active? do
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

    def messages_sent(%Request{message_id: mid}, _opts \\ []) do
      g = Pryer.graph

      :digraph_utils.reachable([mid], g)
      |> Enum.map(&(:digraph.vertex(g, &1) |> elem(1)))
      |> Enum.map(&Map.fetch!(&1, :consumer_count))
      |> Enum.reduce(&(&1 + &2))
    end
  end
end