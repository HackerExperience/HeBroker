defmodule HeBroker.Pry do

  alias HeBroker.Request
  alias HeBroker.Pry.Pryer
  alias HeBroker.Pry.Just
  alias HeBroker.Pry.Dont

  alias HeBroker.Pry.MessageLost

  @active? Application.get_env(:hebroker, :debug, Mix.env in [:test, :dev])
  @processor @active? && Just || Dont

  @spec pry_origin_request(Request.t) :: Request.t
  defdelegate pry_origin_request(request),
    to: @processor

  @spec pry_sent(Request.t, Request.t, String.t, term) :: Request.t
  @doc """
  Annotates on the Pryer that a message is being sent
  """
  defdelegate pry_sent(sent_request, original_request, topic, message),
    to: @processor

  @spec pry_maybe_message_lost([RouteMap.partial], Publisher.topic, Publisher.message, Request.t) :: [RouteMap.partial]
  @doc """
  Checks if there is any callback on the current topic, if none is found,
  annotates that the message was lost
  """
  def pry_maybe_message_lost(partials, topic, message, request)
  def pry_maybe_message_lost([], topic, message, request),
    do: @processor.pry_maybe_message_lost(topic, message, request)
  def pry_maybe_message_lost(partials, _, _, _),
    do: partials

  @spec topics(Request.t, Keyword.t) :: [String.t]
  @doc """
  Returns an unsorted list of topics that the derived from the original request

  `opts` accepts the following values:
  - `:unique` can be passed as `true` to remove repeated topics
  - `:include_lost` can be passed as a `true` to also include messages that were
  lost (ie: sent to a topic without able consumers)
  """
  def topics(%Request{message_id: mid}, opts \\ []) do
    g = Pryer.graph

    reject_lost? = not Keyword.get(opts, :include_lost, false)
    unique? = Keyword.get(opts, :unique, false)

    topics =
      :digraph_utils.reachable_neighbours([mid], g)
      |> Enum.map(&(:digraph.vertex(g, &1) |> elem(1)))
      |> maybe_reject_lost(reject_lost?)
      |> Enum.map(&Map.fetch!(&1, :topic))

    if unique?,
      do: Enum.uniq(topics),
      else: topics
  end

  @spec messages_sent(Request.t, Keyword.t) :: non_neg_integer
  @doc """
  Returns the count of messages sent derived from the original request

  `opts` accepts the following values:
  - `:immediate`, if true returns only the count of messages sent using this
  request as immediate origin
  - `:include_lost`, if true also includes messages that were lost (ie: sent to
  a topic without able consumers)
  """
  def messages_sent(%Request{message_id: mid}, opts \\ []) do
    g = Pryer.graph

    reject_lost? = not Keyword.get(opts, :include_lost, false)
    immediate? = Keyword.get(opts, :immediate, false)

    immediate?
    && :digraph.out_neighbours(g, mid)
    || :digraph_utils.reachable_neighbours([mid], g)
    |> Enum.map(&(:digraph.vertex(g, &1) |> elem(1)))
    |> maybe_reject_lost(reject_lost?)
    |> Enum.count()
  end

  @doc """
  Waits `timeout` for the request to expand it's chain reaction tree.

  Does so by checking if the tree expanded every `step_cooldown` ms, if the count
  of messages sent (including lost) between two checks is the same, this function
  assumes that the chain reaction tree is as expanded as possible.

  If your chain reaction tree has heavy operations that can take quite some time,
  it is advised to increase the `step_cooldown`

  ## Example

  ### Simple universe of effects
  ```
      broker
      |> Publisher.cast("chain:reaction:small", :doit)
      |> Pry.wait_expansion()
      |> Pry.messages_sent()
      |> Kernel.>=(5)
      |> assert
  ```

  ### Universe of effects with heavy-load functions
  ```
      broker
      |> Publisher.cast("chain:reaction:humongous", :doit)
      |> Pry.wait_expansion(60_000, 1_000)
      |> Pry.messages_sent()
      |> Kernel.>=(50)
      |> assert
  """
  def wait_expansion(%Request{message_id: mid}, timeout \\ 15_000, step_cooldown \\ 250) do
    g = Pryer.graph

    task = Task.async fn ->
      loop = fn loop, count ->
        count2 = :digraph_utils.reachable_neighbours([mid], g) |> Enum.count()

        if count === count2 do
          :ok
        else
          :timer.sleep(step_cooldown)
          loop.(loop, count2)
        end
      end

      loop.(loop, 0)
    end

    Task.await(task, timeout)
  end

  @docp """
  Little helper made to receive a pipelined collection of vertices (actually their
  labels) and filter them to remove those that represent _lost_ messages (ie:
  messages sent to a topic without any consumer)
  """
  defp maybe_reject_lost(vertices, false),
    do: vertices
  defp maybe_reject_lost(vertices, true),
    do: Enum.reject(vertices, &match?(%MessageLost{}, &1))


  defmodule Dont do
    @moduledoc false

    def pry_origin_request(request),
      do: request
    def pry_sent(request, _, _, _),
      do: request
    def pry_maybe_message_lost(_, _, _),
      do: []
  end


  defmodule Just do
    @moduledoc false

    alias HeBroker.Pry.Pryer
    alias HeBroker.Pry.MessageSent
    alias HeBroker.Pry.MessageLost

    def pry_origin_request(request) do
      Pryer.annotate_origin_request(request)

      request
    end

    def pry_sent(sent_request, original_request, topic, message) do
      sent_message = %MessageSent{
        request: sent_request,
        message: message,
        moment: DateTime.utc_now(),
        topic: topic}

      Pryer.annotate_sent(original_request, sent_message)

      sent_request
    end

    def pry_maybe_message_lost(topic, message, request) do
      lost_message = %MessageLost{
        topic: topic,
        message: message,
        moment: DateTime.utc_now()}

      Pryer.annotate_lost(request, lost_message)

      []
    end
  end
end