defmodule HeBroker.Pry do

  alias HeBroker.Request
  alias HeBroker.Pry.Pryer
  alias HeBroker.Pry.Just
  alias HeBroker.Pry.Dont

  alias HeBroker.Pry.MessageRelayed
  alias HeBroker.Pry.MessageSent
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

    vertices =
      immediate? &&
      :digraph.out_neighbours(g, mid)
      || :digraph_utils.reachable_neighbours([mid], g)

    vertices
    |> Enum.map(&(:digraph.vertex(g, &1) |> elem(1)))
    |> maybe_reject_lost(reject_lost?)
    |> Enum.count()
  end

  @spec wait_expansion(Request.t, non_neg_integer, non_neg_integer) :: Request.t
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
  def wait_expansion(r = %Request{message_id: mid}, timeout \\ 15_000, step_cooldown \\ 250) do
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
    r
  end

  @spec branches(Request.t, Keyword.t) :: [[Request.t | MessageLost.t | MessageSent.t] | [topic :: String.t]]
  @doc """
  Returns all _branches_ of the chain reaction that emanates from the original
  request

  Does so by collecting all vertices that are reachable from the original request,
  filtering those that are leafs and rebuilding their path.

  Thus, this function will return a collection of lists containing the `topics`
  that where in the path between the original request and the leaf (last request)

  Note that the list of topics is sorted as [v1, ..., v2] and the collection of
  vertices lists is not sorted

  `opts` is a proplist whose possible values are
  - `{:simplify, boolean}`, if true (default), will replace all vertices with
  their topic names, remove the origin request and possibly remove all _message_lost_
  vertices
  - `{:include_lost, boolean}`, if false (default) will remove all _message_lost_
  vertices IF `simplify` is true

  ### Examples
  ```
      request = Publisher.cast("foo", msg)
      # *ORIGIN*
      # |- foo
      # |  |- bar
      # |     |- foobar
      # |
      # |- foo
      #    |- baz
      #    |  |- zab
      #    |     |- abc*
      #    |
      #    |- ooo
      #
      #
      # that is,
      # foo -> bar -> foobar
      # foo -> baz -> zab -> abc # abc has no consumer and thus the msg is lost
      # foo -> ooo

      Pry.branches(request)
      # [["foo", "baz", "zab"], ["foo", "bar", "foobar"], ["foo", "ooo"]]

      Pry.branches(request, include_lost: true)
      # [["foo", "bar", "foobar"], ["foo", "ooo"], ["foo", "baz", "zab", "abc"]]

      Pry.branches(request, simplify: false, include_lost: true)
      # [
      #   [
      #     %HeBroker.Request{},
      #     %HeBroker.Pry.MessageSent{},
      #     %HeBroker.Pry.MessageSent{},
      #     %HeBroker.Pry.MessageSent{}],
      #   [
      #     %HeBroker.Request{},
      #     %HeBroker.Pry.MessageSent{},
      #     %HeBroker.Pry.MessageSent{}],
      #   [
      #     %HeBroker.Request{},
      #     %HeBroker.Pry.MessageSent{},
      #     %HeBroker.Pry.MessageSent{},
      #     %HeBroker.Pry.MessageSent{},
      #     %HeBroker.Pry.MessageLost{}]]

      Pry.branches(request, simplify: false)
      # [
      #   [
      #     %HeBroker.Request{},
      #     %HeBroker.Pry.MessageSent{},
      #     %HeBroker.Pry.MessageSent{},
      #     %HeBroker.Pry.MessageSent{}],
      #   [
      #     %HeBroker.Request{},
      #     %HeBroker.Pry.MessageSent{},
      #     %HeBroker.Pry.MessageSent{}],
      #   [
      #     %HeBroker.Request{},
      #     %HeBroker.Pry.MessageSent{},
      #     %HeBroker.Pry.MessageSent{},
      #     %HeBroker.Pry.MessageSent{}]]


      request = Publisher.cast("unavailable", msg)
      # No consumer on topic "unavailable", message lost

      Pry.branches(request)
      # []

      Pry.branches(request, include_lost: true)
      # [["unavailable"]]

      Pry.branches(request, simplify: false, include_lost: true)
      # [[%HeBroker.Request{}, %HeBroker.Pry.MessageLost{}]]

      Pry.branches(request, simplify: false)
      # [[%HeBroker.Request{}]]
  ```
  """
  def branches(%Request{message_id: mid}, opts \\ []) do
    g = Pryer.graph

    reject_lost? = not Keyword.get(opts, :include_lost, false)

    [mid]
    |> :digraph_utils.reachable(g)
    |> Enum.map(&:digraph.vertex(g, &1))
    |> Enum.filter(&(match?({_, %MessageLost{}}, &1) or match?({_, %MessageSent{}}, &1)))
    |> Enum.map(fn {request_id, label} ->
      # If the message was lost and this request asks to reject lost messages,
      # consider it's parent as the leaf of the tree instead
      vertex = if reject_lost? and match?(%MessageLost{}, label) do
        g |> :digraph.in_neighbours(request_id) |> List.first()
      else
        request_id
      end

      vertices = vertex == mid && [mid] || :digraph.get_path(g, mid, vertex)

      vertices
      |> Enum.map(&:digraph.vertex(g, &1))
      |> Enum.map(&elem(&1, 1))
    end)
    |> Enum.uniq()
    |> maybe_simplify_branches(Keyword.get(opts, :simplify, true), reject_lost?)
    |> rename_structs_from_branch()
    |> make_branch_date_string()
    |> Enum.reject(&(&1 == []))
  end

  @spec root_count() :: non_neg_integer
  def root_count do
    g = Pryer.graph

    :digraph.out_edges(g, :mu)
    |> Enum.count()
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

  defp maybe_simplify_branches(branches, false, _),
    do: branches
  defp maybe_simplify_branches(branches, true, reject_lost?) do
    branches
    |> Enum.map(fn branch ->
      branch
      |> Enum.reject(&(match?(%Request{}, &1)))
      |> Enum.reject(&(reject_lost? and match?(%MessageLost{}, &1)))
      |> Enum.map(&(&1.topic))
    end)
  end

  defp rename_structs_from_branch(branches) do
    branches
    |> Enum.map(fn branch ->
      branch
      |> Enum.map(fn
        m = %MessageRelayed{} ->
          Map.put(m, :__struct__, MessageSent)
        x ->
          x
      end)
    end)
  end

  defp make_branch_date_string(branches) do
    branches
    |> Enum.map(fn branch ->
      branch
      |> Enum.map(fn
        m = %{moment: moment} ->
          Map.put(m, :moment, DateTime.to_string(moment))
        m ->
          m
      end)
    end)
  end

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