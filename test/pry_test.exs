defmodule HeBroker.PryTest do
  use ExUnit.Case, async: true

  alias HeBroker.Pry
  alias HeBroker.Publisher
  alias HeBroker.Consumer

  alias HeBroker.Request
  alias HeBroker.Pry.MessageSent
  alias HeBroker.Pry.MessageLost

  alias HeBroker.TestHelper.Consumer, as: ConsumerHelper

  setup do
    {:ok, broker} = HeBroker.Broker.start_link()

    {:ok, broker: broker}
  end

  @docp """
  The reason for this is to make the callback of each consumer unique.
  If two consumers with the same callback consume the same topic, they will
  be round-robin'ed at message passing (ie: the message will go to only one
  of the two consumers each time)
  """
  defp callback do
    hash = :crypto.strong_rand_bytes(16)

    fn pid, _, message, request ->
      send pid, {message, request}

      hash
    end
  end

  defp loop_ping(broker, send_to) do
    fn {message, request} ->
      send_to
      |> List.wrap()
      |> Enum.each(&Publisher.cast(broker, &1, message, request: request))
    end
  end

  defp sort_branches(branches) do
    branches
    # |> IO.inspect
    |> Enum.sort(fn el1, el2 ->
      el1
      |> Enum.zip(el2)
      |> Enum.reduce_while(nil, fn
        {%Request{}, %Request{}}, _ ->
          {:cont, nil}
        {el1, el2}, _ when is_map(el1) and is_map(el2) ->
          el1.topic == el2.topic && {:cont, nil} || {:halt, el1.topic < el2.topic}
        {el1, el2}, _ ->
          el1 == el2 && {:cont, nil} || {:halt, el1 < el2}
      end)
      |> case do
        nil ->
          Enum.count(el1) < Enum.count(el2)
        v ->
          v
      end
    end)
  end

  describe "prying" do
    test "pries requests", %{broker: broker} do
      me = self()

      ConsumerHelper.spawn_consumer(broker, "foo", cast: fn _, _, _, _ -> send me, :ping end)

      request1 = Publisher.cast(broker, "bar", :ping)
      request2 = Publisher.cast(broker, "foo", :ping)

      # Yes, testing async things is hard
      assert_receive :ping

      assert 0 === Pry.messages_sent(request1)
      assert 1 === Pry.messages_sent(request2)
    end

    test "tracks the graph of messages", %{broker: broker} do
      loop_factory = fn send_to ->
        fn {:ping, request} ->
          Publisher.cast(broker, send_to, :ping, request: request)
        end
      end

      # Foo consumer will relay the message to "bar"
      ConsumerHelper.spawn_consumer(broker, "foo", [cast: callback], loop_factory.("bar"))
      # Bar consumer will relay the message to "baz"
      ConsumerHelper.spawn_consumer(broker, "bar", [cast: callback], loop_factory.("baz"))

      # We will consume the topic "baz" so we can know when the request is completed
      Consumer.subscribe(broker, "baz", cast: callback)

      request = Publisher.cast(broker, "foo", :ping)

      assert_receive {:ping, _}

      # 1 message sent to "foo" -> 1 message sent to "bar" -> 1 message sent to "baz"
      assert 3 === Pry.messages_sent(request)
      assert Enum.sort(~w/foo bar baz/) === Enum.sort(Pry.topics(request))
    end

    test "tracks all branches from the original request", %{broker: broker} do
      # MAIN BRANCH
      ConsumerHelper.spawn_consumer(broker, "foo", [cast: callback], loop_ping(broker, "bar"))
      ConsumerHelper.spawn_consumer(broker, "bar", [cast: callback], loop_ping(broker, ["baz", "zab"]))
      ConsumerHelper.spawn_consumer(broker, "baz", [cast: callback], loop_ping(broker, "test"))

      # BRANCH A
      ConsumerHelper.spawn_consumer(broker, "foo", [cast: callback], loop_ping(broker, ["abc", "def"]))
      ConsumerHelper.spawn_consumer(broker, "abc", [cast: callback], loop_ping(broker, "test"))

      # BRANCH A.2
      ConsumerHelper.spawn_consumer(broker, "def", [cast: callback], loop_ping(broker, "test"))

      # Every 3ms check if the tree expanded, stops when tree doesn't seem to
      # expand anymore. Fails if take more than 3s
      request =
        broker
        |> Publisher.cast("foo", :ping)
        |> Pry.wait_expansion(3_000, 25)


      # THE REQUEST TREE
      #
      # *ORIGIN*
      # |
      # | - [foo]
      # |   | - [bar]
      # |       | - [zab]*
      # |       | - [baz]
      # |           | - [test]*
      # |
      # | - [foo]
      #     | - [abc]
      #     |   | - [test]*
      #     | - [def]
      #         | - [test]*
      #
      # TOTAL: 6 messages (* there is no consumer for topics "zab" and "test", so it won't count as a message sent)
      # TOPICS: foo bar baz zab test abc def

      ## Only those that were received
      expected = Enum.sort(~w/foo bar baz abc def/)
      assert 6 === Pry.messages_sent(request)
      assert expected === Enum.sort(Pry.topics(request, unique: true))

      ## Including "lost" messages
      expected = Enum.sort(~w/foo bar baz zab test abc def/)
      assert 10 === Pry.messages_sent(request, include_lost: true)
      assert expected === Enum.sort(Pry.topics(request, unique: true, include_lost: true))

      all = Enum.sort(Pry.topics(request, include_lost: true))

      assert 3 === all |> Enum.filter(&(&1 === "test")) |> Enum.count()
      assert 2 === all |> Enum.filter(&(&1 === "foo")) |> Enum.count()
    end
  end

  describe "Pry.branches/2" do
    test "are paths to each leaf", %{broker: broker} do
      ConsumerHelper.spawn_consumer(broker, "foo", [cast: callback], loop_ping(broker, "bar"))
      ConsumerHelper.spawn_consumer(broker, "bar", [cast: callback], loop_ping(broker, ["baz", "zab"]))

      branches =
        broker
        |> Publisher.cast("foo", :ping)
        |> Pry.wait_expansion(3_000, 25)
        |> Pry.branches()
        |> sort_branches()

      assert [branch1, branch2] = branches
      assert ["foo", "bar", "baz"] === branch1
      assert ["foo", "bar", "zab"] === branch2
    end

    test "can return structs to represent the actions", %{broker: broker} do
      ConsumerHelper.spawn_consumer(broker, "foo", [cast: callback], loop_ping(broker, "bar"))
      ConsumerHelper.spawn_consumer(broker, "bar", [cast: callback], loop_ping(broker, ["baz", "zab"]))
      ConsumerHelper.spawn_consumer(broker, "zab", [cast: callback])

      branches =
        broker
        |> Publisher.cast("foo", :ping)
        |> Pry.wait_expansion(3_000, 25)
        |> Pry.branches(simplify: false)
        |> sort_branches()

      assert [branch1, branch2] = branches
      assert [%Request{}, %MessageSent{topic: "foo"}, %MessageSent{topic: "bar"}, %MessageLost{topic: "baz"}] = branch1
      assert [%Request{}, %MessageSent{topic: "foo"}, %MessageSent{topic: "bar"}, %MessageSent{topic: "zab"}] = branch2
    end
  end
end