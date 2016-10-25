defmodule HeBroker.PryTest do

  use ExUnit.Case

  alias HeBroker.Pry
  alias HeBroker.Publisher
  alias HeBroker.Consumer
  alias HeBroker.TestHelper.Consumer, as: ConsumerHelper

  setup do
    {:ok, broker} = HeBroker.Broker.start_link()

    {:ok, broker: broker}
  end

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
    callback = fn pid, _, message, request ->
      send pid, {message, request}
    end
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
    # The reason for this is to make the callback of each consumer unique.
    # If two consumers with the same callback consume the same topic, they will
    # be round-robin'ed at message passing (ie: the message will go to only one
    # of the two consumers each time)
    callback = fn ->
      hash = :crypto.strong_rand_bytes(3)

      fn pid, _, message, request ->
        send pid, {message, request}

        hash
      end
    end
    loop_factory = fn send_to ->
      fn {:ping, request} ->
        send_to
        |> List.wrap()
        |> Enum.each(&Publisher.cast(broker, &1, :ping, request: request))
      end
    end

    # MAIN BRANCH
    ConsumerHelper.spawn_consumer(broker, "foo", [cast: callback.()], loop_factory.("bar"))
    ConsumerHelper.spawn_consumer(broker, "bar", [cast: callback.()], loop_factory.(["baz", "zab"]))
    ConsumerHelper.spawn_consumer(broker, "baz", [cast: callback.()], loop_factory.("test"))

    # BRANCH A
    ConsumerHelper.spawn_consumer(broker, "foo", [cast: callback.()], loop_factory.(["abc", "def"]))
    ConsumerHelper.spawn_consumer(broker, "abc", [cast: callback.()], loop_factory.("test"))

    # BRANCH A.2
    ConsumerHelper.spawn_consumer(broker, "def", [cast: callback.()], loop_factory.("test"))

    # We will consume the topic "test" so we can know when the request is completed
    Consumer.subscribe(broker, "test", cast: callback.())

    request = Publisher.cast(broker, "foo", :ping)

    who_will_message_us = ~w/baz abc def/
    Enum.each(who_will_message_us, fn _ -> assert_receive {:ping, _} end)

    # THE REQUEST TREE
    #
    # *ORIGIN*
    # |
    # | - [foo]
    # |   | - [bar]
    # |       | - [zab]*
    # |       | - [baz]
    # |           | - [test]
    # |
    # | - [foo]
    #     | - [abc]
    #     |   | - [test]
    #     | - [def]
    #         | - [test]
    #
    # TOTAL: 9 messages (* there is no consumer for topic "zab", so it won't count as a message sent)
    # TOPICS: foo(2 msgs) bar baz zab(0 msgs) test(3 msgs) abc def

    ## Only those that were received
    assert 9 === Pry.messages_sent(request)
    assert Enum.sort(~w/foo bar baz test abc def/) === Enum.sort(Pry.topics(request, unique: true))

    ## Including "lost" messages
    assert 10 === Pry.messages_sent(request, include_lost: true)
    assert Enum.sort(~w/foo bar baz zab test abc def/) === Enum.sort(Pry.topics(request, unique: true, include_lost: true))

    all = Enum.sort(Pry.topics(request, include_lost: true))

    assert 3 === all |> Enum.filter(&(&1 === "test")) |> Enum.count()
    assert 2 === all |> Enum.filter(&(&1 === "foo")) |> Enum.count()
  end
end