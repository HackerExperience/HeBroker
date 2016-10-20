defmodule HeBroker.ConsumerTest do

  use ExUnit.Case

  alias HeBroker.Broker
  alias HeBroker.Consumer
  alias HeBroker.Publisher
  alias HeBroker.TestHelper.Consumer, as: ConsumerHelper

  setup do
    {:ok, pid} = Broker.start_link()

    {:ok, broker: pid}
  end

  defp test_callbacks do
    me = self()

    [
      cast: fn pid, topic, message, request -> send me, {:hebroker, :cast, pid, topic, message, request} end,
      call: fn pid, topic, message, request -> send me, {:hebroker, :call, pid, topic, message, request} end]
  end

  describe "subscription" do
    test "consumer can subscribe to a topic", %{broker: broker} do
      refute Broker.subscribed?(broker, self())

      Consumer.subscribe(broker, "test:subject", test_callbacks)

      assert Broker.subscribed?(broker, self())
    end

    test "consumer can subscribe to several topics", %{broker: broker} do
      refute Broker.subscribed?(broker, self())

      topics = ~w/test:subject something:else deep:nested:topic you/

      Enum.each(topics, &Consumer.subscribe(broker, &1, test_callbacks))

      assert Broker.subscribed?(broker, self())
      assert Enum.all?(topics, &Broker.subscribed?(broker, self(), &1))
      refute Broker.subscribed?(broker, self(), "random:topic")
    end

    test "more than one consumer can be subscribed to a topic", %{broker: broker} do
      consumers = for _ <- 0..9, do: ConsumerHelper.spawn_consumer(broker, "test", test_callbacks)

      assert Enum.all?(consumers, &Broker.subscribed?(broker, &1, "test"))
    end

    test "if consumer dies, it is unsubscribed", %{broker: broker} do
      consumer = ConsumerHelper.spawn_consumer(broker, "test", test_callbacks)

      assert Broker.subscribed?(broker, consumer)

      ConsumerHelper.stop_consumer(consumer)

      refute Broker.subscribed?(broker, consumer)
    end

    test "fails if no callback is provided", %{broker: broker} do
      assert_raise HeBroker.InvalidCallbackError, fn ->
        Consumer.subscribe(broker, "test", [])
      end
    end

    test "fails if callback is not 4-arity", %{broker: broker} do
      assert_raise HeBroker.InvalidCallbackError, fn ->
        callbacks = [
          cast: fn _, _, _, _ -> :ok end,
          call: fn _, _ -> "this is an invalid callback" end]

          Consumer.subscribe(broker, "test", callbacks)
      end
    end
  end

  describe "messaging" do
    test "message is round-robin-ed", %{broker: broker} do
      me = self()

      ycombinator_factory = fn identifier ->
        fn ->
          loop = fn loop ->
            receive do
              message ->
                send me, {:y, identifier, message}
                loop.(loop)
            end
          end

          loop.(loop)
        end
      end

      ycombinator1 = ycombinator_factory.(:y3)
      ycombinator2 = ycombinator_factory.(:y2)

      cast_callback = fn pid, _, message, _ -> send pid, message end

      ConsumerHelper.spawn_consumer(broker, "test", [cast: cast_callback], ycombinator1)
      ConsumerHelper.spawn_consumer(broker, "test", [cast: cast_callback], ycombinator2)

      Publisher.cast(broker, "test", :foo)
      Publisher.cast(broker, "test", :bar)
      Publisher.cast(broker, "test", :baz)

      spawn_publisher = fn message ->
        me = self()
        spawn fn ->
          Publisher.cast(broker, "test", message)

          send me, :ping
        end

        receive do
          :ping -> :ok
        after 1_000 -> flunk()
        end
      end

      spawn_publisher.(:kek)
      spawn_publisher.(:lol)
      spawn_publisher.(:bbq)

      assert_receive {:y, y1, :foo}
      assert_receive {:y, y2, :bar}
      assert_receive {:y, y3, :baz}
      assert_receive {:y, y4, :kek}
      assert_receive {:y, y5, :lol}
      assert_receive {:y, y6, :bbq}
      assert y1 === y3
      assert y1 === y5
      assert y2 === y4
      assert y2 === y6
      assert y1 !== y2
    end
  end
end