defmodule HeBroker.ConsumerTest do
  use ExUnit.Case, async: true

  alias HeBroker.Broker
  alias HeBroker.Consumer
  alias HeBroker.TestHelper.Consumer, as: ConsumerHelper

  setup do
    {:ok, broker} = Broker.start_link()

    {:ok, broker: broker}
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

    test "only one callback is required", %{broker: broker} do
      callbacks = [cast: fn _, _, _, _ -> :ok end]
      Consumer.subscribe(broker, "test1", callbacks)
      assert Broker.subscribed?(broker, self(), "test1")

      callbacks = [call: fn _, _, _, _ -> :noreply end]
      Consumer.subscribe(broker, "test2", callbacks)
      assert Broker.subscribed?(broker, self(), "test2")
    end

    test "refuses non-string topics", %{broker: broker} do
      invalid_topics = [
        :invalidtopic,
        {:invalidtopic},
        {:invalid, :topic},
        %{invalid: :topic},
        %{invalid: "topic"},
        %{"invalid" => :topic},
        %{"invalid" => "topic"},
        {"invalid:topic"},
        ["invalid:topic"],
        ["invalid", "topic"],
        11
      ]

      Enum.each(invalid_topics, fn topic ->
        assert_raise FunctionClauseError, fn ->
          Consumer.subscribe(broker, topic, test_callbacks)
        end
      end)
    end
  end
end