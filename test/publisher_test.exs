defmodule HeBroker.PublisherTest do

  use ExUnit.Case

  alias HeBroker.Broker
  alias HeBroker.Publisher
  alias HeBroker.TestHelper.Consumer, as: ConsumerHelper

  setup do
    {:ok, pid} = Broker.start_link()

    {:ok, broker: pid}
  end

  describe "messaging" do
    test "nothing happens when messaging a topic without consumers", %{broker: broker} do
      assert 0 === Broker.count_services_on_topic(broker, "empty")

      request = Publisher.cast(broker, "empty", fn -> raise RuntimeError end)

      assert 0 === HeBroker.Pry.messages_sent(request)
    end
  end

  describe "cast" do
  end

  describe "call" do
    test "calling a topic without consumers won't yield any result (and thus raise)", %{broker: broker} do
      assert 0 === Broker.count_services_on_topic(broker, "empty")

      assert_raise HeBroker.Request.TimeoutError, fn ->
        Publisher.call(broker, "empty", fn -> flunk() end, timeout: 100)
      end
    end

    test "calling a topic whose consumers doesn't reply raises", %{broker: broker} do
      assert 0 === Broker.count_services_on_topic(broker, "fail")

      me = self()

      ConsumerHelper.spawn_consumer(broker, "fail", call: fn _, _, message, _ -> send me, message; :noreply end)

      assert 1 === Broker.count_services_on_topic(broker, "fail")

      assert_raise HeBroker.Request.TimeoutError, fn ->
        Publisher.call(broker, "fail", :kek, timeout: 100)
      end

      assert_received :kek
    end

    test "when two different consumers respond a call, only the first received response is returned", %{broker: broker} do
      topic = "let:the:bodies:hit:the:floor"
      assert 0 === Broker.count_services_on_topic(broker, topic)

      me = self()
      call_fun1 = fn _, _, _, _ -> send me, {:received, 1}; {:reply, 1} end
      call_fun2 = fn _, _, _, _ -> send me, {:received, 2}; {:reply, 2} end

      ConsumerHelper.spawn_consumer(broker, topic, call: call_fun1)
      ConsumerHelper.spawn_consumer(broker, topic, call: call_fun2)

      assert 2 === Broker.count_services_on_topic(broker, topic)

      {_request, reply} = Publisher.call(broker, topic, :yay, timeout: 1000)

      assert 1 === reply or 2 === reply
      assert_received {:received, 1}
      assert_received {:received, 2}
      refute_received _ # Ensure that no garbage was sent to our mailbox
    end
  end
end