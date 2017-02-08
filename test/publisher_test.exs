defmodule HeBroker.PublisherTest do
  use ExUnit.Case

  alias HeBroker.Broker
  alias HeBroker.Publisher
  alias HeBroker.Consumer
  alias HeBroker.Request
  alias HeBroker.TestHelper.Consumer, as: ConsumerHelper

  import ExUnit.CaptureLog

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
    test "broken callback won't affect the client", %{broker: broker} do
      callback = [cast: fn _, _, _, _ -> raise RuntimeError end]
      cons = ConsumerHelper.spawn_consumer(broker, "error", callback)

      error_log = capture_log fn ->
        assert %Request{} = Publisher.cast(broker, "error", :kthxbye)
        :timer.sleep(100) # Let the task worker crash
      end

      # If the test reached here, obviously the client hasn't broken
      refute_received _

      # But the task that was running the callback did and thus an error log was
      # issued naturaly
      assert error_log =~ "runtime"

      # As the fail was on the callback, obviously the consumer should not have
      # been affected
      assert Process.alive?(cons)
    end
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
      assert_receive {:received, 1}
      assert_receive {:received, 2}
      refute_received _ # Ensure that no garbage was sent to our mailbox
    end
  end

  describe "request" do
    test "if no request is provided, a default is generated", %{broker: broker} do
      callback = fn pid, _, _, request ->
        send pid, {:request, request}
      end

      Consumer.subscribe(broker, "test", cast: callback)

      request_origin = Publisher.cast(broker, "test", :ping)

      assert_receive {:request, request}
      assert Map.has_key?(request, :headers)
      assert request.headers === request_origin.headers
      refute request == request_origin
      refute request.headers.app_id
      refute request.headers.correlation_id
    end

    test "if a request is passed, it's headers are kept", %{broker: broker} do
      callback = fn pid, _, _, request ->
        send pid, {:request, request}
      end

      loop_foo = fn {:request, request} ->
        Publisher.cast(broker, "bar", :ping, request: request)
      end
      loop_bar = fn {:request, request} ->
        Publisher.cast(broker, "test", :ping, request: request)
      end

      ConsumerHelper.spawn_consumer(broker, "foo", [cast: callback], loop_foo)
      ConsumerHelper.spawn_consumer(broker, "bar", [cast: callback], loop_bar)
      Consumer.subscribe(broker, "test", cast: fn pid, _, _, request -> send pid, {:request, request} end)

      Publisher.cast(broker, "foo", :ping, headers: [app_id: "hebroker", correlation_id: "foobar"])
      assert_receive {:request, request}

      assert "hebroker" === request.headers.app_id
      assert "foobar" === request.headers.correlation_id
    end
  end
end