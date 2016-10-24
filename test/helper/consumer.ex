defmodule HeBroker.TestHelper.Consumer do

  alias HeBroker.Consumer

  def spawn_consumer(broker, topic, callbacks, function \\ fn _msg -> :ok end) do
    me = self()

    consumer = spawn fn ->
      Consumer.subscribe(broker, topic, callbacks)

      send me, {:"UP", self()}
      loop(function)
    end

    receive do
      {:"UP", ^consumer} ->
        consumer
    after 1_000 ->
      ExUnit.Assertions.flunk("consumer didn't start on time")
    end
  end

  def stop_consumer(consumer) do
    ref = Process.monitor(consumer)
    :erlang.exit(consumer, :beatit)

    receive do
      {:"DOWN", ^ref, _, ^consumer, _} ->
        :ok
    after 1_000 ->
      ExUnit.Assertions.flunk("consumer didn't stop on time")
    end
  end

  @docp """
  Produces a loop function for `spawn_consumer/4`.

  Acceps a 1-fun as a param. That function will be executed everytime that a
  message is received by the consumer; that function will receive the message
  as a param

  ## Example
      me = self()

      callbacks = [cast: fn pid, _, message, request -> send pid, {request, message} end]
      function = fn msg -> send me, {:consumed, msg} end

      spawn_consumer(UniversalBroker, "foo", callbacks, function)

      UniversalBroker.cast("foo", {"some", "message"})
      UniversalBroker.cast("foo", :something)

      assert_receive {:consumed, {%Request{}, {"some", "message"}}}
      assert_receive {:consumed, {%Request{}, :something}}
  """
  defp loop(function) do
    loop = fn loop ->
      receive do
        message ->
          function.(message)
          loop.(loop)
      end
    end

    loop.(loop)
  end
end