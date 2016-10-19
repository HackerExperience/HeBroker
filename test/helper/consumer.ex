defmodule HeBroker.TestHelper.Consumer do

  alias HeBroker.Consumer

  def spawn_consumer(broker, topic, callbacks, loop \\ default_loop()) do
    me = self()

    consumer = spawn fn ->
      Consumer.subscribe(broker, topic, callbacks)

      send me, {:"UP", self()}
      loop.()
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

  defp default_loop do
    fn ->
      receive do
        _ -> :ok
      end
    end
  end
end