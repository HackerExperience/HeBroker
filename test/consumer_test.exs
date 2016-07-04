defmodule HeBroker.ConsumerTest do
  use ExUnit.Case

  setup do
    {:ok, _} = HeBroker.start_link
    publisher = HeBroker.Publisher.start_link
    {:ok, publisher: publisher}
  end

  test "Subscribe to a topic and receive a cast", %{publisher: publisher} do
    HeBroker.Consumer.subscribe(:consumer, "foo:bar", cast: fn pid,_,z -> send pid, {:ok, z} end)

    HeBroker.Publisher.cast(publisher, "foo:bar", "lambda")
    assert_receive {:ok, "lambda"}
  end

  test "Subscribe to a topic and receive a call"

  test "Subscribe to different topics with different cast functions"

  test "Pooling from a single publisher in a round-robin fashion"
end
