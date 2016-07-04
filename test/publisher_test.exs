defmodule HeBroker.PublisherTest do
  use ExUnit.Case

  setup_all do
    {:ok, _} = HeBroker.start_link
    publisher = HeBroker.Publisher.start_link
    {:ok, publisher: publisher}
  end

  test "Subscribe from a topic", %{publisher: publisher} do
    assert false == HeBroker.Publisher.lookup(publisher, "foo:bar")

    HeBroker.Publisher.subscribe(publisher, ["foo:bar"])
    assert true == HeBroker.Publisher.lookup(publisher, "foo:bar")
  end

  test "Subscribe from a topic on-the-run"

  test "Be notified of consumer shutdown on subscribed topic"

  test "Be notified of consumer startup on subscribed topic"

  test "Execute all cast functions on a topic"

  test "Execute all call functions on a topic"
end
