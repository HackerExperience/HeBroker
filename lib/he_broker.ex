defmodule HeBroker do

  alias HeBroker.Broker
  alias HeBroker.Publisher
  alias HeBroker.Consumer

  @default_timeout 15_000

  defdelegate start_link(name),
    to: Broker

  defdelegate count_services_on_topic(broker, topic),
    to: Broker

  defdelegate subscribed?(broker, consumer_pid),
    to: Broker

  defdelegate subscribed?(broker, consumer_pid, topic),
    to: Broker

  defdelegate subscribe(broker, topic, callbacks),
    to: Consumer

  def cast(broker, topic, message, params \\ []),
    do: Publisher.cast(broker, topic, message, params)

  def call(broker, topic, message, params \\ []),
    do: Publisher.call(broker, topic, message, params)

  def async(broker, topic, message, params \\ []),
    do: Publisher.async(broker, topic, message, params)

  def yield(request, timeout \\ @default_timeout),
    do: Publisher.yield(request, timeout)

  def await(request, timeout \\ @default_timeout),
    do: Publisher.await(request, timeout)

  def ignore(request),
    do: Publisher.ignore(request)
end