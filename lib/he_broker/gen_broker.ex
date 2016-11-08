defmodule HeBroker.GenBroker do

  defmacro __using__(_) do
    quote do

      @broker __MODULE__

      def start_link,
        do: HeBroker.start_link(@broker)

      def start_link(params),
        do: HeBroker.start_link(@broker, params)

      def count_services_on_topic(topic),
        do: HeBroker.count_services_on_topic(@broker, topic)

      def subscribed?(consumer_pid),
        do: HeBroker.subscribed?(@broker, consumer_pid)

      def subscribed?(consumer_pid, topic),
        do: HeBroker.subscribed?(@broker, consumer_pid, topic)

      def subscribe(topic, callbacks),
        do: HeBroker.subscribe(@broker, topic, callbacks)

      def cast(topic, message),
        do: HeBroker.cast(@broker, topic, message)
      def cast(topic, message, params),
        do: HeBroker.cast(@broker, topic, message, params)

      def call(topic, message),
        do: HeBroker.call(@broker, topic, message)
      def call(topic, message, params),
        do: HeBroker.call(@broker, topic, message, params)

      def async(topic, message),
        do: HeBroker.async(@broker, topic, message)
      def async(topic, message, params),
        do: HeBroker.async(@broker, topic, message, params)

      # The following functions does not depends on the broker and are aliased
      # on the module as a simple utility
      defdelegate yield(request),
        to: HeBroker
      defdelegate yield(request, timeout),
        to: HeBroker
      defdelegate await(request),
        to: HeBroker
      defdelegate await(request, timeout),
        to: HeBroker
      defdelegate ignore(request),
        to: HeBroker
    end
  end
end