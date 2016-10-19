defmodule HeBroker.Consumer do

  defdelegate subscribe(broker, topics, callbacks),
    to: HeBroker.Broker
end