defmodule HeBroker.Pry.MessageSent do

  defstruct [:broker, :topic, :message, :method, :request, :publisher, :consumer_count]
end