defmodule HeBroker.Consumer do

  alias HeBroker.RouteMap

  @spec subscribe(app :: atom, topic :: String.t, cast: RouteMap.cast_fun, call: RouteMap.call_fun) :: no_return
  def subscribe(app, topic, fun) do
    HeBroker.consumer_subscribe(app, topic, fun)
  end
end