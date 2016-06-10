defmodule HeBroker.RouteMap do
  @moduledoc """
  This module implements a map of routes to be used with the `RPCModule.Publisher`
  module

  Each route is composed by a topic (the string that will be used to define which
  _consumers_ you want to send your message to. eg: "user", "user:create", "email:send")
  and a collection of _applications_. Those _applications_ are the consumers of
  the messages sent to the specified topic. The applications are composed of a _cast_
  function (a function to be executed when the publisher wants to send a message
  to the topic without expecting a return), a _call_ function (a function to be
  executed when the publisher wants to send a message to the topic expecting one
  response) and a queue of processes that can consume those messages using the
  specified functions.

  The data representing the routemap should be considered opaque, any code that
  assumes any knowledge about the structure of the routemap is prone to fail since
  this might change in the future. **ALWAYS USE THE FUNCTIONS ON THIS MODULE TO
  PROPERLY USE THE ROUTEMAP**
  """

  @type cast_fun :: ((consumer :: pid, topic :: String.t, message :: any) -> no_return)
  @type call_fun :: ((consumer :: pid, topic :: String.t, message :: any, timeout :: non_neg_integer | :infinity) -> :noreply | {:reply, any})
  @type topic :: String.t

  @opaque application :: {application :: atom, {cast_fun, call_fun, :queue.queue(pid)}}
  @opaque t :: %{topic :: String.t => [application]}

  @spec new() :: t
  @doc """
  Returns a new route map
  """
  def new,
    do: %{}

  @spec merge(t, t) :: t
  @doc """
  Merges two routemaps into one

  The _application collection_ of the `new_routemap` will overwrite the
  _application collection_ of `base_routemap` on topic conflicts
  """
  def merge(base_routemap, new_routemap),
    do: Map.merge(base_routemap, new_routemap)

  @spec topic_call(t, topic) :: {[{call_fun, pid}], t}
  @doc """
  Returns a list of _call functions_ with a PID to properly consume them and
  updates the queue of consumers related to these applications

  If the topic does not exists in the routemap or it has no suitable applications,
  a `{:error, :not_found}` tuple will be returned instead
  """
  def topic_call(routemap, topic) do
    case routemap do
      %{^topic => apps} ->
        return = {app_calls, queued_routes} = get_calls(apps)

        case return do
          {[], []} ->
            {:error, :not_found}
          _ ->
            {:ok, return}
        end
      _ ->
        {:error, :not_found}
    end
  end

  @spec topic_cast(t, topic) :: {[{cast_fun, pid}], t}
  @doc """
  Returns a list of _cast functions_ with a PID to properly consume them and
  updates the queue of consumers related to these applications

  If the topic does not exists in the routemap or it has no suitable applications,
  a `{:error, :not_found}` tuple will be returned instead
  """
  def topic_cast(routemap, topic) do
    case routemap do
      %{^topic => apps} ->
        return = {app_casts, queued_routes} = get_casts(apps)
        
        case return do
          {[], []} ->
            {:error, :not_found}
          _ ->
            {:ok, return}
        end
      _ ->
        {:error, :not_found}
    end
  end

  @spec upsert_topic(t, topic, atom, pid, cast_fun, call_fun) :: t
  @doc """
  Upserts the `application` on the `routemap` putting it on the specified `topic`
  using it's defined `cast` and `call` functions.

  If the topic does not exists in the routemap yet, it will be created
  If the application is already in the topic in the routemap, the application `pid`
  will be inserted in it's queue
  If the application is already in the topic in the routemap but with a different
  `cast` or `call` function, an error will be raised
  """
  # TODO: Use an specific error
  def upsert_topic(routemap, topic, application, pid, cast, call) do
    case routemap do
      %{^topic => apps} ->
        Map.put(routemap, topic, update_apps(apps, application, pid, cast, call))
      _ ->
        Map.put(routemap, topic, [{application, {cast, call, :queue.new() |> :queue.snoc(pid)}}])
    end
  end

  @spec remove_consumer(t, [{topic, app :: atom}], consumer :: pid) :: t
  @doc """
  Removes the specified `pid` from the topics where it consumes as the specified apps.

  If it is in the only consumer with the specified application name on the topic,
  the application is removed. If the removed application was the only application on
  the topic, the topic is removed from the routemap
  """
  def remove_consumer(routemap, [], _),
    do: routemap
  def remove_consumer(routemap, [{topic, app}|t], pid) do
    routemap
    |> remove_from_topic(topic, app, pid)
    |> remove_consumer(t, pid)
  end

  @spec remove_from_topic(t, topic, atom, pid) :: t
  @doc """
  Removes the specified `pid` from the `application` in the `topic` on the `routemap`
  """
  def remove_from_topic(routemap, topic, application, pid) do
    case routemap do
      %{^topic => apps} ->
        case remove_consumer_from_topic(apps, application, pid) do
          [] ->
            Map.delete(routemap, topic)
          apps ->
            Map.put(routemap, topic, apps)
        end
      _ ->
        routemap
    end
  end

  @spec get_calls([application]) :: {[{call_fun, pid}], [application]}
  defp get_calls(apps, acc \\ {[], []})
  defp get_calls([], acc),
    do: acc
  defp get_calls([{a, {cast, call, queue}}|t], {r, c}) do
    {{:value, pid}, q2} = :queue.out(queue)
    get_calls(t, {[{call, pid}|c], [{a, {cast, call, :queue.in(pid, q2)}}|r]})
  end

  @spec get_casts([application]) :: {[{cast_fun, pid}], [application]}
  defp get_casts(apps, acc \\ {[], []})
  defp get_casts([], acc),
    do: acc
  defp get_casts([{a, {cast, call, queue}}|t], {r, c}) do
    {{:value, pid}, q2} = :queue.out(queue)
    get_casts(t, {[{cast, pid}|c], [{a, {cast, call, :queue.in(pid, q2)}}|r]})
  end

  @spec update_apps([application], atom, pid, cast_fun, call_fun, [application]) :: [application]
  defp update_apps(apps, app, pid, cast, call, acc \\ [])
  defp update_apps([], app, pid, cast, call, acc),
    do: [{app, {cast, call, :queue.new() |> :queue.snoc(pid)}}|acc]
  defp update_apps([{app, {cast, call, queue}}|t], app, pid, cast, call, acc),
    do: [{app, {cast, call, :queue.snoc(queue, pid)}}|t] ++ acc
  defp update_apps([{app, _}|_], app, _, _, _, _),
    do: raise RuntimeError, msg: "Incompatible functions for consuming on HeBroker for consumer #{inspect app}"
  defp update_apps([h|t], app, pid, cast, call, acc),
    do: update_apps(t, app, pid, cast, call, [h|acc])

  @spec remove_consumer_from_topic([application], atom, pid, [application]) :: [application]
  defp remove_consumer_from_topic(apps, app, pid, acc \\ [])
  defp remove_consumer_from_topic([], _, _, acc),
    do: acc
  defp remove_consumer_from_topic([{app, {cast, call, queue}}|t], app, pid, acc) do
    q2 = :queue.filter(queue, fn item -> item !== pid end)
    if :queue.empty?(q2),
      do: remove_consumer_from_topic(t, app, pid, acc),
      else: remove_consumer_from_topic(t, app, pid, [{app, {cast, call, q2}}|acc])
  end
  defp remove_consumer_from_topic([h|t], app, pid, acc),
    do: remove_consumer_from_topic(t, app, pid, [h|acc])
end