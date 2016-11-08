defmodule HeBroker.RouteMap do
  @moduledoc """
  This module implements a method of mapping consumer services to their
  subscribed topics so publishers can properly broadcast messages

  Each route is composed by a topic (the string that will be used to define which
  _services_ you want to send your message to. eg: "user", "user:create", "email:send")
  and a collection of _services_. Those _services_ are the consumers of
  the messages sent to the specified topic. The services are composed by a _cast_
  function (a function to be executed when the publisher wants to send a message
  to the topic without expecting a return), a _call_ function (a function to be
  executed when the publisher wants to send a message to the topic expecting one
  response) and a pool of processes that can consume those messages using the
  specified functions.

  The data representing the routemap should be considered opaque, any code that
  assumes any knowledge about the structure of the routemap is prone to fail since
  this might change in the future.
  """

  alias HeBroker.RouteMap.Service
  alias HeBroker.Request

  @type partial :: ((topic, message :: any, Request.t) -> Service.call_return | Service.cast_return)

  @type topic :: String.t
  @type service :: Service.t
  @opaque t :: :ets.tid

  @spec new() :: t
  @doc """
  Returns a new route map
  """
  def new(params \\ []) do
    opts = Enum.reject(params, &match?({:name, _}, &1))
    :ets.new(params[:name] || :hebroker, opts)
  end

  @spec callback(Service.t, :call | :cast) :: partial | nil
  @doc """
  Returns a partial based on the service's callback.

  If the service doesn't provide a proper callback, `nil` is returned
  """
  def callback(%Service{call: nil}, :call),
    do: nil
  def callback(%Service{cast: nil}, :cast),
    do: nil
  def callback(%Service{cast: cast, pool: pool}, :cast),
    do: build_partial(cast, pool)
  def callback(%Service{call: call, pool: pool}, :call),
    do: build_partial(call, pool)

  @spec build_partial(Service.cast | Service.call, Service.pool) :: partial
  @docp """
  Wraps `function` in a partial providing the head pid from `pool`, returning a
  3-fun callback
  """
  defp build_partial(function, pool) do
    pid = Service.pool_out(pool)

    fn topic, message, request ->
      function.(pid, topic, message, request)
    end
  end

  @spec services_on_topic(t, topic) :: [service]
  @doc """
  Returns the services on `topic` on `routemap`
  """
  def services_on_topic(routemap, topic) do
    case :ets.lookup(routemap, topic) do
      [{^topic, services}] ->
        services
      [] ->
        []
    end
  end

  @spec upsert_topic(t, topic, pid, Service.cast, Service.call) :: no_return
  @doc """
  Upserts the service on the `routemap` putting it on the specified `topic`
  using it's defined `cast` and `call` functions.

  If the topic does not exists in the routemap, it will be created.
  If the service is already on the topic in the routemap, `pid` will be added to
  it's pool
  """
  def upsert_topic(routemap, topic, pid, cast, call) do
    services = case :ets.lookup(routemap, topic) do
      [{^topic, services}] ->
        services
      [] ->
        []
    end

    updated_services =
      Enum.reduce(services, {[], false}, fn
        service = %Service{cast: ^cast, call: ^call}, {acc, false} ->
          s = %Service{service| pool: Service.pool_in(service.pool, pid)}
          {[s| acc], true}
        s, {acc, status} ->
          {[s| acc], status}
      end)
      |> case do
        {services, true} ->
          services
        {services, false} ->
          s = %Service{cast: cast, call: call, pool: Service.pool_new(pid)}
          [s| services]
      end

    :ets.insert(routemap, {topic, updated_services})
  end

  @spec remove_consumer(t, topic, consumer :: pid) :: no_return
  @doc """
  Removes the specified `pid` from the topic.

  Might remove the whole `topic` from the `routemap` if `pid` is the only
  subscribed consumer
  """
  def remove_consumer(routemap, topic, pid) do
    case :ets.lookup(routemap, topic) do
      [{^topic, services}] ->
        services
        |> Enum.reduce([], fn service = %Service{pool: pool}, acc ->
          if Service.pool_member?(pool, pid) do
            pids2 = Service.pool_delete(pool, pid)

            if Service.pool_empty?(pids2) do
              # Pid was the only element of the pool and the service now doesn't
              # have any enabled consumer and thus must be removed from the topic
              acc
            else
              # Pid is removed from the pool and the pool isn't empty
              [%{service| pool: pids2}| acc]
            end
          else
            [service| acc]
          end
        end)
        |> case do
          [] ->
            # By removing the consumer, the topic doesn't has any subscribed
            # service consumer anymore
            :ets.delete(routemap, topic)
          services ->
            :ets.insert(routemap, {topic, services})
        end
      [] ->
        :ok
    end
  end
end