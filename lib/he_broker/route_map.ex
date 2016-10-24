defmodule HeBroker.RouteMap do
  @moduledoc """
  This module implements a method of mapping consumer services to their
  subscribed topics so publishers can properly broadcast messages

  Each route is composed by a topic (the string that will be used to define which
  _consumers_ you want to send your message to. eg: "user", "user:create", "email:send")
  and a collection of _services_. Those _services_ are the consumers of
  the messages sent to the specified topic. The services are composed oconsumerf a _cast_
  function (a function to be executed when the publisher wants to send a message
  to the topic without expecting a return), a _call_ function (a function to be
  executed when the publisher wants to send a message to the topic expecting one
  response) and a pool of processes that can consume those messages using the
  specified functions.

  The data representing the routemap should be considered opaque, any code that
  assumes any knowledge about the structure of the routemap is prone to fail since
  this might change in the future. **ALWAYS USE THE FUNCTIONS ON THIS MODULE TO
  PROPERLY USE THE ROUTEMAP**
  """

  alias HeBroker.RouteMap.Service

  @type expected_call_return :: :noreply | {:reply, any}
  @type expected_cast_return :: no_return
  @type callback_return :: expected_call_return | expected_cast_return

  @type cast_fun :: ((consumer :: pid, topic, message :: any, Request.t) -> expected_cast_return)
  @type call_fun :: ((consumer :: pid, topic, message :: any, Request.t) -> expected_call_return)
  @type partial :: ((topic, message :: any, Request.t) -> callback_return)

  @type topic :: String.t
  @type service :: Service.t
  @opaque t :: %{topic => [service]}

  @spec new() :: t
  @doc """
  Returns a new route map
  """
  def new,
    do: %{}

  @spec callback(Service.t, :call | :cast) :: partial
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

  @spec build_partial(cast_fun | call_fun, :queue.queue(pid)) :: partial
  @docp """
  Wraps `function` in a partial providing the head pid from `pool`, returning a
  3-fun callback
  """
  defp build_partial(function, pool) do
    pid = :queue.get(pool)

    fn topic, message, request ->
      function.(pid, topic, message, request)
    end
  end

  @spec services_on_topic(t, topic) :: {t, [service]}
  @doc """
  Returns the services on `topic` in `routemap`

  Does so by cycling the pool of consumer pids of each services and returning a
  2-tuple with both the updated routemap and a list of the services on the topic
  """
  def services_on_topic(routemap, topic) do
    case routemap do
      %{^topic => services} ->
        services = Enum.map(services, fn service ->
          {{:value, pid}, pool} = :queue.out(service.pool)

          %{service| pool: :queue.in(pid, pool)}
        end)

        {Map.put(routemap, topic, services), services}
      _ ->
        {routemap, []}
    end
  end

  @spec upsert_topic(t, topic, pid, cast_fun, call_fun) :: t
  @doc """
  Upserts the service on the `routemap` putting it on the specified `topic`
  using it's defined `cast` and `call` functions.

  If the topic does not exists in the routemap, it will be created.
  If the service is already on the topic in the routemap, `pid` will be added to
  it's pool
  """
  def upsert_topic(routemap, topic, pid, cast, call) do
    same_service? = fn
      %Service{cast: ^cast, call: ^call} ->
        true
      _ ->
        false
    end

    services =
      routemap
      |> Map.get(topic)
      |> List.wrap()
      |> Enum.reduce({[], false}, fn
        service, {acc, false} ->
          if same_service?.(service) do
            s = %Service{service| pool: :queue.in(pid, service.pool)}
            {[s| acc], true}
          else
            {[service| acc], false}
          end
        service, {acc, true} ->
          {[service| acc], true}
      end)
      |> case do
        {services, true} ->
          services
        {services, false} ->
          s = %Service{cast: cast, call: call, pool: :queue.cons(pid, :queue.new())}
          [s| services]
      end

    Map.put(routemap, topic, services)
  end

  @spec remove_consumer(t, topic, consumer :: pid) :: t
  @doc """
  Removes the specified `pid` from the topic.

  Might remove the whole `topic` from the `routemap` if `pid` is the only
  subscribed consumer
  """
  def remove_consumer(routemap, topic, pid) do
    case Map.fetch(routemap, topic) do
      {:ok, services} ->
        services
        |> Enum.reduce([], fn service = %Service{pool: pool}, acc ->
          cond do
            not :queue.member(pid, pool) ->
              [service| acc]
            pids2 = :queue.filter(&(&1 != pid), pool) ->
              if :queue.is_empty(pids2) do
                # Pid was the only element of the pool and the service now doesn't
                # have any enabled consumer and thus must be removed from the topic
                acc
              else
                # Pid is removed from the pool and the pool isn't empty
                [%{service| pool: pids2}| acc]
              end
          end
        end)
        |> case do
          [] ->
            # By removing the consumer, the topic doesn't has any subscribed
            # service consumer anymore
            Map.delete(routemap, topic)
          services ->
            Map.put(routemap, topic, services)
        end
      :error ->
        routemap
    end
  end
end