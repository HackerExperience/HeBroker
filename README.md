# HeBroker

HeBroker is a broker for RPC through a PubSub interface.

By providing a simple interface with very simple and extendible functionality, it
can easily be used as a broker for Events, a broker for Message Queues or even a
simple broker for named applications on a cluster (ie: RPC). You can find a
handful of examples on the `/examples` folder

HeBroker works by instanciating a (usualy) named broker that holds a map of
topics and handlers for each topic. Any process can subscribe to any topic by
passing callbacks to consume messages sent to the very topic.

```
  # Consumption
  {Consumer 1} "Subscribe me to topic 'test'" -> { Broker }
  {Consumer 2} "Subscribe me to topic 'test'" -> { Broker }


  # Publishing
  {Publisher} "I want to send message to topic 'test'"" -> { Broker }
  {Publisher} <- "Take this <callback>, just use it with your message as argument" { Broker }
  {Publisher} ! *Apply <callback>*
  <callback> ~~~> *Send message to {Consumer 1}*
  <callback> ~~~> *Send message to {Consumer 2}*

  # Monitoring
  {Consumer 3} "Subscribe me to topic 'test'" -> { Broker }
  {Consumer 3} ! *Dies*
  { Broker } ! *Removes {Consumer 3} from topic 'test'*
```

## Installation
HeBroker requires Elixir v1.3

1. Add he_broker to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [{:hebroker, git: "https://dev.hackerexperience.com/diffusion/BROKER/HEBroker.git"}]
end
```

2a. Start as many different brokers as you need on your application startup

```elixir
def start(_type, _args) do
  children = [
    worker(HeBroker, [:service_broker]),
    worker(HeBroker, [:event_broker]),
    worker(HeBroker, [:general_broker]),
    worker(MyApplication.SomeService, [])]

  Supervisor.start_link(
    children,
    strategy: :one_for_one,
    name: MyApplication.Supervisor)
end
```

2b. Alternatively, if you need a single broker for your application,
`HeBroker.UniversalBroker` is provided as a facade

```elixir
def start(_type, _args) do
  children = [
    worker(HeBroker.UniversalBroker, []),
    worker(MyApplication.SomeService, [])]

  Supervisor.start_link(
    children,
    strategy: :one_for_one,
    name: MyApplication.Supervisor)
end
```

2c. As another alternative, you can use the `use` macro to create modules that
represent instances of broker

```elixir
defmodule MyApp.ServiceBroker do
  use HeBroker.GenBroker
end
defmodule MyApp.EventBroker do
  use HeBroker.GenBroker
end
defmodule MyApp.GeneralBroker do
  use HeBroker.GenBroker
end

defmodule MyApp do
  use Application

  def start(_type, _args) do
    children = [
      worker(MyApp.ServiceBroker, []),
      worker(MyApp.EventBroker, []),
      worker(MyApp.GeneralBroker, []),
      worker(MyApp.SomeService, [])]

    Supervisor.start_link(
      children,
      strategy: :one_for_one,
      name: MyApp.Supervisor)
  end
end
```

## Todos
- [ ] Use `:pg2` to allow brokering through the cluster
- [ ] Use `:mnesia` to distribute the topics between nodes (working like sets on an union)
- [ ] Use `GenStage` for the consumption flow
- [x] Use (D)`ETS` to store the routes
- [ ] Table heir to recover if broker breaks(?)
- [ ] Start a process for each and every topic and have the broker simply manage (and refer to) them
- [ ] Consumer unsubscribe
- [ ] Pool of consumers dynamically instanciated via `:poolboy`
- [ ] Rename `service` and `server` to `consumer`
- [ ] Rename `client` and `producer` to `publisher`
- [ ] License
- [ ] Proper documentation (with examples)
- [ ] Move the prying flow to a different library to reduce cognitive load on reading the source
- [ ] Consider if Erlang's Trace Tool Builder is of any use for `HeBroker.Pry`
