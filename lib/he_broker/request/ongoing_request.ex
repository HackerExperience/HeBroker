defmodule HeBroker.Request.OngoingRequest do

  alias HeBroker.Publisher
  alias HeBroker.Request.Reply

  @opaque t :: %__MODULE__{}

  defstruct [:task, :request]

  @doc false
  def start(callbacks, message, request, topic) do
    task = Task.async fn ->
      handler(callbacks, message, request, topic)
    end

    %__MODULE__{task: task, request: request}
  end

  @doc false
  def await(ongoing_request, timeout) do
    case yield(ongoing_request, timeout) do
      nil ->
        raise HeBroker.Request.TimeoutError, request: ongoing_request.request
      {:ok, return} ->
        return
    end
  end

  @doc false
  def yield(ongoing_request, timeout),
    do: Task.yield(ongoing_request.task, timeout)

  @doc false
  def shutdown(ongoing_request),
    do: Task.shutdown(ongoing_request.task, :brutal_kill)

  @doc false
  def handler(callbacks, message, request, topic) do
    me = self()

    Enum.each(callbacks, fn callback ->
      spawn fn ->
        worker(me, callback, message, request, topic)
      end
    end)

    receive do
      {:"$hebroker", :reply, reply, request} ->
        Reply.new(request, reply)
    end
  end

  defp worker(reply_to, partial, message, request, topic) do
    {dest_req, callback} = Publisher.prepare_callback(partial, topic, message, request)

    case callback.() do
      :noreply ->
        :ok
      {:reply, reply} ->
        send reply_to, {:"$hebroker", :reply, reply, dest_req}
    end
  end
end