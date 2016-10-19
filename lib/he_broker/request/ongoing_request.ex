defmodule HeBroker.Request.OngoingRequest do

  alias HeBroker.Request.Reply

  @opaque t :: %__MODULE__{}

  defstruct [:task, :request]

  @doc false
  def start(callbacks, message, request) do
    task = Task.async(__MODULE__, :main_task, [callbacks, message, request])

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
  def main_task(callbacks, message, request) do
    me = self()

    Enum.each(callbacks, fn callback ->
      spawn fn ->
        sub_task(me, callback, message, request)
      end
    end)

    wait_loop()
  end

  defp sub_task(main_task, callback, message, request) do
    case callback.(message, request) do
      :noreply ->
        :ok
      {:reply, reply} ->
        send main_task, {:"$hebroker", :reply, reply, request}
    end
  end

  defp wait_loop do
    receive do
      {:"$hebroker", :reply, reply, request} ->
        Reply.new(request, reply)
    end
  end
end