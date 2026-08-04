# Driving AgentHarness from a GenServer

An orchestrator should treat AgentHarness as an asynchronous state machine.
Use subscriptions for turn progress, a native process monitor for session
liveness, and supervised tasks for the few public calls that intentionally
wait on external work.

Do not call `await/2`, enumerate `stream/2`, or synchronously open a CLI from a
GenServer callback. `await/2` and `stream/2` default to `:infinity`, while
session opening has a finite default but still waits for an external runtime.
Any of those waits blocks every other message handled by that GenServer.

## Supervision setup

Give orchestration calls their own task supervisor:

```elixir
children = [
  {Task.Supervisor, name: MyApp.AgentTasks},
  MyApp.AgentCoordinator
]

Supervisor.start_link(children, strategy: :one_for_one)
```

AgentHarness supervises sessions and provider runtimes. `MyApp.AgentTasks`
keeps external waits out of the coordinator mailbox; it is not a pool of
fungible sessions.

## A non-blocking consumer

The following worker starts the CLI in a task, then owns the session monitor
and event subscription itself:

```elixir
defmodule MyApp.AgentWorker do
  use GenServer

  alias AgentHarness.{Event, Request, Response}

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    owner = Keyword.fetch!(opts, :owner)
    prompt = Keyword.fetch!(opts, :prompt)
    provider = Keyword.fetch!(opts, :provider)
    session_opts = Keyword.get(opts, :session_options, [])

    task =
      Task.Supervisor.async(MyApp.AgentTasks, fn ->
        AgentHarness.start_session(provider, session_opts)
      end)

    {:ok,
     %{
       owner: owner,
       prompt: prompt,
       startup_task: task,
       work_task: nil,
       session: nil,
       session_monitor: nil,
       turn: nil,
       subscription: nil
     }}
  end

  @impl true
  def handle_info({ref, {:ok, session}}, %{startup_task: %Task{ref: ref} = task} = state) do
    Process.demonitor(ref, [:flush])
    Process.unlink(task.pid)

    case AgentHarness.monitor(session) do
      {:ok, session_monitor} ->
        worker = self()

        work_task =
          Task.Supervisor.async(MyApp.AgentTasks, fn ->
            start_work(session, state.prompt, worker)
          end)

        {:noreply,
         %{
           state
           | startup_task: nil,
             work_task: work_task,
             session: session,
             session_monitor: session_monitor
         }}

      error ->
        send(state.owner, {:agent_start_failed, session, nil, error})
        {:stop, :normal, %{state | startup_task: nil, session: session}}
    end
  end

  def handle_info({ref, {:error, reason}}, %{startup_task: %Task{ref: ref} = task} = state) do
    Process.demonitor(ref, [:flush])
    Process.unlink(task.pid)
    send(state.owner, {:agent_start_failed, nil, nil, reason})
    {:stop, :normal, %{state | startup_task: nil}}
  end

  def handle_info(
        {:DOWN, ref, :process, _task_pid, reason},
        %{startup_task: %Task{ref: ref}} = state
      ) do
    send(state.owner, {:agent_start_failed, nil, nil, {:startup_task_down, reason}})
    {:stop, :normal, %{state | startup_task: nil}}
  end

  def handle_info(
        {ref, {:ok, turn, subscription}},
        %{work_task: %Task{ref: ref} = task} = state
      ) do
    Process.demonitor(ref, [:flush])
    Process.unlink(task.pid)

    {:noreply,
     %{state | work_task: nil, turn: turn, subscription: subscription}}
  end

  def handle_info(
        {ref, {:error, turn, reason}},
        %{work_task: %Task{ref: ref} = task} = state
      ) do
    Process.demonitor(ref, [:flush])
    Process.unlink(task.pid)
    send(state.owner, {:agent_start_failed, state.session, turn, reason})
    {:stop, :normal, %{state | work_task: nil, turn: turn}}
  end

  def handle_info(
        {:DOWN, ref, :process, _task_pid, reason},
        %{work_task: %Task{ref: ref}} = state
      ) do
    send(
      state.owner,
      {:agent_start_failed, state.session, state.turn, {:work_task_down, reason}}
    )
    {:stop, :normal, %{state | work_task: nil}}
  end

  # Linked Tasks emit both EXIT and DOWN. Keep DOWN authoritative so a failed
  # task is reported exactly once and its matching state is still available.
  def handle_info({:EXIT, task_pid, _reason}, %{startup_task: %Task{pid: task_pid}} = state),
    do: {:noreply, state}

  def handle_info({:EXIT, task_pid, _reason}, %{work_task: %Task{pid: task_pid}} = state),
    do: {:noreply, state}

  def handle_info({:EXIT, _task_pid, :normal}, state), do: {:noreply, state}
  def handle_info({:EXIT, _parent, :shutdown}, state), do: {:stop, :shutdown, state}
  def handle_info({:EXIT, _pid, reason}, state), do: {:stop, reason, state}

  def handle_info(
        {:agent_harness, ref, %Event{type: :request_created, data: request}},
        %{subscription: %{ref: ref}} = state
      ) do
    response = response_for(request)
    owner = state.owner

    task_result =
      Task.Supervisor.start_child(MyApp.AgentTasks, fn ->
        result = AgentHarness.respond(request, response)
        send(owner, {:agent_response_result, request.id, result})
      end)

    if match?({:error, _reason}, task_result) do
      send(owner, {:agent_response_result, request.id, task_result})
    end

    {:noreply, state}
  end

  def handle_info(
        {:agent_harness, ref, %Event{type: type} = event},
        %{subscription: %{ref: ref}} = state
      )
      when type in [
             :turn_completed,
             :turn_failed,
             :turn_cancelled,
             :turn_interrupted
           ] do
    send(state.owner, {:agent_terminal, state.session, state.turn, event})

    Task.Supervisor.start_child(MyApp.AgentTasks, fn ->
      AgentHarness.unsubscribe(state.subscription)
    end)

    {:noreply, %{state | subscription: nil}}
  end

  def handle_info(
        {:agent_harness, ref, %Event{} = event},
        %{subscription: %{ref: ref}} = state
      ) do
    send(state.owner, {:agent_event, state.turn, event})
    {:noreply, state}
  end

  def handle_info(
        {:DOWN, ref, :process, _server, reason},
        %{session_monitor: ref} = state
      ) do
    send(state.owner, {:agent_session_down, state.session, reason})
    {:stop, :normal, %{state | session_monitor: nil}}
  end

  # A demonitor/unlink race may leave the second half of a Task notification.
  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state), do: {:noreply, state}

  defp response_for(%Request{kind: :question}) do
    # Replace this with a user/UI round trip when policy cannot decide.
    Response.answer("Postgres")
  end

  defp response_for(%Request{}) do
    Response.deny("No unattended approval policy matched")
  end

  defp start_work(session, prompt, subscriber) do
    with {:ok, turn} <- AgentHarness.start_turn(session, prompt) do
      case AgentHarness.subscribe(turn, pid: subscriber, from: :start) do
        {:ok, subscription} -> {:ok, turn, subscription}
        error -> {:error, turn, error}
      end
    else
      {:error, {:session_call_timeout, turn}} = error -> {:error, turn, error}
      error -> {:error, nil, error}
    end
  end
end
```

This example is a one-turn event consumer. After the terminal event it removes
the subscription but remains alive to own the session monitor. The owner
receives any accepted turn handle along with the session and error, or receives
both handles with the terminal event. It must deliberately reuse the session,
stop it, or transfer monitor ownership before stopping this worker. That
separation prevents an event consumer crash from silently closing a coding
session.

The startup task is linked to the worker, which traps exits while distinguishing
the task from its supervisor. This matters because AgentHarness treats the
process calling `start_session/2` as the startup owner until readiness is
acknowledged in both directions. If the worker dies, its linked startup task
dies too and the partially opened session is cleaned up instead of becoming an
idle orphan. A pending attempt marker also makes a Store commit reclaimable
after an untrappable SessionServer kill in that handoff window.

The important ownership detail is that `AgentHarness.monitor/1` is called by
the GenServer. BEAM monitors notify the process that creates them. The worker
then delegates `start_turn/3` and replay subscription to another task, passing
`pid: worker` so events and the SessionServer's subscriber monitor still belong
to the worker. This keeps provider admission and custom Store reads/writes out
of callbacks without moving lifecycle ownership to the task.

An uncertain admission failure is followed by SessionServer termination. Since
the worker keeps its session monitor after the terminal event, it reports the
subsequent `:DOWN` as a separate lifecycle signal. If the owner takes over the
session instead, it should call `AgentHarness.monitor/1` itself before asking
the worker to exit; either `{:error, :session_not_found}` or a later `:DOWN`
then triggers reconciliation.

The subscription uses `from: :start`, so a fast provider cannot finish in the
gap between `start_turn/3` and `subscribe/2`. Replay is installed before live
delivery, preserving event order. `start_turn/3` itself returns after local
acceptance with a stable `:starting` turn; provider admission finishes
asynchronously.

The response call is also placed in a supervised task. Request ownership is
exactly once in the SessionServer, but the provider call can still take time.
Track `{:agent_response_result, request_id, result}` so rejected, expired, or
uncertain responses are visible to the orchestrator.

An in-flight duplicate response returns `:response_in_progress`; a later one
returns `:already_resolved`. A definite provider rejection leaves the request
pending, while `{:provider_command_uncertain, reason}` is followed by terminal
turn events and session `:DOWN`. Reconcile before retrying an uncertain answer.

## Questions that require a human

Do not keep the GenServer callback waiting while a person decides. Store the
`%Request{}` by its ID, notify the UI or parent process, and return
`{:noreply, state}`. Later, a cast or info message can launch the same
asynchronous `AgentHarness.respond/2` operation.

Pending requests expire when their turn ends. Treat `:request_expired` as the
signal to remove the request from UI state. A late response then returns an
error instead of being forwarded twice.

## Completion and failure signals

These signals mean different things:

- `:turn_completed`, `:turn_failed`, `:turn_cancelled`, and
  `:turn_interrupted` are authoritative turn outcomes.
- `{:DOWN, session_monitor, :process, server, reason}` means the session process
  disappeared. It does not prove whether already-started tools or filesystem
  changes ran.
- `:transport_error` says the provider runtime was lost; an active turn is also
  failed and the session becomes unavailable.
- A timeout from a calling task is not evidence that a coding action did not
  start. Reconcile with the stable IDs, events, Store, and workspace before
  retrying.

`AgentHarness.stop_session/2` and `unsubscribe/1` are idempotent. Live-session
operations on a stopped handle return `{:error, :session_not_found}`;
`purge_session/2` intentionally operates only after the session is no longer
live.

## Restart reconciliation

Stable IDs let a coordinator process that restarts inside a still-running
application compare live ownership with history retained by the Store:

```elixir
live_by_id = Map.new(AgentHarness.list_sessions(), &{&1.id, &1})
{:ok, stored} = AgentHarness.list_stored_sessions()

stale =
  Enum.filter(stored, fn entry ->
    not entry.live? and entry.snapshot.status not in [:closed]
  end)
```

The default Memory Store survives an individual coordinator restart, but it is
lost when the AgentHarness application or VM stops. Reconciliation across a
redeploy, application restart, or machine failure requires a durable custom
Store. Pass the same Store owner used by the sessions when reading inventory:

```elixir
store = {MyApp.AgentStore, MyApp.AgentStore}

{:ok, stored} = AgentHarness.list_stored_sessions(store: store)
```

Use that same `store` option when starting, replacing, or purging those session
aggregates.

For each stale aggregate, inspect its last event, provider session ID, pending
request records, and workspace before deciding what happened. AgentHarness
does not automatically retry a turn whose side effects are uncertain.

The explicit recovery choices are:

- keep the aggregate for audit and start a differently named session;
- resume the provider conversation using its saved provider session/thread ID;
- `purge_session/2` after determining the history is no longer needed;
- start with `reuse: :closed` for a cleanly closed ID;
- start with `reuse: :replace` to destructively replace a stale non-live ID.

Both reuse modes preserve the old aggregate until the replacement provider has
opened successfully, then begin a fresh core journal. They do not reconstruct
the old SessionServer state.

Use deterministic session IDs derived from work ownership when that helps a
coordinator find duplicates after restart. Do not start the same deterministic
ID independently on multiple BEAM nodes; v0.x inventory and Registry ownership
are node-local.
