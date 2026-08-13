defmodule AgentHarness.Exec do
  @moduledoc """
  Behaviour for running a command in some execution environment.

  Provider client layers describe *what* to run — an argv, environment
  entries, and a working directory — and an Exec implementation decides
  *where* it runs: a local OS process, a remote sandbox, an SSH channel.
  The contract is byte-level and protocol-free; framing and parsing belong
  to the caller.

  ## Spec

  `start/3` takes a spec map:

    * `:cmd` — argv list; the head is the executable name, resolved in the
      execution environment (its `PATH`, not the orchestrator's).
    * `:env` — map of environment entries merged over the execution
      environment's own environment. A value of `false` unsets the variable.
    * `:cwd` — working directory inside the execution environment, or `nil`.
    * `:stderr` (optional) — `:merged` interleaves stderr into the data
      stream; `:passthrough` (default) keeps it out of the stream and lets
      the implementation surface it elsewhere (locally: the VM's stderr).

  `cwd`, `env`, and executable resolution are all interpreted where the
  command runs. Nothing in the spec refers to the orchestrator's filesystem.

  ## Owner messages

  The implementation delivers messages to the `owner` pid passed to
  `start/3`:

    * `{:exec_data, handle, binary}` — output bytes in arrival order.
      Chunk boundaries carry no meaning; lines can arrive split.
    * `{:exec_exit, handle, reason}` — delivered exactly once, after which
      no further `:exec_data` for that handle arrives. `reason` is
      `{:exit_status, non_neg_integer()}` for a process exit, `:killed`
      after `kill/1`, or an implementation-specific term for transport
      failures.

  Implementations must monitor `owner` and release their resources if it
  dies.

  ## Commands

    * `write/2` sends bytes to stdin. After exit it returns
      `{:error, :closed}`. When the command has stopped draining its input
      and the implementation cannot accept more without blocking, it
      returns `{:error, :busy}` instead of suspending; the caller decides
      whether that fails the operation.
    * `kill/1` forcefully stops the command, including the underlying OS
      process or remote resource, not merely the byte stream. It is
      idempotent, returns `:ok` even when the handle is already gone, and
      guarantees the exactly-once `:exec_exit` delivery (with reason
      `:killed`) if no exit was delivered yet. In-band cancellation (agent
      protocols cancel over stdin) is the caller's job; `kill/1` is the
      force-stop fallback, and destroying a remote sandbox is a valid
      implementation.
  """

  @type handle :: term()

  @type spec :: %{
          required(:cmd) => [String.t(), ...],
          required(:env) => %{optional(String.t()) => String.t() | false},
          required(:cwd) => String.t() | nil,
          optional(:stderr) => :merged | :passthrough
        }

  @callback start(spec(), owner :: pid(), opts :: keyword()) ::
              {:ok, handle()} | {:error, term()}

  @callback write(handle(), iodata()) :: :ok | {:error, term()}

  @callback kill(handle()) :: :ok
end
