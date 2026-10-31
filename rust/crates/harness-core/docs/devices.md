# Attached device execution

The native `devices` module composes the ordinary Machines shell with an
authenticated Actor socket. It adds no process executor or transport protocol.
The device owner supplies the admitted `MachinesWorkspace`, current public
`DeviceAuthority`, original invocation clock and cancellation, and owner-side
approval policy. Human, model and parent approvals use `DeviceApprover`; an
agent-host approval never substitutes for an owner decision.

The host attaches the target with `attach` on its original Actor `Store`.
`TargetActor` and `TargetComposer` run under the same account's `targets` Actor
namespace, claimed with `Runtime::claiming`. `TargetComposer` receives the socket
host that admitted those connections. Other Actor namespaces keep their own
handlers. A device opens its original Stream Session Actor socket with
`DeviceRunner::attach`, then passes that socket to `DeviceRunner::serve`.

`TargetDirectory` implements the ordinary `TargetResolver`. Its one `follow`
observes presence; `for_agent` shares that subscription and roster while binding
another agent's original grant and home executor. Bind the resulting resolver
through `runtime::Bindings.targets`. There is no fallback when a named device is
offline. The device's Open and Close events publish its presence, including
Close(Runtime) after host recovery. An ended presence producer invalidates the
shared roster.

The resolver queues a Run in the same commit as the tool-journal claim and the
owning Actor's activation. The Run binds the original agent, tool operation,
call, project VolumeRef, device Actor, connection session and generation, exact
arguments and task deadline. Its sealed scope authenticates those facts without
delegation or signing custody. The device checks the selected volume against
its original workspace admission before executing. A child whose project fork
has not been admitted on that device receives a refusal; it cannot run in its
parent's view.

The Actor commits Started before handing over the Run. The device checks current
authority and owner policy, then pulls the existing Machine output stream.
Output is split into protective chunks and published incrementally to the
original operation stream. One pending report slot applies backpressure across
the connection. Tool observation materializes immutable chunk files through the
original task's checked content publisher and returns a bounded preview with the
actual output volume and directory. Reconciliation reuses each original stream
position as its file publication identity.

A terminal operation head prevents another handover of that operation. A
replayed queue sequence is also refused by the physical connection's high-water
mark. Ending a session publishes one immutable Lost marker covering all its
unfinished operations, without enumerating them. An Exit already committed
before Close remains recoverable. Losing a connection or current grant drops the
same Machine stream and reports uncertainty; it never redispatches the command
or certifies physical cleanup from expiry alone.

For archive, pass `directory.lifecycle(original_owner, existing_resources)` as
the `AgentLifecycle` in `AgentHost::composing`. Its close operation revokes the
exact agent's parent device grant before delegating resource destruction. The
original trust event stops active device work. Pause delegates resource
suspension while keeping the grant. The close participant repeats only the
original idempotent releases when its journal publication retries.

The real native integration control runs with:

```sh
cargo test -p acyclic-harness-core --features filesystem-local,native-execution,machines --test machine-workspace device_runner::committed_device_handover_runs_once_and_archive_fences_its_grant -- --exact
```

It retains the actual local Machine, authenticated Actor socket, committed
operation and original grant. A held child descriptor prevents exit until output
has crossed the socket and operation publication boundary.
