# acyclic-harness-machines

Explicit bridge from Harness sandbox and checkpoint references to a replaceable `MachinesProvider`.

```sh
cargo add acyclic-harness-machines
```

`MachinesHost` implements Harness's provider-owned fork capture and admission verifier. Bind it to `FilesystemForkPreparer::with_capture_provider` to capture a selected, forkable process checkpoint; the parent then publishes the exact retained reference. Admission rechecks the checkpoint with the same Machines provider and refuses missing or non-forkable revisions. No active process is implicitly cloned: a child's sandbox is a separate, admitted Machines operation. The adapter preserves provider identities and operation recovery; it does not upgrade a simulator into a hosted isolation boundary. Select a Machines provider whose stated assurance matches your deployment. See the [adapter API](https://docs.rs/acyclic-harness-machines/latest/acyclic_harness_machines/), [Machines guide](https://docs.rs/acyclic-machines/latest/acyclic_machines/), and [Harness guide](https://docs.rs/acyclic-harness/latest/acyclic_harness/).

`MachinesExecution` is the explicit task-placement adapter. Register each exact `MachinesTaskBuild` before binding it through `HarnessBuilder::execution` or `Bindings::execution`. Its spawner and state owner must advertise the same immutable execution identity, and a `MachinesExecutionAccess` implementation must attest input transfer, mounts, and scoped credential bindings without returning secret bytes. Qualification checks the registered task/machine identities, immutable image, running sandbox, compatibility revision, and access attestation before durable admission. The selected build, environment, and readiness commitment are retained in the task or batch admission; retry and recovery use that retained route instead of silently selecting another machine. Machines itself provides environments and checkpoints, not a task RPC or second agent loop: the supplied spawner/state pair must implement actual dispatch, observation, cancellation, and reconciliation for the registered build. A local closure, path, socket, or credential is never presumed usable remotely.
