# acyclic-machines-daytona

`MachinesProvider` over [Daytona](https://www.daytona.io) sandboxes: VM classes (`linux-vm`,
`windows`) with native memory-and-disk fork, and `container` sandboxes without an admitted atomic snapshot boundary.
It is one replaceable provider behind the `acyclic-machines` contract: nothing Daytona-specific
crosses into the harness, and switching providers changes bindings, not orchestration.

| Trait method            | Daytona REST call (`https://app.daytona.io/api`)                            |
|-------------------------|-----------------------------------------------------------------------------|
| `qualify_image`         | digest → registered snapshot; `GET /snapshots/{name}` must report a VM class |
| `create`                | `POST /sandbox` (`CreateSandbox`), named `acyclic-{key}` so replays collide  |
| `checkpoint`            | `POST /sandbox/{id}/snapshot` with `includeMemory: true` (hot snapshot)       |
| `fork`                  | `POST /sandbox` from the hot snapshot, once per child                         |
| `fork_machine` (VM)     | `POST /sandbox/{id}/fork` per child: memory and disk (`LiveFork`)             |
| `fork_machine` (container) | refused: no admitted atomic snapshot boundary |
| `suspend` / `wake`      | `POST /sandbox/{id}/pause` / `POST /sandbox/{id}/start`                       |
| `set_suspension_policy` | `POST /sandbox/{id}/autopause/{minutes}` (pause keeps memory; stop does not)  |
| `destroy_*`             | `DELETE /sandbox/{id}` / `DELETE /snapshots/{id}`                             |
| `events`                | poll `GET /sandbox/{id}` and diff state                                       |
| `usage`                 | allocation estimate, marked provisional (Daytona has no per-sandbox meter)    |
| `recover`               | canonical Streams settlement; labels never prove completion                |

Live fork goes through the trait (`MachinesProvider::fork_machine`); callers read the fidelity
from the machine contract or the outcome, never from the provider type:

- **VM classes** declare `LiveFork`. Daytona's native fork copies the running sandbox's memory
  and disk into each child without a checkpoint (`ForkFidelity::MemoryAndDisk`). Children
  share the parent's environment and credentials. A VM runs one fork at a time (it is
  `forking` for several seconds and refuses another fork with 400 until `started`), so the
  shared core fans out as a doubling tree: every node that exists forks once per round (parent;
  then parent and child 0; then parent and children 0-2; ...), so 15 children take four rounds
  and 16 take five. Each fork waits for its source to be started and retries the state refusal
  with backoff in that core. Children forked from earlier children inherit what those executed since their
  own fork, so the set is memory-identical only if the workload holds still during the fork.
  Daytona refuses to delete a sandbox with live fork children, so `destroy_machine` checks
  `GET /sandbox/{id}/forks` first and returns `ProviderError::Conflict` until the children are
  gone; destroy children in reverse index order (a source always has a lower index than the
  children forked from it), then the parent.
- **Containers** declare no fork capability. A pair of matching TAR reads cannot
  prove one atomic filesystem generation. Container checkpoint/fork requests are
  refused until a provider snapshot or canonical immutable filesystem generation
  covers every writer. No archive capture or restore-copy path remains.

Every live-fork child carries `acyclic.kind=live-fork`, `acyclic.parent=<requested machine>`,
`acyclic.fork_source=<sandbox it was forked from>`, its index and the requested count, and
`acyclic.ready=true` once complete. These labels support native ownership checks;
only a durable Streams settlement proves success after restart. An admitted but
unsettled operation remains indeterminate and never authorizes redispatch. Containers cannot pause, checkpoint memory, or
auto-suspend: their contracts carry `SuspensionPolicy::Manual`, and `suspend`, `wake`,
`set_suspension_policy`, and `checkpoint` return `ProviderError::Unsupported`.

The sandbox class comes from the snapshot, never from the create request. Other classes are
rejected.

Safety rules the provider enforces:

- **Network commitment.** A request's `network_policy_digest` must be registered with
  `DaytonaConfig::register_network_policy` (`BlockAll` or `AllowDomains`); the sandbox is created
  with exactly that policy (`networkBlockAll` / `domainAllowList`). An unregistered digest is
  refused instead of falling back to the organization default.
- **Tenancy.** Every sandbox carries `acyclic.tenant` (or none when no tenant is configured);
  listing and reads ignore sandboxes of any other tenant.
- **Adoption.** A replayed create or fork adopts the holder of its deterministic name only when
  the holder carries exactly this request's provider labels (key, kind, fork slot, parent,
  tenant, contract); a native fork child must also be listed among the parent's forks.
- **Cancellation.** `cancel` deletes only sandboxes the operation created, including one whose
  create returned after the cancellation, never the machine a suspend, wake, policy change,
  checkpoint, or destroy acts on.
- **Recovery.** The shared Streams core retains the exact successful result and settlement
  identity. Native labels cannot turn an absent or partial operation into success.
- **Toolbox.** Guest `exec` prepares the toolbox v2 process-ledger stream lazily.
  Core owns binary stdout/stderr, exit status, cancellation, output limits and
  expiry; process-group cleanup and transport framing are shared there too.
  `read_output` reads core's bounded ephemeral command history after canonical
  machine/driver admission. This history is not restart recovery. The existing
  `DaytonaApi::execute`, `download_file`, and `upload_file` methods remain direct
  wire calls. Toolbox calls send credentials only to admitted proxy hosts.
  Live guest transport qualification still requires a registered process-ledger image.

Lifecycle operation ownership, idempotency, state events and watch use
[`acyclic_machines::core`](../../../docs/backends/machines-core.md). Construction
requires an explicit `OperationRegistry` over the application's canonical Streams
backend. Restart with that same store; pending outcomes never authorize redispatch.
Checkpoint inspection and fork sequencing use that same core and the original
admitted contract, including the opaque provider snapshot address. An untracked
native snapshot is refused; it cannot inherit a historical contract from its
source's current mutable state. Accepted native handles remain cleanup debt when
metadata validation fails, including a collision with an existing logical ID.

Live checks are ignored by default and require `DAYTONA_API_KEY`. Provider checks
also require `DAYTONA_MACHINES_JOURNAL=/durable/account-specific/directory`:

```sh
# Container smoke test of the REST client (create, exec, delete; bounded TTL):
DAYTONA_API_KEY=... cargo test -p acyclic-machines-daytona --test live -- --ignored smoke
# Full Machines conformance suite; needs a Linux VM snapshot and bills the organization:
DAYTONA_API_KEY=... DAYTONA_SNAPSHOT=<vm snapshot> \
  cargo test -p acyclic-machines-daytona --test live -- --ignored conformance
```
