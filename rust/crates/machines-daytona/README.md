# acyclic-machines-daytona

`MachinesProvider` over [Daytona](https://www.daytona.io) sandboxes: VM classes (`linux-vm`,
`windows`) with the full feature set, and `container` sandboxes with disk fork only.
It is one replaceable provider behind the `acyclic-machines` contract: nothing Daytona-specific
crosses into the harness, and switching providers changes bindings, not orchestration.

| Trait method            | Daytona REST call (`https://app.daytona.io/api`)                            |
|-------------------------|-----------------------------------------------------------------------------|
| `qualify_image`         | digest → registered snapshot; `GET /snapshots/{name}` must report a VM class |
| `create`                | `POST /sandbox` (`CreateSandbox`), named `acyclic-{key}` so replays collide  |
| `checkpoint`            | `POST /sandbox/{id}/snapshot` with `includeMemory: true` (hot snapshot)       |
| `fork`                  | `POST /sandbox` from the hot snapshot, once per child                         |
| `fork_machine` (VM)     | `POST /sandbox/{id}/fork` per child: memory and disk (`LiveFork`)             |
| `fork_machine` (container) | tar the workspace via toolbox, `POST /sandbox` from the parent's snapshot, upload and unpack (`DiskFork`) |
| `suspend` / `wake`      | `POST /sandbox/{id}/pause` / `POST /sandbox/{id}/start`                       |
| `set_suspension_policy` | `POST /sandbox/{id}/autopause/{minutes}` (pause keeps memory; stop does not)  |
| `destroy_*`             | `DELETE /sandbox/{id}` / `DELETE /snapshots/{id}`                             |
| `events`                | poll `GET /sandbox/{id}` and diff state                                       |
| `usage`                 | allocation estimate, marked provisional (Daytona has no per-sandbox meter)    |
| `recover`               | registry, then `GET /sandbox?labels=` on the `acyclic.key` label              |

Live fork goes through the trait (`MachinesProvider::fork_machine`); callers read the fidelity
from the machine contract or the outcome, never from the provider type:

- **VM classes** declare `LiveFork`. Daytona's native fork copies the running sandbox's memory
  and disk into each child without a checkpoint (`ForkFidelity::MemoryAndDisk`). Children
  share the parent's environment and credentials. Daytona refuses to delete a parent with live
  fork children, so `destroy_machine` checks `GET /sandbox/{id}/forks` first and returns
  `ProviderError::Conflict` until the children are gone.
- **Containers** declare only `DiskFork` (`ForkFidelity::DiskOnly`). The provider archives
  `DaytonaConfig::workspace_dir` (default `/home/daytona/workspace`, env
  `DAYTONA_WORKSPACE_DIR`) in the parent once, creates each child from the parent's snapshot
  with its lifecycle intervals, environment, and non-provider labels, and unpacks the archive
  into it through the toolbox files API. Processes and memory are not inherited: the caller
  restarts its workload in each child from durable history. The copy is as consistent as `tar`
  over a directory the parent may still be writing. Children never block deleting the parent.
  Measured on `daytona-small` (eu): a 4 MiB workspace forked into two children in about 7 s.

Every live-fork child carries `acyclic.kind=live-fork` and `acyclic.parent=<source id>`, so
`recover` rebuilds the outcome after a restart. Containers cannot pause, checkpoint memory, or
auto-suspend: their contracts carry `SuspensionPolicy::Manual`, and `suspend`, `wake`,
`set_suspension_policy`, and `checkpoint` return `ProviderError::Unsupported`.

The sandbox class comes from the snapshot, never from the create request. Other classes are
rejected.

Live checks, all ignored by default and gated on `DAYTONA_API_KEY`:

```sh
# Container smoke test of the REST client (create, exec, delete; bounded TTL):
DAYTONA_API_KEY=... cargo test -p acyclic-machines-daytona --test live -- --ignored smoke
# Container disk fork through the trait (three short-lived sandboxes, always deleted):
DAYTONA_API_KEY=... cargo test -p acyclic-machines-daytona --test live -- --ignored container_disk_fork
# Full Machines conformance suite; needs a Linux VM snapshot and bills the organization:
DAYTONA_API_KEY=... DAYTONA_SNAPSHOT=<vm snapshot> \
  cargo test -p acyclic-machines-daytona --test live -- --ignored conformance
```
