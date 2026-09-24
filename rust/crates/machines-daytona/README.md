# acyclic-machines-daytona

`MachinesProvider` over [Daytona](https://www.daytona.io) VM sandboxes (`linux-vm`, `windows`).
It is one replaceable provider behind the `acyclic-machines` contract: nothing Daytona-specific
crosses into the harness, and switching providers changes bindings, not orchestration.

| Trait method            | Daytona REST call (`https://app.daytona.io/api`)                            |
|-------------------------|-----------------------------------------------------------------------------|
| `qualify_image`         | digest → registered snapshot; `GET /snapshots/{name}` must report a VM class |
| `create`                | `POST /sandbox` (`CreateSandbox`), named `acyclic-{key}` so replays collide  |
| `checkpoint`            | `POST /sandbox/{id}/snapshot` with `includeMemory: true` (hot snapshot)       |
| `fork`                  | `POST /sandbox` from the hot snapshot, once per child                         |
| `suspend` / `wake`      | `POST /sandbox/{id}/pause` / `POST /sandbox/{id}/start`                       |
| `set_suspension_policy` | `POST /sandbox/{id}/autopause/{minutes}` (pause keeps memory; stop does not)  |
| `destroy_*`             | `DELETE /sandbox/{id}` / `DELETE /snapshots/{id}`                             |
| `events`                | poll `GET /sandbox/{id}` and diff state                                       |
| `usage`                 | allocation estimate, marked provisional (Daytona has no per-sandbox meter)    |
| `recover`               | registry, then `GET /sandbox?labels=` on the `acyclic.key` label              |

`DaytonaProvider::fork_machine` is the fork-join fast path outside the trait: Daytona's native
VM fork (`POST /sandbox/{id}/fork`) copies a *running* sandbox's memory and disk into each child
without a checkpoint. Children share the parent's environment and credentials, and Daytona
refuses to delete a parent while it has live fork children.

The sandbox class comes from the snapshot, never from the create request. Container snapshots
are rejected because they have no pause, memory snapshot, or fork.

Live checks, both ignored by default and gated on `DAYTONA_API_KEY`:

```sh
# Container smoke test of the REST client (create, exec, delete; bounded TTL):
DAYTONA_API_KEY=... cargo test -p acyclic-machines-daytona --test live -- --ignored smoke
# Full Machines conformance suite; needs a Linux VM snapshot and bills the organization:
DAYTONA_API_KEY=... DAYTONA_SNAPSHOT=<vm snapshot> \
  cargo test -p acyclic-machines-daytona --test live -- --ignored conformance
```
