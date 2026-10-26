# acyclic-machines-daytona

`MachinesProvider` over [Daytona](https://www.daytona.io) sandboxes: VM classes (`linux-vm`,
`windows`) with native snapshot/fork support, and `container` sandboxes without snapshot/fork support.
It is one replaceable provider behind the `acyclic-machines` contract: nothing Daytona-specific
crosses into the harness, and switching providers changes bindings, not orchestration.

| Trait method            | Daytona REST call (`https://app.daytona.io/api`)                            |
|-------------------------|-----------------------------------------------------------------------------|
| `qualify_image`         | digest → registered snapshot; `GET /snapshots/{name}` must report a VM class |
| `create`                | `POST /sandbox` (`CreateSandbox`), named `acyclic-{key}` so replays collide  |
| `checkpoint`            | `POST /sandbox/{id}/snapshot` with `includeMemory: true` (hot snapshot)       |
| `fork`                  | `POST /sandbox` from the hot snapshot, once per child                         |
| `fork_machine` (VM)     | `POST /sandbox/{id}/fork` per child: memory and disk (`LiveFork`)             |
| `fork_machine` (container) | refused: no atomic filesystem snapshot boundary |
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
  share the parent's environment and credentials. A VM runs one fork at a time (it is
  `forking` for several seconds and refuses another fork with 400 until `started`), so the
  provider fans out as a doubling tree: every node that exists forks once per round (parent;
  then parent and child 0; then parent and children 0-2; ...), so 15 children take four rounds
  and 16 take five. Each fork waits for its source to be started and retries the state refusal
  with backoff. Children forked from earlier children inherit what those executed since their
  own fork, so the set is memory-identical only if the workload holds still during the fork.
  Daytona refuses to delete a sandbox with live fork children, so `destroy_machine` checks
  `GET /sandbox/{id}/forks` first and returns `ProviderError::Conflict` until the children are
  gone; destroy children in reverse index order (a source always has a lower index than the
  children forked from it), then the parent.
- **Containers** advertise no fork capability. `fork_machine` refuses with
  `ProviderError::Unsupported` until an atomic provider snapshot or an admitted immutable
  filesystem generation covers every writer. Sequential TAR reads cannot establish a
  consistent snapshot, even when their checksums match. `DaytonaConfig::workspace_dir`
  (default `/home/daytona/workspace`, env `DAYTONA_WORKSPACE_DIR`) selects the working
  directory for execution; it is not a snapshot boundary.

Every live-fork child carries `acyclic.kind=live-fork`, `acyclic.parent=<requested machine>`,
`acyclic.fork_source=<sandbox it was forked from>`, its index and the requested count, and
`acyclic.ready=true` once complete, so `recover` rebuilds the outcome after a restart and
reports a partial or unfinished fork as indeterminate. Containers cannot pause, checkpoint memory, or
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
  listing, reads, and label recovery ignore sandboxes of any other tenant.
- **Adoption.** A replayed create or fork adopts the holder of its deterministic name only when
  the holder carries exactly this request's provider labels (key, kind, fork slot, parent,
  tenant, contract); a native fork child must also be listed among the parent's forks.
- **Cancellation.** `cancel` deletes only sandboxes the operation created, including one whose
  create returned after the cancellation, never the machine a suspend, wake, policy change,
  checkpoint, or destroy acts on.
- **Recovery.** Fork children record the requested count; label recovery returns a fork only
  when every index is present and reports a partial one as indeterminate.
- **Toolbox.** `DaytonaApi::execute`, `download_file`, and `upload_file` read the sandbox by id
  and send the API key only to a toolbox proxy on the API host or its subdomains (or
  `toolbox_proxy_hosts`).

Live checks, all ignored by default and gated on `DAYTONA_API_KEY`:

```sh
# Container smoke test of the REST client (create, exec, delete; bounded TTL):
DAYTONA_API_KEY=... cargo test -p acyclic-machines-daytona --test live -- --ignored smoke
# Full Machines conformance suite; needs a Linux VM snapshot and bills the organization:
DAYTONA_API_KEY=... DAYTONA_SNAPSHOT=<vm snapshot> \
  cargo test -p acyclic-machines-daytona --test live -- --ignored conformance
```
