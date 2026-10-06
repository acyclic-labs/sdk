# @acyclic-labs/machines

Typed machine lifecycle, image qualification, checkpoints, forks, operation recovery, events, and usage receipts. Providers declare their actual assurance level.

```sh
npm install @acyclic-labs/machines
```

```ts
import { SimulatedMachines, managedOci } from "@acyclic-labs/machines";

const machines = new SimulatedMachines();
const image = managedOci(`registry.example/app@sha256:${"a".repeat(64)}`);
console.log(await machines.qualifyImage(image));
console.log(machines.assurance); // "process-local-simulation"
```

Use `await Machines.fromEnv()` for the Rust native provider. The native service uses `ACYCLIC_MACHINES_ENDPOINT`, `ACYCLIC_MACHINES_CA_FILE`, `ACYCLIC_MACHINES_CERT_FILE`, and `ACYCLIC_MACHINES_KEY_FILE`. Qualify an immutable image before creating a machine, retain idempotency keys for recovery, and inspect operations whose outcome is indeterminate. The simulator runs the canonical Rust state machine through WebAssembly for contract tests. String identity aliases are normalized by Rust.

`forkMachine` forks a running machine directly when its contract declares `live-fork` (memory and disk) or `disk-fork` (disk only). The returned fidelity is explicit; open network connections are never inherited. Children have fresh identities and must be destroyed before their source. Providers without either capability require a checkpoint fork or restart instead. The Rust-backed simulator lets callers test both fidelity paths with `SimulatedMachines({ capabilities: [...] })`.

[API source](https://github.com/acyclic-labs/sdk/tree/main/typescript/packages/machines/src) · [Protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/machines)
