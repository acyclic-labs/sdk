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

Use `HttpMachinesProvider` for an authenticated HTTPS service. Qualify an immutable image before creating a machine, retain idempotency keys for recovery, and inspect operations whose outcome is indeterminate. The simulator is for contract tests: it does **not** execute an OS, isolate workloads, provide durability, or offer service availability. A hosted provider must document its own guarantees.

[API source](https://github.com/acyclic-labs/sdk/tree/main/typescript/packages/machines/src) · [Protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/machines)
