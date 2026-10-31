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

Use `createMachinesClient` from `@acyclic-labs/machines/transport` with the host's authenticated Connect transport, or `createMachinesGrpcClient` from `@acyclic-labs/machines/grpc` in Node. Both use the public Machines protobuf service. Qualify an immutable image before creating a machine, retain idempotency keys for recovery, and inspect operations whose outcome is indeterminate. The simulator runs the canonical Rust state machine through WebAssembly. Its request and result boundary uses generated Machines protobuf; TypeScript projects those messages into the ergonomic public API and validates identities and observations. String identity aliases are normalized by Rust. The simulator is for contract tests: it does **not** execute an OS, isolate workloads, provide durability, or offer service availability. A hosted provider must document its own guarantees.

The Cloud product API uses gRPC. In Node or Bun, import `createMachinesGrpcClient` from `@acyclic-labs/machines/grpc` and request types from `@acyclic-labs/machines/proto`. Its `machines` client accepts protobuf request fields and returns admissions with machine and operation identities. Watch the original operation to observe completion. Omit Create's image to use the server's installed default; the resulting contract records its immutable identity. Call `close()` to release the connection; RPC options accept an `AbortSignal` and per-call timeout.

`machines.exec({ protocol, machine, argv, timeoutMs })` streams tagged `output.data { stream: STDOUT | STDERR, bytes }` frames, followed by one terminal `output.exitCode` or typed `output.stopped` receipt. Output is yielded as it arrives; the terminal event completes without waiting for transport EOF. Arguments stream with explicit boundaries in chunks of at most 64 KiB, independently of process stdin. The first argument must be nonempty, every argument must be NUL-free, and later empty arguments are preserved. Native OS exec reports argument capacity errors; the SDK imposes no aggregate argument/count limit. Supply a positive uint64 `bigint` timeout in milliseconds; it covers every shell timeout without clamping. Exec on an idle-suspended Machine whose policy is after-idle wakes it inside the same call and deadline; a manually suspended Machine is refused. Optional fields carry binary stdin, guest cwd, absolute authority expiry and a combined output allowance. There is no implicit shell evaluation. Execution has no lifecycle operation ID and is never automatically retried: a canceled, timed-out or lost reply does not prove that the command did not run. Missing terminal results and empty or oversized data frames fail the stream. Only the producer's cleanup receipt attests physical termination.

`machines.readFile` and `writeFile` transfer native binary chunks, at most 65536 bytes each, with `access: { machine, path, offset, deadlineUnixMs }`. Paths are absolute guest paths, offsets and deadlines are bigint, and replies preserve exact bytes, short writes and POSIX errno. Both canonical RPCs are bidirectional streams; generated service clients can retain the same RPC across chunks with the original machine/path/deadline. These scalar helpers send one chunk and close on its native receipt, including when the server keeps its output open. `truncate: true` is allowed only at offset zero. Neither method executes a shell, resets the original deadline or automatically repeats an interrupted write.

`machines.readOutput({ protocol, machine, offset, limit })` reads live guest console bytes. Offsets use `bigint`; `MAX_OUTPUT_READ_BYTES` from the gRPC entry point is 65,536. Continue from the returned `offset + BigInt(data.length)`. A later returned offset exposes discarded console bytes; an empty read means no newer bytes are available now, rather than permanent EOF. The client rejects substituted machine identities, backward offsets, oversized data and offset overflow.

```ts
import { createMachinesGrpcClient } from "@acyclic-labs/machines/grpc";

const client = createMachinesGrpcClient({ endpoint, token });
try {
  const admission = await client.machines.create(request);
  for await (const state of client.machines.watchOperation({
    protocol: { major: 1, minor: 5 }, operation: admission.operation,
  }, { timeoutMs: 0 })) console.log(state);
  const protocol = { major: 1, minor: 5 };
  for await (const frame of client.machines.exec({
    protocol, machine: admission.machine, argv: ["/bin/echo", "hello"], timeoutMs: 1000n,
  })) console.log(frame.output);
  const output = await client.machines.readOutput({
    protocol, machine: admission.machine, offset: 0n, limit: 65536,
  });
  console.log(output.offset, output.data);
} finally {
  client.close();
}
```

`forkMachine` forks a running machine directly when its contract declares `live-fork` (memory and disk) or `disk-fork` (disk only). Like `fork`, it returns the same `created` outcome as `create`, whose `provenance` names the source and the explicit fidelity; open network connections are never inherited. Children have fresh identities and must be destroyed before their source. Providers without either capability require a checkpoint fork or restart instead. The Rust-backed simulator lets callers test both fidelity paths with `SimulatedMachines({ capabilities: [...] })`.

The gRPC transport retains the shared Auth credential request through unary admission and both duplex bodies. Authenticated rejection applies to that exact lease; policy denial and cancellation preserve it. Source events cancel the original HTTP request, and every body pull and delivered frame checks current source validity and absolute expiry. Raw callbacks have no fabricated validity or renewal identity. A refusal never replays a mutation.

[API source](https://github.com/acyclic-labs/sdk/tree/main/typescript/packages/machines/src) · [Protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/machines)
