import { describe, expect, test } from "bun:test";
import { HttpMachinesProvider, Machines, MachinesTransportError, SimulatedMachines, checkpointId, idempotencyKey, managedOci, machineId, operationId, type CreateMachine } from "../src/index.ts";
import { normalize_identity as rustNormalizeIdentity, WasmSimulatedMachines } from "../generated/wasm/acyclic_machines_wasm.js";
import { ensureMachinesWasm } from "../src/wasm-runtime.ts";

const request = (idempotencyKey: string): CreateMachine => ({
  idempotencyKey,
  image: { kind: "custom", digestHex: "07".repeat(32) },
  compatibility: { kind: "best-effort" },
  suspension: { kind: "after-idle", milliseconds: 15_000 },
  expiration: { kind: "never" },
  networkPolicyDigestHex: "08".repeat(32),
  budgets: { spendMicros: 0n, concurrency: 0 },
});

describe("Machines simulation", () => {
  test("uses the Rust OCI constructor and preserves its public image shape", async () => {
    await ensureMachinesWasm();
    expect(managedOci(`ghcr.io/acyclic/agent@sha256:${"a".repeat(64)}`)).toEqual({ kind: "managed-oci", digestHex: "a".repeat(64) });
    expect(managedOci(`registry.example/app@sha256:${"AB".repeat(32)}`)).toEqual({ kind: "managed-oci", digestHex: "ab".repeat(32) });
    expect(managedOci(`registry.example/one@sha256:${"0".repeat(63)}1`)).toEqual({ kind: "managed-oci", digestHex: `${"0".repeat(63)}1` });
    expect(() => managedOci("registry.example/one:latest")).toThrow("OCI image must contain @sha256:<digest>");
    expect(() => managedOci(`registry.example/one@sha256:${"0".repeat(64)}`)).toThrow("image digest cannot be zero");
    expect(() => managedOci(`registry.example/one@sha256:${"a".repeat(63)}`)).toThrow("OCI image digest is invalid");
    expect(() => managedOci(`registry.example/one@sha256:${"g".repeat(64)}`)).toThrow("OCI image digest is invalid");
  });

  test("normalizes public identities through the Rust contract", async () => {
    await ensureMachinesWasm();
    expect(idempotencyKey("caller-key")).toBe(rustNormalizeIdentity("idempotency", "caller-key"));
    expect(machineId("machine-label")).toBe(rustNormalizeIdentity("machine", "machine-label"));
    expect(checkpointId("checkpoint-label")).toBe(rustNormalizeIdentity("checkpoint", "checkpoint-label"));
    expect(operationId("operation-label")).toBe(rustNormalizeIdentity("operation", "operation-label"));
    const uuid = "01020304-0506-4708-890a-0b0c0d0e0f10";
    expect(machineId(uuid)).toBe(uuid);
    expect(() => idempotencyKey("")).toThrow("identity value is required");
  });

  test("rejects malformed WASM inputs through the async boundary", async () => {
    await ensureMachinesWasm();
    const wasm = new WasmSimulatedMachines();
    expect(JSON.parse(WasmSimulatedMachines.encodeHttpRequest({ count: 7n, bytes: Uint8Array.of(0, 255, 128) }))).toEqual({ count: { $bigint: "7" }, bytes: { $bytes: "AP+A" } });
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    expect(() => WasmSimulatedMachines.encodeHttpRequest(cyclic)).toThrow("cyclic");
    const accessor: Record<string, unknown> = {};
    Object.defineProperty(accessor, "value", { enumerable: true, get: () => 1 });
    expect(() => WasmSimulatedMachines.encodeHttpRequest(accessor)).toThrow("accessor");
    const reject = async (invoke: () => Promise<unknown>, message?: string) => {
      const pending = invoke();
      expect(pending).toBeInstanceOf(Promise);
      if (message === undefined) await expect(pending).rejects.toThrow();
      else await expect(pending).rejects.toThrow(message);
    };
    const base = {
      idempotencyKey: "malformed-wasm",
      image: { kind: "custom", digestHex: "07".repeat(32) },
      compatibility: { kind: "best-effort" },
      suspension: { kind: "manual" },
      expiration: { kind: "never" },
      networkPolicyDigestHex: "08".repeat(32),
      budgets: { spendMicros: 0n, concurrency: 0 },
    };
    try {
      await reject(() => wasm.create({ ...base, performance: "dedicated" } as never), "unknown field");
      await reject(() => wasm.fork({ checkpointId: "old-mode", count: 1, idempotencyKey: "old-mode", performance: "elastic" } as never), "unknown field");
      await reject(() => wasm.listMachines({ after: null, limit: 1.5 as never }));
      await reject(() => wasm.create({ ...base, budgets: { spendMicros: 1.5 as never, concurrency: 0 } }));
      await reject(() => wasm.create({ ...base, compatibility: { kind: "require", capabilities: ["elastic-cpu", "elastic-cpu"] } }), "duplicates");
      await reject(() => wasm.create({ ...base, compatibility: { kind: "best-effort", capabilities: [] } as never }), "unknown field");
      await reject(() => wasm.create({ ...base, suspension: { kind: "after-idle", milliseconds: 0 } }), "nonzero");
      await reject(() => wasm.create({ ...base, suspension: { kind: "manual", milliseconds: 1 } as never }), "unknown field");
      await reject(() => wasm.create({ ...base, expiration: { kind: "never", milliseconds: 1 } as never }), "unknown field");
      await reject(() => wasm.create({ ...base, expiration: { kind: "max-age", milliseconds: 0 } }), "nonzero");
      await reject(() => wasm.qualifyImage({ kind: "managed-oci", digestHex: "0".repeat(64) }), "zero");
    } finally {
      wasm.free();
    }
  });

  test("does not advance the Rust clock for a missing suspension-policy target", async () => {
    const provider = new SimulatedMachines();
    const first = await provider.create(request("policy-clock-first"));
    if (first.kind !== "created") throw new Error("wrong create outcome");
    await expect(provider.setSuspensionPolicy(machineId("missing"), { kind: "manual" }, idempotencyKey("policy-clock-missing"))).rejects.toThrow("not found");
    const second = await provider.create(request("policy-clock-second"));
    if (second.kind !== "created") throw new Error("wrong create outcome");
    expect(second.machine.createdAtUnixMs).toBe(first.machine.createdAtUnixMs + 1);
  });

  test("routes every lifecycle operation through the Rust simulator", async () => {
    const provider = new SimulatedMachines();
    const created = await provider.create(request("full-lifecycle"));
    if (created.kind !== "created") throw new Error("wrong create outcome");
    const id = created.machine.id;
    const suspended = await provider.suspend(id, idempotencyKey("suspend-full"));
    expect(suspended).toEqual({ kind: "suspended", machineId: id });
    expect((await provider.inspectMachine(id)).state).toBe("suspended");
    const woken = await provider.wake(id, idempotencyKey("wake-full"));
    expect(woken).toEqual({ kind: "woken", machineId: id });
    const policy = { kind: "manual" } as const;
    expect(await provider.setSuspensionPolicy(id, policy, idempotencyKey("policy-full"))).toEqual({ kind: "suspension-policy-set", machineId: id, policy });
    expect((await provider.inspectMachine(id)).contract.suspension).toEqual(policy);
    const events = await provider.events(id, null, 16);
    expect(events.events.map(event => event.fact)).toEqual([
      { kind: "state", state: "running" }, { kind: "state", state: "suspended" }, { kind: "state", state: "running" },
    ]);
    expect((await provider.events(id, 1, 16)).events.map(event => event.sequence)).toEqual([2, 3]);
    const captured = await provider.checkpoint(id, idempotencyKey("checkpoint-full"));
    if (captured.kind !== "checkpointed") throw new Error("wrong checkpoint outcome");
    expect(await provider.inspectCheckpoint(captured.checkpoint.id)).toEqual(captured.checkpoint);
    const forked = await provider.fork(captured.checkpoint.id, 2, idempotencyKey("fork-full"));
    if (forked.kind !== "forked") throw new Error("wrong fork outcome");
    expect((await provider.listMachines(null, 256)).machines).toHaveLength(3);
    expect(await provider.recover(idempotencyKey("fork-full"))).toEqual(forked);
    const usage = await provider.usage(id, 1, 2);
    expect(usage.machine).toBe(id);
    expect(usage.receipt).toBeInstanceOf(Uint8Array);
    expect(usage.lineageReceiptSha256).toBeInstanceOf(Uint8Array);
    expect(await provider.destroyCheckpoint(captured.checkpoint.id, idempotencyKey("destroy-checkpoint-full"))).toEqual({ kind: "checkpoint-destroyed", checkpointId: captured.checkpoint.id });
    await expect(provider.fork(captured.checkpoint.id, 1, idempotencyKey("fork-after-destroy"))).rejects.toThrow("fork");
    expect(await provider.destroyMachine(id, idempotencyKey("destroy-machine-full"))).toEqual({ kind: "machine-destroyed", machineId: id });
    expect((await provider.inspectMachine(id)).state).toBe("destroyed");
  });

  test("exposes checked high-level qualification, attachment, recovery, and bounded listing", async () => {
    const provider = new SimulatedMachines();
    const machines = new Machines(provider);
    const image = managedOci(`ghcr.io/acyclic/agent@sha256:${"a".repeat(64)}`);
    expect((await machines.qualifyImage(image)).image).toEqual(image);
    const created = await machines.create({ ...request("high-level"), idempotencyKey: idempotencyKey("high-level") });
    expect((await machines.attach(created.id)).id).toBe(created.id);
    await expect(machines.attach(machineId("missing"))).rejects.toThrow("resource not found");
    const operation = await machines.recoverOperation(idempotencyKey("high-level"));
    expect(await machines.recover(operation.id)).toEqual({ id: operation.id, phase: "succeeded" });
    expect(await machines.recoverMutation(idempotencyKey("high-level"))).toHaveProperty("kind", "created");
    expect(operationId(operation.id)).toBe(operation.id);
    const listed: string[] = [];
    for await (const machine of machines.list({ pageSize: 1, maximum: 1 })) listed.push(machine.id);
    expect(listed).toEqual([created.id]);
    const none: string[] = [];
    for await (const machine of machines.list({ maximum: 0 })) none.push(machine.id);
    expect(none).toEqual([]);
  });

  test("does not impose a client-side maximum beyond safe integer precision", async () => {
    const provider = new SimulatedMachines();
    const machines = new Machines(provider);
    const iterator = machines.list({ maximum: 65_537 });
    await expect(iterator.next()).resolves.toEqual({ done: true, value: undefined });
  });

  test("uses Rust-generated page limits for high-level defaults", async () => {
    const provider = new SimulatedMachines();
    let listLimit: number | undefined;
    const listMachines = provider.listMachines.bind(provider);
    provider.listMachines = async (after, limit) => {
      listLimit = limit;
      return listMachines(after, limit);
    };
    const machines = new Machines(provider);
    await machines.list().next();
    expect(listLimit).toBe(256);

    let eventLimit: number | undefined;
    provider.events = async (_machineId, _afterSequence, limit) => {
      eventLimit = limit;
      return { events: [], nextSequence: null };
    };
    await machines.machine(machineId("page-limit-test")).events();
    expect(eventLimit).toBe(1024);
  });

  test("constructs a hosted client from explicit or process environment", () => {
    expect(Machines.fromEnv({ endpoint: "https://example.test", token: "token" }).provider).toBeInstanceOf(HttpMachinesProvider);
    expect(() => Machines.fromEnv({})).toThrow("ACYCLIC_MACHINES_ENDPOINT is required");
  });

  test("replays exact create and rejects key rebinding", async () => {
    const provider = new SimulatedMachines();
    const first = await provider.create(request("create-1"));
    expect(await provider.create(request("create-1"))).toEqual(first);
    await expect(provider.create({ ...request("create-1"), networkPolicyDigestHex: "b".repeat(64) })).rejects.toThrow("bound to another intent");
  });

  test("copies caller-owned budgets into the retained machine contract", async () => {
    const provider = new SimulatedMachines();
    const authored = request("owned-budget");
    const pending = provider.create(authored);
    (authored.budgets as { concurrency: number }).concurrency = 99;
    const created = await pending;
    if (created.kind !== "created") throw new Error("wrong create outcome");
    expect((await provider.inspectMachine(created.machine.id)).contract.budgets.concurrency).toBe(0);
  });

  test("recovers and observes durable mutation operations", async () => {
    const provider = new SimulatedMachines();
    await provider.create(request("operation-1"));
    const operation = await provider.recoverOperation("operation-1");
    const expected = { id: operation, phase: "succeeded" };
    expect(await provider.inspectOperation(operation)).toEqual(expected);
    expect(await provider.cancel(operation)).toEqual(expected);
    const observations = [];
    for await (const observation of provider.watchOperation(operation)) observations.push(observation);
    expect(observations).toEqual([expected]);
    await expect(provider.recoverOperation("unknown")).rejects.toThrow("resource not found");
    await expect(provider.inspectOperation("operation:unknown:0")).rejects.toThrow("resource not found");
  });

  test("uses the lineage receipt commitment instead of the retired quantity", async () => {
    const provider = new SimulatedMachines();
    const created = await provider.create(request("usage-1"));
    if (created.kind !== "created") throw new Error("wrong create outcome");
    const usage = await provider.usage(created.machine.id, 1, 2);
    expect(usage.lineageReceiptSha256).toEqual(new Uint8Array(32));
    expect("lineageSharedBytes" in usage).toBe(false);
  });

  test("keeps Rust generated byte DTOs owned by each result", async () => {
    const provider = new SimulatedMachines();
    const created = await provider.create(request("usage-owned-by-result"));
    if (created.kind !== "created") throw new Error("wrong create outcome");
    const first = await provider.usage(created.machine.id, 1, 2);
    const expectedLineage = first.lineageReceiptSha256.slice();
    const expectedReceipt = first.receipt.slice();
    first.lineageReceiptSha256[0] ^= 0xff;
    if (first.receipt.length > 0) first.receipt[0] ^= 0xff;
    const second = await provider.usage(created.machine.id, 1, 2);
    expect(second.lineageReceiptSha256).toEqual(expectedLineage);
    expect(second.receipt).toEqual(expectedReceipt);
  });

  test("canonical replay ignores object insertion order", async () => {
    const provider = new SimulatedMachines();
    const original = request("canonical");
    const reordered: CreateMachine = {
      budgets: original.budgets,
      networkPolicyDigestHex: original.networkPolicyDigestHex,
      expiration: original.expiration,
      suspension: original.suspension,
      compatibility: original.compatibility,
      image: original.image,
      idempotencyKey: original.idempotencyKey,
    };
    expect(await provider.create(reordered)).toEqual(await provider.create(original));
  });

  test("checkpoint forks fresh machines without cascading lifetime", async () => {
    const provider = new SimulatedMachines();
    const created = await provider.create(request("create-2"));
    if (created.kind !== "created") throw new Error("wrong create outcome");
    const captured = await provider.checkpoint(created.machine.id, "checkpoint-1");
    if (captured.kind !== "checkpointed") throw new Error("wrong checkpoint outcome");
    const forked = await provider.fork(captured.checkpoint.id, 2, idempotencyKey("fork-1"));
    if (forked.kind !== "forked") throw new Error("wrong fork outcome");
    expect(new Set(forked.machines.map((machine) => machine.id)).size).toBe(2);
    await provider.destroyCheckpoint(captured.checkpoint.id, "checkpoint-destroy-1");
    expect((await provider.inspectMachine(created.machine.id)).state).toBe("running");
    expect((await provider.inspectMachine(forked.machines[0]!.id)).state).toBe("running");
  });

  test("managed images cannot be constructed from mutable OCI tags", async () => {
    const provider = new SimulatedMachines();
    expect(() => managedOci("ghcr.io/acyclic/agent:latest")).toThrow();
    expect(() => managedOci(`ghcr.io/acyclic/agent@sha256:${"0".repeat(64)}`)).toThrow("zero");
    const image = managedOci(`ghcr.io/acyclic/agent@sha256:${"a".repeat(64)}`);
    expect((await provider.qualifyImage(image)).image).toEqual(image);
  });

  test("cursor walk returns every machine exactly once in canonical order", async () => {
    const provider = new SimulatedMachines();
    for (const key of ["a", "B", "ä", "Z"]) await provider.create(request(key));
    const ids: string[] = [];
    let cursor: string | null = null;
    do {
      const page = await provider.listMachines(cursor, 1);
      ids.push(...page.machines.map((machine) => machine.id));
      cursor = page.next;
    } while (cursor !== null);
    expect(new Set(ids).size).toBe(4);
    expect(ids).toEqual([...ids].sort());
  });

  test("terminal no-op mutations preserve timestamps and event history", async () => {
    const provider = new SimulatedMachines();
    const created = await provider.create(request("no-op"));
    if (created.kind !== "created") throw new Error("wrong create outcome");
    await provider.destroyMachine(created.machine.id, "destroy-first");
    const first = await provider.inspectMachine(created.machine.id);
    const firstEvents = await provider.events(created.machine.id, null, 16);
    await provider.destroyMachine(created.machine.id, "destroy-again");
    expect(await provider.inspectMachine(created.machine.id)).toEqual(first);
    expect(await provider.events(created.machine.id, null, 16)).toEqual(firstEvents);
  });

  test("managed transport rejects insecure configuration and malformed contracts", async () => {
    expect(() => new HttpMachinesProvider({ endpoint: "http://example.test", token: "x" })).toThrow(TypeError);
    expect(() => new HttpMachinesProvider({ endpoint: "https://example.test", token: "x", maximumResponseBytes: 0 })).toThrow(RangeError);
    const provider = new HttpMachinesProvider({ endpoint: "https://example.test", token: "x", fetcher: async () => new Response(JSON.stringify({ id: "machine", state: "imaginary" })) });
    await expect(provider.inspectMachine("machine" as never)).rejects.toBeInstanceOf(MachinesTransportError);
    const invalidUtf8 = new HttpMachinesProvider({ endpoint: "https://example.test", token: "x", fetcher: async () => new Response(new Uint8Array([0xff])) });
    await expect(invalidUtf8.inspectMachine("machine" as never)).rejects.toThrow("valid UTF-8");
  });

  test("managed transport rejects substituted identities and impossible machine evidence", async () => {
    const simulated = new SimulatedMachines();
    const created = await simulated.create(request("remote-shape"));
    if (created.kind !== "created") throw new Error("wrong create outcome");
    const encode = (value: unknown) => JSON.stringify(value, (_key, item) => typeof item === "bigint" ? { $bigint: item.toString() } : item instanceof Uint8Array ? { $bytes: btoa(String.fromCharCode(...item)) } : item);
    const serve = (value: unknown) => new HttpMachinesProvider({ endpoint: "https://example.test", token: "x", fetcher: async () => new Response(encode(value)) });
    const inspected = await serve(created.machine).inspectMachine(created.machine.id);
    expect(inspected.id).toBe(created.machine.id);
    expect(Object.getPrototypeOf(inspected)).toBe(Object.prototype);
    expect(Object.getPrototypeOf(inspected.contract)).toBe(Object.prototype);
    expect(Object.getPrototypeOf(inspected.contract.image)).toBe(Object.prototype);
    expect(Object.getPrototypeOf(inspected.contract.budgets)).toBe(Object.prototype);
    expect(inspected.contract.budgets.spendMicros).toBe(0n);
    let requestBody: unknown;
    const capturing = new HttpMachinesProvider({ endpoint: "https://example.test", token: "x", fetcher: async (_input, init) => { requestBody = JSON.parse(String(init?.body)); return new Response(encode(created)); } });
    await capturing.create(request("request-encoder"));
    expect(requestBody).toMatchObject({ budgets: { spendMicros: { $bigint: "0" } }, image: { digestHex: "07".repeat(32) } });
    await expect(serve({ image: created.machine.contract.image }).qualifyImage(created.machine.contract.image)).rejects.toThrow("capabilities");
    await expect(serve({ ...created.machine, lastCheckpoint: undefined }).inspectMachine(created.machine.id)).rejects.toThrow("lastCheckpoint");
    await expect(serve({ machines: [], next: undefined }).listMachines(null, 8)).rejects.toThrow("next");
    await expect(serve({ ...created.machine, id: "other" }).inspectMachine(created.machine.id)).rejects.toThrow("substituted");
    await expect(serve({ ...created.machine, changedAtUnixMs: 0 }).inspectMachine(created.machine.id)).rejects.toThrow("positive");
    await expect(serve({ ...created.machine, endpoints: [{ name: "x", uri: "a" }, { name: "x", uri: "b" }] }).inspectMachine(created.machine.id)).rejects.toThrow("duplicate");
    await expect(serve({ ...created.machine, contract: { ...created.machine.contract, budgets: { spendMicros: -1n, concurrency: 1 } } }).inspectMachine(created.machine.id)).rejects.toThrow("negative");
    await expect(serve({ ...created.machine, contract: { ...created.machine.contract, budgets: { spendMicros: 1, concurrency: 1 } } }).inspectMachine(created.machine.id)).rejects.toThrow("bigint");
    await expect(serve({ ...created.machine, createdAtUnixMs: Number.MAX_SAFE_INTEGER + 1 }).inspectMachine(created.machine.id)).rejects.toThrow("safe integer");
    await expect(serve({ events: [{ machine: "other", sequence: 1, observedAtUnixMs: 1, fact: { kind: "capacity-changed" } }], nextSequence: 1 }).events(created.machine.id, null, 8)).rejects.toThrow("identity");
    const capacityEvent = { machine: created.machine.id, sequence: 1, observedAtUnixMs: 1, fact: { kind: "capacity-changed" } };
    expect((await serve({ events: [capacityEvent], nextSequence: 1 }).events(created.machine.id, null, 8)).events[0]?.fact.kind).toBe("capacity-changed");
    await expect(serve({ events: [{ ...capacityEvent, fact: { kind: "capacity" } }], nextSequence: 1 }).events(created.machine.id, null, 8)).rejects.toThrow("event.fact.kind");
    const usage = { machine: created.machine.id, startUnixMs: 1, endUnixMs: 2, elasticCpuNs: 0n, dedicatedCpuNs: 0n, privateResidentByteSeconds: 0n, durablePrivateBytes: 0n, egressBytes: 0n, lineageReceiptSha256: new Uint8Array(32), receipt: new Uint8Array() };
    expect((await serve(usage).usage(created.machine.id, 1, 2)).lineageReceiptSha256).toBeInstanceOf(Uint8Array);
    const maximumU64 = 18_446_744_073_709_551_615n;
    expect((await serve({ ...usage, elasticCpuNs: maximumU64 }).usage(created.machine.id, 1, 2)).elasticCpuNs).toBe(maximumU64);
    await expect(serve({ ...usage, startUnixMs: -1 }).usage(created.machine.id, -1, 2)).rejects.toThrow("safe integer");
    await expect(serve({ ...created.machine, id: " " }).inspectMachine(created.machine.id)).rejects.toThrow("non-empty");
    await expect(serve({ ...usage, lineageReceiptSha256: [1, 2, 3] }).usage(created.machine.id, 1, 2)).rejects.toThrow("bytes");
    await expect(serve({ ...usage, lineageReceiptSha256: { $bytes: "AR==" } }).usage(created.machine.id, 1, 2)).rejects.toThrow("bytes");
  });

  test("binds hosted mutation outcomes to their request identities and policies", async () => {
    const simulated = new SimulatedMachines();
    const created = await simulated.create(request("hosted-mutation-bindings"));
    if (created.kind !== "created") throw new Error("wrong create outcome");
    const encode = (value: unknown) => JSON.stringify(value, (_key, item) => typeof item === "bigint" ? { $bigint: item.toString() } : item instanceof Uint8Array ? { $bytes: btoa(String.fromCharCode(...item)) } : item);
    const serve = (value: unknown) => new HttpMachinesProvider({ endpoint: "https://example.test", token: "x", fetcher: async () => new Response(encode(value)) });
    await expect(serve({ ...created, machine: { ...created.machine, contract: { ...created.machine.contract, suspension: { kind: "manual" } } } }).create(request("hosted-mutation-bindings"))).rejects.toThrow("suspension policy");

    const checkpointed = await simulated.checkpoint(created.machine.id, idempotencyKey("hosted-mutation-checkpoint"));
    if (checkpointed.kind !== "checkpointed") throw new Error("wrong checkpoint outcome");
    expect(await serve(checkpointed).checkpoint(created.machine.id, idempotencyKey("hosted-mutation-checkpoint"))).toEqual(checkpointed);
    await expect(serve({ ...checkpointed, checkpoint: { ...checkpointed.checkpoint, source: machineId("other-source") } }).checkpoint(created.machine.id, idempotencyKey("hosted-mutation-checkpoint"))).rejects.toThrow("substituted");

    const checkpointForked = await simulated.fork(checkpointed.checkpoint.id, 1, idempotencyKey("hosted-mutation-checkpoint-fork"));
    if (checkpointForked.kind !== "forked") throw new Error("wrong checkpoint fork outcome");
    expect(await serve(checkpointForked).fork(checkpointed.checkpoint.id, 1, idempotencyKey("hosted-mutation-checkpoint-fork"))).toEqual(checkpointForked);
    const substitutedCheckpoint = checkpointId("other-checkpoint");
    await expect(serve({
      ...checkpointForked,
      machines: checkpointForked.machines.map(machine => ({
        ...machine,
        lastCheckpoint: substitutedCheckpoint,
        contract: { ...machine.contract, image: { kind: "checkpoint", checkpointId: substitutedCheckpoint } },
      })),
    }).fork(checkpointed.checkpoint.id, 1, idempotencyKey("hosted-mutation-checkpoint-fork"))).rejects.toThrow("checkpoint");

    const forked = await simulated.forkMachine(created.machine.id, 2, idempotencyKey("hosted-mutation-fork"));
    if (forked.kind !== "machine-forked") throw new Error("wrong machine fork outcome");
    expect(await serve(forked).forkMachine(created.machine.id, 2, idempotencyKey("hosted-mutation-fork"))).toEqual(forked);
    await expect(serve({ ...forked, source: machineId("other-source") }).forkMachine(created.machine.id, 2, idempotencyKey("hosted-mutation-fork"))).rejects.toThrow("substituted");
    await expect(serve({
      ...forked,
      children: forked.children.map((machine, index) => index === 0 ? { ...machine, id: created.machine.id } : machine),
    }).forkMachine(created.machine.id, 2, idempotencyKey("hosted-mutation-fork"))).rejects.toThrow("source");
    await expect(serve({
      ...forked,
      fidelity: "disk-only",
      children: forked.children.map(machine => ({ ...machine, contract: { ...machine.contract, capabilities: ["live-fork"] as const } })),
    }).forkMachine(created.machine.id, 2, idempotencyKey("hosted-mutation-fork"))).rejects.toThrow("capabilities");

    const policy = { kind: "suspension-policy-set" as const, machineId: created.machine.id, policy: { kind: "manual" as const } };
    expect(await serve(policy).setSuspensionPolicy(created.machine.id, policy.policy, idempotencyKey("hosted-mutation-policy"))).toEqual(policy);
    await expect(serve({ ...policy, machineId: machineId("other-machine") }).setSuspensionPolicy(created.machine.id, policy.policy, idempotencyKey("hosted-mutation-policy"))).rejects.toThrow("substituted");
    await expect(serve({ ...policy, policy: { kind: "after-idle", milliseconds: 30_000 } }).setSuspensionPolicy(created.machine.id, policy.policy, idempotencyKey("hosted-mutation-policy"))).rejects.toThrow("policy");

    await expect(serve({ kind: "suspended", machineId: machineId("other-machine") }).suspend(created.machine.id, idempotencyKey("hosted-mutation-suspend"))).rejects.toThrow("substituted");
  });

  test("cancels oversized streaming transport responses at the configured bound", async () => {
    let cancelled = false;
    const body = new ReadableStream<Uint8Array>({
      start(controller) { controller.enqueue(new Uint8Array([1, 2, 3])); },
      cancel() { cancelled = true; throw new Error("cancel failed"); },
    });
    const provider = new HttpMachinesProvider({ endpoint: "https://example.test", token: "x", maximumResponseBytes: 2, fetcher: async () => new Response(body) });
    await expect(provider.inspectMachine("machine" as never)).rejects.toBeInstanceOf(MachinesTransportError);
    expect(cancelled).toBeTrue();
  });
});
