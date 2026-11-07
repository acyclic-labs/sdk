import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { createMachine, listMachines, machineEvents, SimulatedMachines, checkpointId, idempotencyKey, managedOci, machineId, operationId, type CreateMachine } from "../src/index.ts";
import { normalize_identity as rustNormalizeIdentity, validatePageSize, WasmSimulatedMachines } from "../generated/wasm/acyclic_machines_wasm.js";

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
    expect(managedOci(`ghcr.io/acyclic/agent@sha256:${"a".repeat(64)}`)).toEqual({ kind: "managed-oci", digestHex: "a".repeat(64) });
    expect(managedOci(`registry.example/app@sha256:${"AB".repeat(32)}`)).toEqual({ kind: "managed-oci", digestHex: "ab".repeat(32) });
    expect(managedOci(`registry.example/one@sha256:${"0".repeat(63)}1`)).toEqual({ kind: "managed-oci", digestHex: `${"0".repeat(63)}1` });
    expect(() => managedOci("registry.example/one:latest")).toThrow();
    expect(() => managedOci(`registry.example/one@sha256:${"0".repeat(64)}`)).toThrow();
    expect(() => managedOci(`registry.example/one@sha256:${"a".repeat(63)}`)).toThrow();
    expect(() => managedOci(`registry.example/one@sha256:${"g".repeat(64)}`)).toThrow();
  });

  test("normalizes public identities through the Rust contract", async () => {
    for (const [kind, value, expected] of [
      ["machine", "machine-label", "f505efb2-cd64-5ea0-9974-9ba4f3406d41"],
      ["idempotency", "caller-key", "17349194-b4fc-57e4-8d9b-9b394f7ef694"],
      ["checkpoint", "checkpoint-label", "35d49ad2-6173-566b-899b-3b9e7ca6dd8f"],
      ["operation", "operation-label", "e244134d-f2b2-5427-966f-fed03a393176"],
      ["machine", "00000000-0000-0000-0000-000000000000", "1e2580b5-4e09-54f6-8a53-c545d68d48f0"],
      ["machine", "λ/東京", "f7b55a84-f196-5927-b8d1-b58c87631e9c"],
      ["operation", "machine-label", "ce6b2acf-3009-5161-80df-3e82cd99b3ea"],
      ["machine", "{01020304-0506-4708-890A-0B0C0D0E0F10}", "01020304-0506-4708-890a-0b0c0d0e0f10"],
    ] as const) {
      expect(rustNormalizeIdentity(kind, value)).toBe(expected);
    }
    expect(idempotencyKey("caller-key")).toBe(rustNormalizeIdentity("idempotency", "caller-key"));
    expect(machineId("machine-label")).toBe(rustNormalizeIdentity("machine", "machine-label"));
    expect(checkpointId("checkpoint-label")).toBe(rustNormalizeIdentity("checkpoint", "checkpoint-label"));
    expect(operationId("operation-label")).toBe(rustNormalizeIdentity("operation", "operation-label"));
    const uuid = "01020304-0506-4708-890a-0b0c0d0e0f10";
    expect(machineId(uuid)).toBe(uuid);
    expect(machineId(uuid.toUpperCase())).toBe(uuid);
    const kinds = ["machine", "checkpoint", "operation", "idempotency"];
    for (const label of ["caller-label", "00000000-0000-0000-0000-000000000000"]) {
      const identities = kinds.map(kind => rustNormalizeIdentity(kind, label));
      expect(new Set(identities).size).toBe(kinds.length);
      for (const [index, identity] of identities.entries()) {
        const bytes = createHash("sha256").update("acyclic-machines-identity-v1\0")
          .update(kinds[index]!).update("\0").update(label).digest().subarray(0, 16);
        bytes[6] = (bytes[6]! & 0x0f) | 0x50;
        bytes[8] = (bytes[8]! & 0x3f) | 0x80;
        const hex = bytes.toString("hex");
        const expected = `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
        expect(identity).toBe(expected);
        expect(machineId(identity)).toBe(identity);
      }
    }
    expect(() => idempotencyKey("")).toThrow();
  });

  test("rejects malformed WASM inputs through the async boundary", async () => {
    const wasm = new WasmSimulatedMachines();
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
      await reject(() => wasm.create({ ...base, selectedVolumeRef: {
        provider: { namespace: "example", family: "filesystem", version: "1" },
        id: "selected-workspace", volumeClass: 1, owner: { owner: { project: "project" } },
      } }), "no hosted guest admission");
      expect((await wasm.listMachines({ after: null, limit: 1 })).machines).toEqual([]);
      await reject(() => wasm.create({ ...base, image: undefined }), "no installed default image");
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
      // Rejections return through Rust rather than throwing past its frames,
      // so the instance that saw every malformed input above stays usable.
      const created = await wasm.create({ ...base, idempotencyKey: rustNormalizeIdentity("idempotency", "malformed-wasm") });
      expect(created.kind).toBe("created");
    } finally {
      wasm.free();
    }
  });

  test("does not advance the Rust clock for a missing suspension-policy target", async () => {
    const provider = new SimulatedMachines();
    const first = await provider.create(request("policy-clock-first"));
    if (first.kind !== "created") throw new Error("wrong create outcome");
    await expect(provider.setSuspensionPolicy(machineId("missing"), { kind: "manual" }, idempotencyKey("policy-clock-missing"))).rejects.toThrow();
    const second = await provider.create(request("policy-clock-second"));
    if (second.kind !== "created") throw new Error("wrong create outcome");
    expect(second.machines[0]!.createdAtUnixMs).toBe(first.machines[0]!.createdAtUnixMs + 1);
  });

  test("routes every lifecycle operation through the Rust simulator", async () => {
    const provider = new SimulatedMachines();
    const created = await provider.create(request("full-lifecycle"));
    if (created.kind !== "created") throw new Error("wrong create outcome");
    const id = created.machines[0]!.id;
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
    for (const unsafe of [-1, 1.5, 2 ** 53]) {
      await expect(provider.events(id, unsafe, 16)).rejects.toBeInstanceOf(RangeError);
      await expect(provider.usage(id, unsafe, 2 ** 53 - 1)).rejects.toBeInstanceOf(RangeError);
    }
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
    await expect(provider.fork(captured.checkpoint.id, 1, idempotencyKey("fork-after-destroy"))).rejects.toThrow();
    expect(await provider.destroyMachine(id, idempotencyKey("destroy-machine-full"))).toEqual({ kind: "machine-destroyed", machineId: id });
    expect((await provider.inspectMachine(id)).state).toBe("destroyed");
  });

  test("exposes checked high-level qualification, attachment, recovery, and bounded listing", async () => {
    const provider = new SimulatedMachines();
    const image = managedOci(`ghcr.io/acyclic/agent@sha256:${"a".repeat(64)}`);
    expect((await provider.qualifyImage(image)).image).toEqual(image);
    const created = await createMachine(provider, { ...request("high-level"), idempotencyKey: idempotencyKey("high-level") });
    expect((await provider.inspectMachine(created)).id).toBe(created);
    await expect(provider.inspectMachine(machineId("missing"))).rejects.toMatchObject({ name: "MachinesError", code: "not-found" });
    const operation = await provider.recoverOperation(idempotencyKey("high-level"));
    expect(await provider.inspectOperation(operation)).toEqual({ id: operation, phase: "succeeded" });
    expect(await provider.recover(idempotencyKey("high-level"))).toHaveProperty("kind", "created");
    expect(operationId(operation)).toBe(operation);
    const listed: string[] = [];
    for await (const machine of listMachines(provider, { pageSize: 1, maximum: 1 })) listed.push(machine);
    expect(listed).toEqual([created]);
    const none: string[] = [];
    for await (const machine of listMachines(provider, { maximum: 0 })) none.push(machine);
    expect(none).toEqual([]);

    const forked = await provider.forkMachine(created, 1025, idempotencyKey("high-level-fork"));
    if (forked.kind !== "forked") throw new Error("wrong live fork outcome");
    const all = new Set<string>();
    for await (const machine of listMachines(provider)) all.add(machine);
    expect(all.size).toBe(1026);
    expect(all.has(created)).toBe(true);
    let cursor: import("../src/index.js").ResultCursor | null = { cut: forked.results, from: "0" };
    let selected = 0;
    while (cursor !== null) {
      const page = await provider.readResults(cursor, 3);
      expect(page.machines.length).toBeLessThanOrEqual(3);
      for (const child of page.machines) expect(all.has(child.id)).toBe(true);
      selected += page.machines.length;
      cursor = page.next;
    }
    expect(selected).toBe(1025);
    const bounded: string[] = [];
    for await (const machine of listMachines(provider, { maximum: 1 })) bounded.push(machine);
    expect(bounded).toHaveLength(1);
  });

  test("does not impose a client-side maximum beyond safe integer precision", async () => {
    const provider = new SimulatedMachines();
    const iterator = listMachines(provider, { maximum: 65_537 });
    await expect(iterator.next()).resolves.toEqual({ done: true, value: undefined });
  });

  test("uses Rust-generated page limits for high-level defaults", async () => {
    const provider = new SimulatedMachines();
    let listLimit: number | undefined;
    const listPage = provider.listMachines.bind(provider);
    provider.listMachines = async (after, limit) => {
      listLimit = limit;
      return listPage(after, limit);
    };
    await listMachines(provider).next();
    expect(listLimit).toBe(256);

    let eventLimit: number | undefined;
    provider.events = async (_machineId, _afterSequence, limit) => {
      eventLimit = limit;
      return { events: [], nextSequence: null };
    };
    await machineEvents(provider, machineId("page-limit-test"));
    expect(eventLimit).toBe(1024);
  });

  test("replays exact create and rejects key rebinding", async () => {
    const provider = new SimulatedMachines();
    const first = await provider.create(request("create-1"));
    expect(await provider.create(request("create-1"))).toEqual(first);
    await expect(provider.create({ ...request("create-1"), networkPolicyDigestHex: "b".repeat(64) })).rejects.toThrow();
  });

  test("copies caller-owned budgets into the retained machine contract", async () => {
    const provider = new SimulatedMachines();
    const authored = request("owned-budget");
    const pending = provider.create(authored);
    (authored.budgets as { concurrency: number }).concurrency = 99;
    const created = await pending;
    if (created.kind !== "created") throw new Error("wrong create outcome");
    expect((await provider.inspectMachine(created.machines[0]!.id)).contract.budgets.concurrency).toBe(0);
  });

  test("recovers and pulls terminal mutation observations", async () => {
    const provider = new SimulatedMachines();
    await provider.create(request("operation-1"));
    const operation = await provider.recoverOperation("operation-1");
    const expected = { id: operation, phase: "succeeded" };
    expect(await provider.inspectOperation(operation)).toEqual(expected);
    expect(await provider.cancel(operation)).toEqual(expected);
    const observations = [];
    for await (const observation of provider.watchOperation(operation)) observations.push(observation);
    expect(observations).toEqual([expected]);
    const unopened = provider.watchOperation(operation);
    await unopened.return();
    expect(await unopened.next()).toEqual({ done: true, value: undefined });
    const wasm = new WasmSimulatedMachines();
    try {
      await wasm.create(request("pull-operation"));
      const id = await wasm.recoverOperation("pull-operation");
      const watch = await wasm.watchOperation(id);
      try {
        expect(await watch.next()).toEqual({ id, phase: "succeeded" });
        expect(await watch.next()).toBeNull();
        watch.close();
        expect(await watch.next()).toBeNull();
      } finally { watch.free(); }
    } finally { wasm.free(); }

    await expect(provider.recoverOperation("unknown")).rejects.toMatchObject({ name: "MachinesError", code: "not-found" });
    await expect(provider.inspectOperation("operation:unknown:0")).rejects.toMatchObject({ name: "MachinesError", code: "not-found" });
  });

  test("uses the lineage receipt commitment instead of the retired quantity", async () => {
    const provider = new SimulatedMachines();
    const created = await provider.create(request("usage-1"));
    if (created.kind !== "created") throw new Error("wrong create outcome");
    const usage = await provider.usage(created.machines[0]!.id, 1, 2);
    expect(usage.lineageReceiptSha256).toEqual(new Uint8Array(32));
    expect("lineageSharedBytes" in usage).toBe(false);
  });

  test("keeps Rust generated byte DTOs owned by each result", async () => {
    const provider = new SimulatedMachines();
    const created = await provider.create(request("usage-owned-by-result"));
    if (created.kind !== "created") throw new Error("wrong create outcome");
    const first = await provider.usage(created.machines[0]!.id, 1, 2);
    const expectedLineage = first.lineageReceiptSha256.slice();
    const expectedReceipt = first.receipt.slice();
    first.lineageReceiptSha256[0] ^= 0xff;
    if (first.receipt.length > 0) first.receipt[0] ^= 0xff;
    const second = await provider.usage(created.machines[0]!.id, 1, 2);
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
    const captured = await provider.checkpoint(created.machines[0]!.id, "checkpoint-1");
    if (captured.kind !== "checkpointed") throw new Error("wrong checkpoint outcome");
    const forked = await provider.fork(captured.checkpoint.id, 2, idempotencyKey("fork-1"));
    if (forked.kind !== "forked") throw new Error("wrong fork outcome");
    expect(forked.provenance).toEqual({ source: { kind: "checkpoint", checkpointId: captured.checkpoint.id }, fidelity: "memory-and-disk" });
    const children = (await provider.readResults({ cut: forked.results, from: "0" }, 2)).machines;
    expect(new Set(children.map((machine) => machine.id)).size).toBe(2);
    // A fork child is an ordinary machine: every read names its checkpoint image.
    const listed = (await provider.listMachines(null, 256)).machines;
    for (const child of children) {
      expect(child.contract.image).toEqual({ kind: "checkpoint", checkpointId: captured.checkpoint.id });
      expect(await provider.inspectMachine(child.id)).toEqual(child);
      expect(listed).toContainEqual(child);
    }
    await provider.destroyCheckpoint(captured.checkpoint.id, "checkpoint-destroy-1");
    expect((await provider.inspectMachine(created.machines[0]!.id)).state).toBe("running");
    expect((await provider.inspectMachine(children[0]!.id)).state).toBe("running");
  });

  test("managed images cannot be constructed from mutable OCI tags", async () => {
    const provider = new SimulatedMachines();
    expect(() => managedOci("ghcr.io/acyclic/agent:latest")).toThrow();
    expect(() => managedOci(`ghcr.io/acyclic/agent@sha256:${"0".repeat(64)}`)).toThrow();
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
    await provider.destroyMachine(created.machines[0]!.id, "destroy-first");
    const first = await provider.inspectMachine(created.machines[0]!.id);
    const firstEvents = await provider.events(created.machines[0]!.id, null, 16);
    await provider.destroyMachine(created.machines[0]!.id, "destroy-again");
    expect(await provider.inspectMachine(created.machines[0]!.id)).toEqual(first);
    expect(await provider.events(created.machines[0]!.id, null, 16)).toEqual(firstEvents);
  });

  test("Rust page validation rejects coerced JavaScript values", () => {
    const validatePage = validatePageSize as unknown as (pageSize: unknown) => void;
    for (const pageSize of ["1", true, new Number(1), 1n, Symbol("page-size"), null, undefined, 0, 257, 1.5]) {
      expect(() => validatePage(pageSize)).toThrow();
    }
    validatePage(1);
  });

  test("delegates machine page limits to the Rust provider boundary", async () => {
    const provider = new SimulatedMachines();
    await expect(listMachines(provider, { pageSize: 0, maximum: 1 }).next()).rejects.toThrow();
    await expect(listMachines(provider, { pageSize: 257, maximum: 257 }).next()).rejects.toThrow();
  });

  test("validates page size before custom providers are called", async () => {
    class RecordingProvider extends SimulatedMachines {
      calls = 0;
      override listMachines(after: ReturnType<typeof machineId> | null, limit: number) {
        this.calls += 1;
        return Promise.resolve({ machines: [], next: after });
      }
    }
    const provider = new RecordingProvider();
    for (const pageSize of [0, 257, 1.5]) {
      await expect(listMachines(provider, { pageSize, maximum: 1 }).next()).rejects.toThrow(RangeError);
    }
    expect(provider.calls).toBe(0);
  });
});
