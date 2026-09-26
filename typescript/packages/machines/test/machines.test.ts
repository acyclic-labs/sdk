import { describe, expect, test } from "bun:test";
import { HttpMachinesProvider, Machines, MachinesTransportError, SimulatedMachines, customImage, forkFidelity, idempotencyKey, managedOci, machineId, operationId, type CreateMachine } from "../src/index.ts";
import { decodeUsageReceipt, wireResults } from "../src/wire-results.ts";
import { create, toBinary } from "@bufbuild/protobuf";
import { ImageKind, ImageQualificationSchema, MachineIdSchema, MutationOutcomeSchema, UsageReceiptSchema } from "../generated/proto/machines/v1/machines_pb.js";

const request = (idempotencyKey: string): CreateMachine => ({
  idempotencyKey,
  image: { kind: "custom", digestHex: "07".repeat(32) },
  compatibility: { kind: "best-effort" },
  performance: "elastic",
  suspension: { kind: "after-idle", milliseconds: 15_000 },
  expiration: { kind: "never" },
  networkPolicyDigestHex: "08".repeat(32),
  budgets: { spendMicros: 0n, concurrency: 0 },
});

describe("Machines simulation", () => {
  test("live forks preserve fidelity, idempotency, and source lifetime", async () => {
    expect(forkFidelity(["live-fork", "disk-fork"])).toBe("memory-and-disk");
    expect(forkFidelity(["disk-fork"])).toBe("disk-only");
    expect(forkFidelity(["live-checkpoint"])).toBeNull();
    const provider = new SimulatedMachines();
    const created = await provider.create(request("live-source"));
    if (created.kind !== "created") throw new Error("wrong create outcome");
    const forked = await provider.forkMachine(created.machine.id, 2, idempotencyKey("live-fork"));
    if (forked.kind !== "machine-forked") throw new Error("wrong fork outcome");
    expect(forked.source).toBe(created.machine.id);
    expect(forked.fidelity).toBe("memory-and-disk");
    expect(forked.children).toHaveLength(2);
    expect(new Set(forked.children.map(child => child.id)).size).toBe(2);
    expect(await provider.forkMachine(created.machine.id, 2, idempotencyKey("live-fork"))).toEqual(forked);
    await expect(provider.forkMachine(created.machine.id, 3, idempotencyKey("live-fork"))).rejects.toThrow();
    await expect(provider.destroyMachine(created.machine.id, idempotencyKey("destroy-too-early"))).rejects.toThrow();
    for (const child of forked.children) await provider.destroyMachine(child.id, idempotencyKey(`destroy-${child.id}`));
    await provider.destroyMachine(created.machine.id, idempotencyKey("destroy-after-children"));
    const disk = new SimulatedMachines({ capabilities: ["disk-fork", "suspend-resume"] });
    const diskSource = await disk.create(request("disk-source"));
    if (diskSource.kind !== "created") throw new Error("wrong create outcome");
    const diskFork = await disk.forkMachine(diskSource.machine.id, 1, idempotencyKey("disk-fork"));
    expect(diskFork.kind === "machine-forked" && diskFork.fidelity).toBe("disk-only");
    const unsupported = new SimulatedMachines({ capabilities: ["live-checkpoint"] });
    const plain = await unsupported.create(request("plain-source"));
    if (plain.kind !== "created") throw new Error("wrong create outcome");
    await expect(unsupported.forkMachine(plain.machine.id, 1, idempotencyKey("unsupported"))).rejects.toThrow();
  });
  test("exposes checked high-level qualification, attachment, recovery, and bounded listing", async () => {
    const provider = new SimulatedMachines();
    const machines = new Machines(provider);
    const image = managedOci(`ghcr.io/acyclic/agent@sha256:${"a".repeat(64)}`);
    expect((await machines.qualifyImage(image)).image).toEqual(image);
    const created = await machines.create({ ...request("high-level"), idempotencyKey: idempotencyKey("high-level") });
    expect((await machines.attach(created.id)).id).toBe(created.id);
    await expect(machines.attach(machineId("missing"))).rejects.toThrow("not found");
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

  test("constructs a hosted client from explicit or process environment", () => {
    expect(Machines.fromEnv({ endpoint: "https://example.test", token: "token" }).provider).toBeInstanceOf(HttpMachinesProvider);
    expect(() => Machines.fromEnv({})).toThrow("ACYCLIC_MACHINES_ENDPOINT is required");
  });

  test("replays exact create and rejects key rebinding", async () => {
    const provider = new SimulatedMachines();
    const first = await provider.create(request("create-1"));
    expect(await provider.create(request("create-1"))).toEqual(first);
    await expect(provider.create({ ...request("create-1"), performance: "dedicated" })).rejects.toThrow("bound to another intent");
  });

  test("copies caller-owned budgets into the retained machine contract", async () => {
    const provider = new SimulatedMachines();
    const authored = request("owned-budget");
    const created = await provider.create(authored);
    if (created.kind !== "created") throw new Error("wrong create outcome");
    (authored.budgets as { concurrency: number }).concurrency = 99;
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
    await expect(provider.recoverOperation("unknown")).rejects.toThrow("not found");
    await expect(provider.inspectOperation("operation:unknown:0")).rejects.toThrow("not found");
  });

  test("uses the lineage receipt commitment instead of the retired quantity", async () => {
    const provider = new SimulatedMachines();
    const created = await provider.create(request("usage-1"));
    if (created.kind !== "created") throw new Error("wrong create outcome");
    const usage = await provider.usage(created.machine.id, 1, 2);
    expect(usage.lineageReceiptSha256).toEqual(new Uint8Array(32));
    expect("lineageSharedBytes" in usage).toBe(false);
  });

  test("preserves exact u64 usage quantities across the Rust protobuf boundary", () => {
    const value = decodeUsageReceipt(toBinary(UsageReceiptSchema, create(UsageReceiptSchema, {
      machine: create(MachineIdSchema, { value: Uint8Array.from({ length: 16 }, () => 1) }),
      startUnixMs: 1n, endUnixMs: 2n,
      elasticCpuNs: 9007199254740993n, dedicatedCpuNs: 18446744073709551615n,
      privateResidentByteSeconds: 9007199254740995n, durablePrivateBytes: 9007199254740997n,
      lineageReceiptSha256: new Uint8Array(32), egressBytes: 9007199254740999n, receipt: new Uint8Array(),
    })));
    expect(value.elasticCpuNs).toBe(9007199254740993n);
    expect(value.dedicatedCpuNs).toBe(18446744073709551615n);
    expect(value.egressBytes).toBe(9007199254740999n);
  });

  test("rejects substituted protobuf qualification and mutation identities", () => {
    const digest = new Uint8Array(32).fill(7);
    const qualification = toBinary(ImageQualificationSchema, create(ImageQualificationSchema, {
      image: { kind: ImageKind.CUSTOM, immutableReference: { case: "customDigest", value: digest } },
      compatibilityRevision: new Uint8Array(32).fill(8),
    }));
    expect(() => wireResults.qualification(qualification, { kind: "custom", digestHex: "09".repeat(32) })).toThrow("substituted");
    const mutation = toBinary(MutationOutcomeSchema, create(MutationOutcomeSchema, {
      result: { case: "suspended", value: { value: new Uint8Array(16).fill(7) } },
    }));
    expect(() => wireResults.mutation(mutation, { kind: "suspended", machineId: machineId("different") })).toThrow("substituted");
  });

  test("rejects zero and malformed image digests at every Machines boundary", async () => {
    const zero = "0".repeat(64);
    expect(() => managedOci(`registry.example/app@sha256:${zero}`)).toThrow("non-zero");
    expect(() => customImage(zero)).toThrow("non-zero");
    const malformed = { kind: "custom" as const, digestHex: zero };
    await expect(new SimulatedMachines().qualifyImage(malformed)).rejects.toThrow("non-zero");
    await expect(new SimulatedMachines().create({ ...request("zero-network"), networkPolicyDigestHex: zero })).rejects.toThrow("non-zero");
    const hosted = new HttpMachinesProvider({ endpoint: "https://example.test", token: "token", fetcher: (() => { throw new Error("fetch should not run"); }) as typeof fetch });
    await expect(hosted.qualifyImage(malformed)).rejects.toThrow("non-zero");
    for (const bytes of [new Uint8Array(31).fill(1), new Uint8Array(32)]) {
      const qualification = toBinary(ImageQualificationSchema, create(ImageQualificationSchema, {
        image: { kind: ImageKind.CUSTOM, immutableReference: { case: "customDigest", value: bytes } },
        compatibilityRevision: new Uint8Array(32).fill(8),
      }));
      expect(() => wireResults.qualification(qualification, { kind: "custom", digestHex: "01".repeat(32) })).toThrow("digest");
    }
  });

  test("rejects non-u32 counts before entering the WASM boundary", async () => {
    const provider = new SimulatedMachines();
    await expect(provider.listMachines(null, 1.5)).rejects.toThrow(RangeError);
    await expect(provider.events("machine" as never, null, -1)).rejects.toThrow(RangeError);
    await expect(provider.fork("checkpoint" as never, 0x1_0000_0000, "elastic", "key" as never)).rejects.toThrow(RangeError);
  });

  test("normalizes checkpoint image identities at the Rust boundary", async () => {
    const provider = new SimulatedMachines();
    const image = { kind: "checkpoint" as const, checkpointId: "captured" as never };
    const first = await provider.qualifyImage(image);
    const second = await provider.qualifyImage(image);
    expect(first.image).toEqual(second.image);
    expect(first.image.kind).toBe("checkpoint");
    if (first.image.kind === "checkpoint") expect(first.image.checkpointId).toMatch(/^[0-9a-f-]{36}$/);
  });

  test("validates observations against Rust-normalized identity arguments", async () => {
    const provider = new SimulatedMachines();
    const created = await provider.create(request("normalized-observation"));
    if (created.kind !== "created") throw new Error("wrong create outcome");
    const alias = machineId(created.machine.id.toUpperCase());
    expect((await provider.inspectMachine(alias)).id).toBe(created.machine.id);
    expect((await provider.events(alias, null, 8)).events[0]?.machine).toBe(created.machine.id);
    expect((await provider.usage(alias, 1, 2)).machine).toBe(created.machine.id);
    const captured = await provider.checkpoint(alias, "normalized-checkpoint" as never);
    if (captured.kind !== "checkpointed") throw new Error("wrong checkpoint outcome");
    expect((await provider.inspectCheckpoint(captured.checkpoint.id.toUpperCase() as never)).id).toBe(captured.checkpoint.id);
    const operation = await provider.recoverOperation("normalized-observation" as never);
    const operationAlias = operation.toUpperCase() as never;
    expect((await provider.inspectOperation(operationAlias)).id).toBe(operation);
    expect((await provider.cancel(operationAlias)).id).toBe(operation);
    const watched = [];
    for await (const value of provider.watchOperation(operationAlias)) watched.push(value.id);
    expect(watched).toEqual([operation]);
  });

  test("canonical replay ignores object insertion order", async () => {
    const provider = new SimulatedMachines();
    const original = request("canonical");
    const reordered: CreateMachine = {
      budgets: original.budgets,
      networkPolicyDigestHex: original.networkPolicyDigestHex,
      expiration: original.expiration,
      suspension: original.suspension,
      performance: original.performance,
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
    const forked = await provider.fork(captured.checkpoint.id, 2, "elastic", "fork-1");
    if (forked.kind !== "forked") throw new Error("wrong fork outcome");
    expect(new Set(forked.machines.map((machine) => machine.id)).size).toBe(2);
    await provider.destroyCheckpoint(captured.checkpoint.id, "checkpoint-destroy-1");
    expect((await provider.inspectMachine(created.machine.id)).state).toBe("running");
    expect((await provider.inspectMachine(forked.machines[0]!.id)).state).toBe("running");
  });

  test("decodes every terminal mutation from the Rust protobuf boundary", async () => {
    const provider = new SimulatedMachines();
    const created = await provider.create(request("wire-mutations"));
    if (created.kind !== "created") throw new Error("wrong create outcome");
    const id = created.machine.id;
    expect(await provider.suspend(id, "wire-suspend" as never)).toEqual({ kind: "suspended", machineId: id });
    expect(await provider.wake(id, "wire-wake" as never)).toEqual({ kind: "woken", machineId: id });
    expect(await provider.setSuspensionPolicy(id, { kind: "manual" }, "wire-policy" as never)).toEqual({
      kind: "suspension-policy-set", machineId: id, policy: { kind: "manual" },
    });
    expect(await provider.recover("wire-policy" as never)).toEqual({
      kind: "suspension-policy-set", machineId: id, policy: { kind: "manual" },
    });
    expect(await provider.destroyMachine(id, "wire-destroy" as never)).toEqual({ kind: "machine-destroyed", machineId: id });
  });

  test("managed images cannot be constructed from mutable OCI tags", async () => {
    const provider = new SimulatedMachines();
    expect(() => managedOci("ghcr.io/acyclic/agent:latest")).toThrow();
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
  });

  test("managed transport rejects substituted identities and impossible machine evidence", async () => {
    const simulated = new SimulatedMachines();
    const created = await simulated.create(request("remote-shape"));
    if (created.kind !== "created") throw new Error("wrong create outcome");
    const encode = (value: unknown) => JSON.stringify(value, (_key, item) => typeof item === "bigint" ? { $bigint: item.toString() } : item instanceof Uint8Array ? { $bytes: btoa(String.fromCharCode(...item)) } : item);
    const serve = (value: unknown) => new HttpMachinesProvider({ endpoint: "https://example.test", token: "x", fetcher: async () => new Response(encode(value)) });
    await expect(serve({ ...created.machine, id: "other" }).inspectMachine(created.machine.id)).rejects.toMatchObject({ message: "invalid machines/inspect response", status: 200 });
    await expect(serve({ ...created.machine, changedAtUnixMs: 0 }).inspectMachine(created.machine.id)).rejects.toMatchObject({ message: "invalid machines/inspect response", status: 200 });
    await expect(serve({ ...created.machine, endpoints: [{ name: "x", uri: "a" }, { name: "x", uri: "b" }] }).inspectMachine(created.machine.id)).rejects.toMatchObject({ message: "invalid machines/inspect response", status: 200 });
    await expect(serve({ ...created.machine, contract: { ...created.machine.contract, budgets: { spendMicros: -1n, concurrency: 1 } } }).inspectMachine(created.machine.id)).rejects.toMatchObject({ message: "invalid machines/inspect response", status: 200 });
    await expect(serve({ events: [{ machine: "other", sequence: 1, observedAtUnixMs: 1, fact: { kind: "capacity-changed" } }], nextSequence: 1 }).events(created.machine.id, null, 8)).rejects.toMatchObject({ message: "invalid machines/events response", status: 200 });
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

  test("does not expose server error bodies in transport errors", async () => {
    const provider = new HttpMachinesProvider({ endpoint: "https://example.test", token: "x", fetcher: async () => new Response("reflected-secret", { status: 403 }) });
    await expect(provider.inspectMachine("missing" as never)).rejects.toMatchObject({ message: "HTTP 403", status: 403 });
  });
});
