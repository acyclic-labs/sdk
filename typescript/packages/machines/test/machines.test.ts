import { describe, expect, test } from "bun:test";
import { HttpMachinesProvider, Machines, MachinesTransportError, SimulatedMachines, idempotencyKey, managedOci, machineId, operationId, type CreateMachine } from "../src/index.ts";

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
    await expect(serve({ ...created.machine, id: "other" }).inspectMachine(created.machine.id)).rejects.toThrow("substituted");
    await expect(serve({ ...created.machine, changedAtUnixMs: 0 }).inspectMachine(created.machine.id)).rejects.toThrow("positive");
    await expect(serve({ ...created.machine, endpoints: [{ name: "x", uri: "a" }, { name: "x", uri: "b" }] }).inspectMachine(created.machine.id)).rejects.toThrow("duplicate");
    await expect(serve({ ...created.machine, contract: { ...created.machine.contract, budgets: { spendMicros: -1n, concurrency: 1 } } }).inspectMachine(created.machine.id)).rejects.toThrow("negative");
    await expect(serve({ events: [{ machine: "other", sequence: 1, observedAtUnixMs: 1, fact: { kind: "capacity-changed" } }], nextSequence: 1 }).events(created.machine.id, null, 8)).rejects.toThrow("identity");
  });

  test("cancels oversized streaming transport responses at the configured bound", async () => {
    let cancelled = false;
    const body = new ReadableStream<Uint8Array>({
      start(controller) { controller.enqueue(new Uint8Array([1, 2, 3])); },
      cancel() { cancelled = true; },
    });
    const provider = new HttpMachinesProvider({ endpoint: "https://example.test", token: "x", maximumResponseBytes: 2, fetcher: async () => new Response(body) });
    await expect(provider.inspectMachine("machine" as never)).rejects.toThrow("configured bound");
    expect(cancelled).toBeTrue();
  });
});
