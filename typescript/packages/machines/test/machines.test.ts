import { describe, expect, test } from "bun:test";
import { HttpMachinesProvider, Machines, MachinesTransportError, SimulatedMachines, forkFidelity, idempotencyKey, managedOci, machineId, operationId, type CreateMachine } from "../src/index.ts";

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

  test("fork fidelity prefers live fork over disk fork", () => {
    expect(forkFidelity(["live-fork", "disk-fork"])).toBe("memory-and-disk");
    expect(forkFidelity(["disk-fork"])).toBe("disk-only");
    expect(forkFidelity(["live-checkpoint"])).toBeNull();
  });

  test("simulator declares disk fork by default", async () => {
    expect((await new SimulatedMachines().qualifyImage(request("q").image)).capabilities).toContain("disk-fork");
  });

  test("live-forks a running machine at the contract's fidelity and replays exactly", async () => {
    const provider = new SimulatedMachines();
    const created = await provider.create(request("live-source"));
    if (created.kind !== "created") throw new Error("wrong create outcome");
    const forked = await provider.forkMachine(created.machine.id, 2, "live-fork-1");
    if (forked.kind !== "machine-forked") throw new Error("wrong fork outcome");
    expect(forked.source).toBe(created.machine.id);
    expect(forked.fidelity).toBe("memory-and-disk");
    expect(forked.children.map((child) => child.contract)).toEqual([created.machine.contract, created.machine.contract]);
    expect(new Set(forked.children.map((child) => child.id)).size).toBe(2);
    expect(forked.children.every((child) => child.state === "running" && child.lastCheckpoint === null)).toBeTrue();
    expect(await provider.forkMachine(created.machine.id, 2, "live-fork-1")).toEqual(forked);
    await expect(provider.forkMachine(created.machine.id, 3, "live-fork-1")).rejects.toThrow("bound to another intent");
    expect(await provider.recover(idempotencyKey("live-fork-1"))).toEqual(forked);
    const machine = new Machines(provider).machine(created.machine.id);
    const high = await machine.fork(1, idempotencyKey("live-fork-2"));
    expect(high.fidelity).toBe("memory-and-disk");
    expect(high.children).toHaveLength(1);
  });

  test("disk-fork-only contracts fork at disk-only fidelity", async () => {
    const provider = new SimulatedMachines({ capabilities: ["disk-fork", "suspend-resume"] });
    const created = await provider.create(request("disk-source"));
    if (created.kind !== "created") throw new Error("wrong create outcome");
    const forked = await provider.forkMachine(created.machine.id, 1, "disk-fork-1");
    if (forked.kind !== "machine-forked") throw new Error("wrong fork outcome");
    expect(forked.fidelity).toBe("disk-only");
  });

  test("live fork admission rejects unsupported contracts, non-running sources, bad counts, and early source destruction", async () => {
    const unsupported = new SimulatedMachines({ capabilities: ["live-checkpoint"] });
    const plain = await unsupported.create(request("plain"));
    if (plain.kind !== "created") throw new Error("wrong create outcome");
    await expect(unsupported.forkMachine(plain.machine.id, 1, "unsupported-fork")).rejects.toThrow("does not declare live fork");
    const provider = new SimulatedMachines();
    await expect(provider.forkMachine(machineId("missing"), 1, "missing-fork")).rejects.toThrow("resource not found");
    const created = await provider.create(request("admission"));
    if (created.kind !== "created") throw new Error("wrong create outcome");
    for (const count of [0, 1025, 1.5]) await expect(provider.forkMachine(created.machine.id, count, `count-${count}`)).rejects.toThrow("1..=1024");
    await provider.suspend(created.machine.id, "admission-suspend");
    await expect(provider.forkMachine(created.machine.id, 1, "suspended-fork")).rejects.toThrow("only a running machine");
    await provider.wake(created.machine.id, "admission-wake");
    const forked = await provider.forkMachine(created.machine.id, 1, "join-fork");
    if (forked.kind !== "machine-forked") throw new Error("wrong fork outcome");
    await expect(provider.destroyMachine(created.machine.id, "early-destroy")).rejects.toThrow("live-fork children");
    await provider.destroyMachine(forked.children[0]!.id, "child-destroy");
    expect(await provider.destroyMachine(created.machine.id, "early-destroy")).toEqual({ kind: "machine-destroyed", machineId: created.machine.id });
  });

  test("managed transport decodes machine forks and rejects substituted evidence", async () => {
    const simulated = new SimulatedMachines();
    const created = await simulated.create(request("remote-fork"));
    if (created.kind !== "created") throw new Error("wrong create outcome");
    const forked = await simulated.forkMachine(created.machine.id, 2, "remote-fork-1");
    if (forked.kind !== "machine-forked") throw new Error("wrong fork outcome");
    const encode = (value: unknown) => JSON.stringify(value, (_key, item) => typeof item === "bigint" ? { $bigint: item.toString() } : item);
    let seen: { url: string; body: unknown } | undefined;
    const serve = (value: unknown) => new HttpMachinesProvider({ endpoint: "https://example.test", token: "x", fetcher: async (input, init) => { seen = { url: String(input), body: JSON.parse(String(init?.body)) }; return new Response(encode(value)); } });
    expect(await serve(forked).forkMachine(created.machine.id, 2, idempotencyKey("remote-fork-1"))).toEqual(forked);
    expect(seen).toEqual({ url: "https://example.test/v1/machines/machines/fork", body: { machineId: created.machine.id, count: 2, idempotencyKey: "remote-fork-1" } });
    expect((await new Machines(serve(forked)).machine(created.machine.id).fork(2, idempotencyKey("remote-fork-1"))).children.map((child) => child.id)).toEqual(forked.children.map((child) => child.id));
    await expect(serve({ ...forked, source: "other" }).forkMachine(created.machine.id, 2, idempotencyKey("k"))).rejects.toThrow("substituted");
    await expect(serve(forked).forkMachine(created.machine.id, 3, idempotencyKey("k"))).rejects.toThrow("substituted");
    await expect(serve({ ...forked, fidelity: "disk-only" }).forkMachine(created.machine.id, 2, idempotencyKey("k"))).rejects.toThrow("fidelity");
    await expect(serve({ ...forked, fidelity: "imaginary" }).forkMachine(created.machine.id, 2, idempotencyKey("k"))).rejects.toThrow("fidelity is invalid");
    await expect(serve({ ...forked, children: [forked.children[0], forked.children[0]] }).forkMachine(created.machine.id, 2, idempotencyKey("k"))).rejects.toThrow("duplicated");
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
      cancel() { cancelled = true; throw new Error("cancel failed"); },
    });
    const provider = new HttpMachinesProvider({ endpoint: "https://example.test", token: "x", maximumResponseBytes: 2, fetcher: async () => new Response(body) });
    await expect(provider.inspectMachine("machine" as never)).rejects.toBeInstanceOf(MachinesTransportError);
    expect(cancelled).toBeTrue();
  });
});
