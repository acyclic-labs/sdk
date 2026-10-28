import { DurableObject } from "cloudflare:workers";
import { ActorsDomain, ActorsPackage } from "./generated/acyclic_cloudflare_conformance.js";
import { WasmActorHost, WasmStream } from "../../../../typescript/packages/harness/generated/wasm/acyclic_harness_wasm.js";
import { SqliteEventStorage, SqliteJournal, type StreamChange } from "../../../../typescript/packages/harness/src/cloudflare-storage.js";
import { streamLedgerPort } from "../../../../typescript/packages/harness/src/event-ledger.js";

interface Env { ACTORS: DurableObjectNamespace<Actors>; ACTOR: DurableObjectNamespace<Actor> }
interface Route { address: number[]; paths: string[]; generation: number; pending: boolean }
const root = "actors/state";
const recovery = "bash scripts/qualify-cloudflare.sh";
function refuse(step: string): never { throw new Error(`STEP hosted-actors-${step} failed; recovery: ${recovery}`); }
function key(address: Uint8Array): string { return Array.from(address, byte => byte.toString(16).padStart(2, "0")).join(""); }
function failure(error: unknown) { return { code: typeof error === "object" && error !== null && "code" in error && typeof error.code === "string" ? error.code : "unavailable" }; }
function route(value: string): Route {
  let row: Route;
  try { row = JSON.parse(value); } catch { return refuse("routing-json"); }
  if (!row || !Array.isArray(row.address) || row.address.length === 0
    || row.address.some(byte => !Number.isInteger(byte) || byte < 0 || byte > 255)
    || !Array.isArray(row.paths) || row.paths.some(path => typeof path !== "string" || !path)
    || !Number.isSafeInteger(row.generation) || row.generation < 0 || typeof row.pending !== "boolean") refuse("routing-metadata");
  return row;
}

/** One consistency domain; actual suite and remote owners share its counted provider. */
export class Actors extends DurableObject<Env> {
  private readonly routes = new SqliteEventStorage(this.ctx.storage, "actors-routing");
  private readonly metadata = new SqliteEventStorage(this.ctx.storage, "actors-discovery");
  private readonly domain = Promise.resolve().then(() => ActorsDomain.open({
    configure: (configuration: Uint8Array, maximum: number) => this.journal.configure(configuration, maximum),
    flush: () => this.journal.flush(), read: (index: number) => this.journal.read(index),
    append: async (index: number, admit: () => Uint8Array, changes: readonly StreamChange[]) => {
      // Durable recovery exists BEFORE publishing any state or routing changes.
      await this.ctx.storage.setAlarm(Date.now());
      await this.journal.append(index, admit, changes);
    },
  }, async (address: Uint8Array, paused: boolean) => {
    await this.discover();
    if (this.routes.get(key(address)) === undefined) refuse("pause-owner");
    await this.owner(address).pause(paused);
  }, () => this.restart()));
  private readonly journal = new SqliteJournal(this.ctx.storage, "actors", changes => {
    if (changes.some(([path]) => path === root)) {
      const generation = Number(this.metadata.get("generation") ?? "0") + 1;
      if (!Number.isSafeInteger(generation)) refuse("discovery-generation");
      this.metadata.put("generation", String(generation)); this.metadata.put("pending", "1");
    }
    for (const [name, route] of this.rows()) {
      if (changes.some(([path, subtree]) => route.paths.some(owned => path === owned || (subtree && owned.startsWith(`${path}/`))))) {
        if (!Number.isSafeInteger(route.generation + 1)) refuse("generation");
        this.routes.put(name, JSON.stringify({ ...route, generation: route.generation + 1, pending: true }));
      }
    }
  });
  private readonly calls = { dispatch: 0, cursor: 0, register: 0, wake: 0, lookup: 0, reset: 0 };

  private rows(): [string, Route][] { return this.routes.entries().map(([name, value]) => [name, route(value)]); }
  private owner(address: Uint8Array) { ++this.calls.lookup; return this.env.ACTOR.getByName(`${this.ctx.id.name}/${key(address)}`); }
  private async discover() {
    if (this.metadata.get("pending") !== "1") return;
    const generation = this.metadata.get("generation");
    let full;
    do {
      const page = await (await this.domain).created(BigInt(this.metadata.get("cursor") ?? "0")) as { actors: Uint8Array[]; next: bigint; full: boolean };
      for (const address of page.actors) {
        const name = key(address);
        if (this.routes.get(name) === undefined) {
          if (this.rows().length >= 1000) refuse("capacity");
          this.routes.put(name, JSON.stringify({ address: [...address], paths: [], generation: 0, pending: true }));
        }
        await this.owner(address).admit(this.ctx.id.name!, address);
      }
      this.metadata.put("cursor", String(page.next));
      full = page.full;
    } while (full);
    if (generation === this.metadata.get("generation")) this.metadata.put("pending", "0");
    await this.metadata.flush();
  }
  async register(address: Uint8Array, paths: string[]) {
    ++this.calls.register;
    const name = key(address), raw = this.routes.get(name);
    if (raw === undefined || paths.length === 0 || paths.length > 1000 || new Set(paths).size !== paths.length
      || paths.some(path => !path || path.length > 4096)) refuse("registration");
    const row = route(raw);
    if (this.rows().some(([other, route]) => other !== name && route.paths.some(path => paths.includes(path)))) refuse("route-owner");
    // Register before Host::wake performs its final committed-input recheck.
    this.routes.put(name, JSON.stringify({ ...row, paths }));
    await this.routes.flush();
  }
  async dispatch(_actor: string, operation: string, input: Uint8Array) {
    ++this.calls.dispatch;
    const provider = (await this.domain).provider();
    try { return { value: await provider.dispatch(operation, input) }; }
    catch (error) { return failure(error); } finally { provider.free(); }
  }
  async open(operation: string, input: Uint8Array, preceding: Uint8Array) {
    ++this.calls.cursor;
    const provider = (await this.domain).provider();
    try {
      const cursor = await provider.openProviderCursor(operation, input, preceding);
      let closed = false;
      const close = () => { if (!closed) { closed = true; cursor.close(); cursor.free(); provider.free(); } };
      return { value: new ReadableStream<Uint8Array>({
        async pull(controller) {
          try { const value = await cursor.next(); if (value === null) { controller.close(); close(); } else controller.enqueue(value); }
          catch (error) { controller.error(error); close(); }
        }, cancel: close,
      }, { highWaterMark: 0 }) };
    } catch (error) { provider.free(); return failure(error); }
  }
  async alarm() {
    await this.ctx.storage.setAlarm(Date.now() + 1000);
    await this.discover();
    const errors: unknown[] = [];
    await Promise.all(this.rows().filter(([, route]) => route.pending).map(async ([name, row]) => {
      try {
        ++this.calls.wake;
        await this.owner(Uint8Array.from(row.address)).wake();
        const current = route(this.routes.get(name) ?? refuse("lost-routing-metadata"));
        if (current.generation === row.generation) this.routes.put(name, JSON.stringify({ ...current, pending: false }));
      } catch (error) { errors.push(error); }
    }));
    await this.routes.flush();
    if (errors.length) throw new AggregateError(errors, `STEP hosted-actors-wake failed; recovery: ${recovery}`);
    if (this.metadata.get("pending") !== "1" && !this.rows().some(([, route]) => route.pending)) await this.ctx.storage.deleteAlarm();
  }
  private async restart() {
    await this.discover();
    for (const [, row] of this.rows()) {
      const address = Uint8Array.from(row.address);
      const before = await this.owner(address).incarnation();
      ++this.calls.reset;
      let reset = false;
      try { await this.owner(address).reset(); } catch { reset = true; }
      if (!reset || before === await this.owner(address).incarnation()) refuse("physical-restart");
    }
  }
  async run(bench: boolean) {
    if (await this.ctx.storage.get("ran")) refuse("fresh-domain");
    await this.ctx.storage.put("ran", true);
    const domain = await this.domain;
    const sample = bench ? await domain.bench() : await domain.verify();
    return { sample, rpc_including_setup: { ...this.calls } };
  }
}

/** A real, separately evictable owner for ONE admitted complete actor address. */
export class Actor extends DurableObject<Env> {
  private readonly instance = crypto.randomUUID();
  private readonly package = new ActorsPackage();
  private host?: WasmActorHost;
  private configuration?: { domain: string; address: Uint8Array };
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => { this.configuration = await ctx.storage.get<{ domain: string; address: Uint8Array }>("identity"); });
  }
  async admit(domain: string, address: Uint8Array) {
    if (this.configuration && (this.configuration.domain !== domain || key(this.configuration.address) !== key(address))) refuse("identity");
    await this.ctx.storage.put("identity", { domain, address });
    this.configuration = { domain, address };
  }
  private opened() {
    const configuration = this.configuration ?? refuse("unadmitted-owner");
    const ledger = this.env.ACTORS.getByName(configuration.domain);
    if (!this.host) {
      const stream = WasmStream.connectProvider(streamLedgerPort(ledger, key(configuration.address)));
      try {
        this.host = WasmActorHost.open(stream, root, configuration.address, this.package.manifest(),
          (delivery: Uint8Array, signal: AbortSignal, interrupt: AbortSignal) => this.package.onRecord(delivery, signal, interrupt), 1, 64);
      } finally { stream.free(); }
    }
    return { host: this.host, ledger, address: configuration.address };
  }
  async wake() {
    await this.ctx.storage.setAlarm(Date.now() + 1000);
    const { host, ledger, address } = this.opened();
    for (let changes = 0; changes < 64; changes++) {
      const paths = await host.wakePaths();
      await ledger.register(address, paths);
      const next = await host.wake();
      // A changed subscription set must be registered BEFORE another recheck.
      if (JSON.stringify(paths) !== JSON.stringify(await host.wakePaths())) continue;
      if (next === undefined) await this.ctx.storage.deleteAlarm();
      else if (Number.isSafeInteger(Number(next))) await this.ctx.storage.setAlarm(Number(next));
      else refuse("alarm-range");
      return;
    }
    refuse("routing-work-bound");
  }
  alarm() { return this.wake(); }
  async pause(paused: boolean) {
    await this.opened().host.pause(paused);
    if (!paused) await this.ctx.storage.setAlarm(Date.now());
  }
  incarnation() { return this.instance; }
  reset() { this.ctx.abort("STEP hosted-actors intentional physical restart; recovery: reopen this same durable owner"); }
}
