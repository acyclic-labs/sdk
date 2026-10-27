import { AgentGuest, WasmAgentProviders, WasmAgents, WasmStream,
  type WasmAgentRegistry, type WasmBrowserAgentOptions, type WasmReducer, type WasmFileRefWire,
} from "../generated/wasm/acyclic_harness_wasm.js";
import { streamLedgerPort, type StreamLedger } from "./event-ledger.js";
import { decodeMetadata, type EventStorage } from "./event-storage.js";

export { EventStreamLedger, streamLedgerPort } from "./event-ledger.js";
export type { StreamLedger, ImmutableObjects, ProviderReply } from "./event-ledger.js";
export type { EventStorage, StreamChange, Placement } from "./event-storage.js";

interface Identity {
  intent: string; brief?: string; path: string; create: boolean;
  stage: "claiming" | "creating" | "ready";
}

/** Shared one-actor host with event-owned delivery and durable recovery. */
export class EventAgentHost {
  private readonly host: WasmAgents;
  private readonly guest: AgentGuest;
  private serial: Promise<unknown> = Promise.resolve();
  private commands: Promise<unknown> = Promise.resolve();
  private generation = 0;

  constructor(private readonly storage: EventStorage, private readonly ledger: StreamLedger,
    private readonly name: string, options: WasmBrowserAgentOptions,
    owner: WasmReducer, registry: WasmAgentRegistry) {

    const stream = WasmStream.connectProvider(streamLedgerPort(ledger, name));
    const objects = {
      length: (key: Uint8Array) => ledger.objectLength(key),
      read: (key: Uint8Array, maximum: bigint) => ledger.objectRead(key, maximum),
      put: (keys: Uint8Array[], values: Uint8Array[]) => ledger.objectPut(keys, values),
    };
    const providers = WasmAgentProviders.injected(stream, objects, true, options);
    try {
      this.guest = AgentGuest.open(providers, options, owner, registry);
      this.host = WasmAgents.openHostingEvent(providers, options, owner, this.guest.manifest(),
        (delivery: Uint8Array, signal: AbortSignal) => this.guest.onRecord(delivery, signal));
    } finally { providers.free(); stream.free(); }
  }

  private identity(): Identity | undefined {
    const value = this.storage.get("identity");
    if (value === undefined) return undefined;
    const row: unknown = decodeMetadata(value);
    if (row === null) return undefined;
    if (typeof row !== "object" || row === null || !("intent" in row) || typeof row.intent !== "string"
      || ("brief" in row && typeof row.brief !== "string") || !("path" in row) || typeof row.path !== "string"
      || !("create" in row) || typeof row.create !== "boolean" || !("stage" in row)
      || (row.stage !== "claiming" && row.stage !== "creating" && row.stage !== "ready")
      || (row.stage === "creating" && !row.create)) {
      throw new Error("STEP actor-identity failed; recovery: restore host metadata from a verified backup");
    }
    return row as unknown as Identity;
  }

  private async retain(identity: Identity | null): Promise<void> {
    this.storage.put("identity", JSON.stringify(identity));
    await this.storage.flush();
  }

  private async resume(identity: Identity): Promise<Identity> {
    if (identity.stage === "claiming") {
      const placement = await this.ledger.register(this.name, this.host.wakePaths(identity.path));
      if ("refused" in placement) {
        await this.retain(null);
        throw new Error(`STEP actor-placement refused: ${placement.refused}; recovery: choose an unclaimed actor path and an available consistency domain`);
      }
      identity = { ...identity, stage: identity.create ? "creating" : "ready" };
      await this.retain(identity);
    }
    if (identity.stage === "creating") {
      await this.host.create(identity.intent, identity.brief);
      identity = { ...identity, stage: "ready" };
      await this.retain(identity);
    }
    return identity;
  }

  private async place(candidate: Identity): Promise<Identity> {
    const identity = this.identity();
    if (identity && (identity.intent !== candidate.intent || identity.brief !== candidate.brief
      || identity.path !== candidate.path || identity.create !== candidate.create)) {
      throw new Error("STEP actor-placement conflict; recovery: retry the original placement and prompt, or choose a different named actor host");
    }
    // Retain the proposal before the first remote claim. A lost acknowledgement
    // retries exactly that claim; only a definitive refusal makes it reusable.
    if (!identity) await this.retain(candidate);
    return this.resume(identity ?? candidate);
  }
  private command<T>(action: () => Promise<T>): Promise<T> {
    const next = this.commands.then(action);
    this.commands = next.catch(() => undefined);
    return next;
  }

  private event<T>(action: () => Promise<T>): Promise<T> {
    // Commit controls while a turn is running so its canonical inbox follow
    // can abort the guest. Only delivery holds the actor's execution slot.
    return this.command(async () => {
      await this.storage.alarm(Date.now() + 1000);
      const generation = ++this.generation;
      if (!Number.isSafeInteger(generation)) {
        throw new Error("STEP actor-event failed: event counter exhausted; recovery: reopen this named event host from its retained storage");
      }
      return { value: await action(), generation };
    }).then(({ value, generation }) => {
      const next = this.serial.then(() => this.drain(generation));
      this.serial = next.catch(() => undefined);
      return next.then(() => value);
    });
  }

  private async drain(generation: number): Promise<void> {
    const identity = await this.command(async () => {
      const pending = this.identity();
      return pending ? this.resume(pending) : undefined;
    });
    let alarm: bigint | undefined;
    if (identity) {
      alarm = await this.host.drain(identity.path);
    }
    await this.command(async () => {
      // A later admitted control already owns a durable recovery alarm. An
      // earlier delivery must not erase it after observing an older input cut.
      if (this.generation !== generation) return;
      if (alarm === undefined) await this.storage.alarm(undefined);
      else await this.storage.alarm(Number(alarm));
    });
  }

  create(intent: string, brief?: string): Promise<string> {
    return this.event(async () => {
      const path = this.host.validateCreate(intent, brief);
      await this.place({ intent, brief, path, create: true, stage: "claiming" });
      return path;
    });
  }

  send(intent: string, text: string) {
    return this.event(async () => {
      const identity = this.identity();
      if (identity?.stage !== "ready") throw new Error("STEP actor-send failed; recovery: complete create with the actor's original intent and prompt");
      return this.host.send(identity.path, intent, text);
    });
  }

  /** Explicitly activate an already spawned child; spawning alone never calls this. */
  adopt(path: string): Promise<void> {
    return this.event(async () => {
      await this.host.validateAdopt(path);
      await this.place({ intent: path, path, create: false, stage: "claiming" });
    });
  }

  async read(stream: "inbox" | "conversation" | "status", from = 0n, limit = 128) {
    const identity = this.identity();
    if (identity?.stage !== "ready") return [];
    return this.host.read(identity.path, stream, from, limit);
  }

  private path(): string {
    const identity = this.identity();
    if (identity?.stage !== "ready") throw new Error("STEP actor-control failed; recovery: complete the actor's original placement");
    return identity.path;
  }

  cancel(intent: string) { return this.event(() => this.host.cancel(this.path(), intent)); }
  archive(intent: string) { return this.event(() => this.host.archive(this.path(), intent)); }
  pause(paused: boolean) { return this.event(() => this.host.pause(this.path(), paused)); }
  readFile(file: WasmFileRefWire) { return this.host.readFile(this.path(), file); }

  /** Forward both the actor alarm and ledger wake RPC here. */
  wake(): Promise<void> { return this.event(async () => undefined); }
}
