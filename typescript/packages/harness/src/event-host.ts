import { AgentGuest, WasmAgentProviders, WasmAgentServer, WasmStream,
  type WasmAgentRegistry, type WasmBrowserAgentOptions, type WasmAgentAuthority, type WasmFileRefWire,
  type WasmHostingAdmission, type WasmSessionAuthority, type AgentCommand,
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

/** Verified by the owning platform before it opens the host, never by a guest. */
export interface EventHostAccess {
  admission: WasmHostingAdmission;
  authority: WasmSessionAuthority;
  credential: Uint8Array;
  policy(principal: Uint8Array, command: AgentCommand): boolean | Promise<boolean>;
}

/** Shared one-actor host with event-owned delivery and durable recovery. */
export class EventAgentHost {
  private readonly host: WasmAgentServer;
  private serial: Promise<unknown> = Promise.resolve();
  private commands: Promise<unknown> = Promise.resolve();
  private readonly events = { generation: 0 };

  constructor(private readonly storage: EventStorage, private readonly ledger: StreamLedger,
    private readonly name: string, options: WasmBrowserAgentOptions,
    owner: WasmAgentAuthority, registry: WasmAgentRegistry, private readonly access: EventHostAccess) {

    const stream = WasmStream.connectProvider(streamLedgerPort(ledger, name));
    const objects = {
      length: (key: Uint8Array) => ledger.objectLength(key),
      read: (key: Uint8Array, maximum: bigint) => ledger.objectRead(key, maximum),
      put: (keys: Uint8Array[], values: Uint8Array[]) => ledger.objectPut(keys, values),
    };
    const providers = WasmAgentProviders.injected(stream, objects, true, options);
    try {
      const { audience, scope } = access.admission;
      const guest = AgentGuest.open(providers, options, owner, registry, { kind: "admitted", audience, scope });
      const events = this.events;
      this.host = WasmAgentServer.open(providers, options, owner, registry, access.admission, access.authority, {
        manifest: guest.manifest(),
        onRecord: (delivery: Uint8Array, signal: AbortSignal, interrupt: AbortSignal) => guest.onRecord(delivery, signal, interrupt),
        policy: (principal: Uint8Array, command: AgentCommand) => access.policy(principal, command),
        admitted: () => recoverable(storage, events),
      });
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
      || (row.stage !== "claiming" && row.stage !== "creating" && row.stage !== "ready")) {
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
      identity = { ...identity, stage: "creating" };
      await this.retain(identity);
    }
    // A reopened event host binds the same committed tree before any command.
    // Opening a child observes its adoption; it does not deliver a turn.
    if (identity.stage === "ready") await this.host.adopt(identity.path);
    else if (identity.create) {
      await this.host.create(identity.intent);
      if (identity.brief !== undefined) await this.host.command({ kind: "prompt", agent: identity.path,
        id: identity.intent, body: identity.brief }, this.access.credential);
    } else await this.host.adopt(identity.path);
    if (identity.stage === "creating") {
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
      await recoverable(this.storage, this.events);
      const generation = this.events.generation;
      const identity = this.identity();
      if (identity?.stage === "ready") await this.resume(identity);
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
      if (this.events.generation !== generation) return;
      if (alarm === undefined) await this.storage.alarm(undefined);
      else await this.storage.alarm(Number(alarm));
    });
  }

  create(intent: string, brief?: string): Promise<string> {
    const path = this.host.validateCreate(intent, brief);
    return this.event(async () => {
      await this.place({ intent, ...(brief === undefined ? {} : { brief }), path, create: true, stage: "claiming" });
      return path;
    });
  }

  send(intent: string, text: string) {
    return this.event(async () => {
      const identity = this.identity();
      if (identity?.stage !== "ready") throw new Error("STEP actor-send failed; recovery: complete create with the actor's original intent and prompt");
      return this.host.command({ kind: "prompt", agent: identity.path, id: intent,
        body: text }, this.access.credential);
    });
  }

  /** Explicitly activate an already spawned child; spawning alone never calls this. */
  async adopt(path: string): Promise<void> {
    await this.host.validateAdopt(path);
    return this.event(async () => {
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

  cancel(intent: string) { return this.event(() => this.host.command({ kind: "cancel", agent: this.path(), id: intent }, this.access.credential)); }
  archive(intent: string) { return this.event(() => this.host.archive(this.path(), intent)); }
  pause(paused: boolean) { return this.event(() => this.host.pause(this.path(), paused)); }
  readFile(file: WasmFileRefWire) { return this.host.readFile(this.path(), file); }

  /** Host-published routing metadata for the public harness client. */
  binding(): Uint8Array { return this.host.binding(); }

  /** Canonical calls may await a spawn while a separate wake delivers it. */
  async call(invocation: Uint8Array, credential: Uint8Array): Promise<Uint8Array> {
    await this.command(async () => {
      const identity = this.identity();
      if (identity?.stage !== "ready") throw new Error("STEP actor-call failed; recovery: complete the original root placement");
      await this.resume(identity);
    });
    const generation = this.events.generation;
    try { return await this.host.call(invocation, credential); }
    finally { if (this.events.generation !== generation) await this.wake(); }
  }

  /** Forward both the actor alarm and ledger wake RPC here. */
  wake(): Promise<void> { return this.event(async () => undefined); }
}

async function recoverable(storage: EventStorage, events: { generation: number }): Promise<void> {
  await storage.alarm(Date.now() + 1000);
  if (!Number.isSafeInteger(++events.generation)) {
    throw new Error("STEP actor-event failed: event counter exhausted; recovery: reopen this named event host from its retained storage");
  }
}
