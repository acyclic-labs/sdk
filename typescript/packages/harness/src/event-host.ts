import { AgentGuest, WasmAgentProviders, WasmAgents, WasmStream,
  type WasmAgentRegistry, type WasmBrowserAgentOptions, type WasmReducer, type WasmFileRefWire,
} from "../generated/wasm/acyclic_harness_wasm.js";
import { streamLedgerPort, type StreamLedger } from "./event-ledger.js";
import { decodeMetadata, type EventStorage } from "./event-storage.js";

export { EventStreamLedger, streamLedgerPort } from "./event-ledger.js";
export type { StreamLedger, ImmutableObjects, ProviderReply } from "./event-ledger.js";
export type { EventStorage, StreamChange } from "./event-storage.js";

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

  private identity(): { intent: string; brief: string | undefined; path: string | undefined } | undefined {
    const value = this.storage.get("identity");
    if (value === undefined) return undefined;
    const row: unknown = decodeMetadata(value);
    if (typeof row !== "object" || row === null || !("intent" in row) || typeof row.intent !== "string"
      || ("brief" in row && typeof row.brief !== "string") || ("path" in row && typeof row.path !== "string")) {
      throw new Error("STEP actor-identity failed; recovery: restore host metadata from a verified backup");
    }
    return { intent: row.intent, brief: "brief" in row ? row.brief as string : undefined,
      path: "path" in row ? row.path as string : undefined };
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
    let identity = this.identity();
    if (identity && !identity.path) {
      const path = await this.host.create(identity.intent, identity.brief);
      this.storage.put("identity", JSON.stringify({ ...identity, path }));
      await this.storage.flush();
      identity = { ...identity, path };
    }
    let alarm: bigint | undefined;
    if (identity) {
      await this.ledger.register(this.name, this.host.wakePaths(identity.path!));
      alarm = await this.host.drain(identity.path!);
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
      const identity = this.identity();
      if (identity && (identity.intent !== intent || identity.brief !== brief)) {
        throw new Error("STEP actor-create conflict; recovery: retry the original intent and prompt, or choose a different named actor host");
      }
      if (!identity) this.storage.put("identity", JSON.stringify({ intent, brief }));
      await this.storage.flush();
      const path = await this.host.create(intent, brief);
      this.storage.put("identity", JSON.stringify({ intent, brief, path }));
      await this.storage.flush();
      return path;
    });
  }

  send(intent: string, text: string) {
    return this.event(async () => {
      const identity = this.identity();
      if (!identity?.path) throw new Error("STEP actor-send failed; recovery: complete create with the actor's original intent and prompt");
      return this.host.send(identity.path, intent, text);
    });
  }

  /** Explicitly activate an already spawned child; spawning alone never calls this. */
  adopt(path: string): Promise<void> {
    return this.event(async () => {
      const identity = this.identity();
      if (identity && identity.path !== path) {
        throw new Error("STEP actor-adopt conflict; recovery: route this path to a different named actor host");
      }
      if (!identity) this.storage.put("identity", JSON.stringify({ intent: path, path }));
      await this.storage.flush();
    });
  }

  async read(stream: "inbox" | "conversation" | "status", from = 0n, limit = 128) {
    const identity = this.identity();
    if (!identity?.path) return [];
    return this.host.read(identity.path, stream, from, limit);
  }

  private path(): string {
    const path = this.identity()?.path;
    if (!path) throw new Error("STEP actor-control failed; recovery: complete create with the actor's original intent and prompt");
    return path;
  }

  cancel(intent: string) { return this.event(() => this.host.cancel(this.path(), intent)); }
  archive(intent: string) { return this.event(() => this.host.archive(this.path(), intent)); }
  pause(paused: boolean) { return this.event(() => this.host.pause(this.path(), paused)); }
  readFile(file: WasmFileRefWire) { return this.host.readFile(this.path(), file); }

  /** Forward both the actor alarm and ledger wake RPC here. */
  wake(): Promise<void> { return this.event(async () => undefined); }
}
