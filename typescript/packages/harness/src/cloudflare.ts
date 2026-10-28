import { type WasmAgentRegistry, type WasmBrowserAgentOptions, type WasmAgentAuthority,
  type WasmJournalStreamLimits } from "../generated/wasm/acyclic_harness_wasm.js";
import { EventAgentHost, type EventHostAccess } from "./event-host.js";
import { EventStreamLedger, type StreamLedger } from "./event-ledger.js";
import { SqliteEventStorage, SqliteJournal, SqliteObjects, type SqliteStorage } from "./cloudflare-storage.js";

export { SqliteJournal, SqliteObjects } from "./cloudflare-storage.js";

/** Pass the actual Durable Object context; all provider gates remain native. */
export interface CloudflareContext {
  readonly storage: SqliteStorage & {
    setAlarm(time: number): Promise<void>;
    deleteAlarm(): Promise<void>;
  };
}

/** Compose shared event hosting over the DO's SQLite and alarm primitives. */
export function openCloudflareAgent(ctx: CloudflareContext, ledger: StreamLedger, name: string,
  options: WasmBrowserAgentOptions, owner: WasmAgentAuthority, registry: WasmAgentRegistry, access: EventHostAccess): EventAgentHost {
  return new EventAgentHost(new SqliteEventStorage(ctx.storage, "actor"), ledger, name, options, owner, registry, access);
}

/** One SQLite consistency domain, shared by actors requiring atomic forks. */
export function openCloudflareLedger(ctx: CloudflareContext, wake: (actor: string) => Promise<void>,
  limits: WasmJournalStreamLimits): EventStreamLedger {
  return new EventStreamLedger(new SqliteEventStorage(ctx.storage, "wakes"),
    new SqliteObjects(ctx.storage, "filesystem"),
    changed => new SqliteJournal(ctx.storage, "streams", changed), wake, limits);
}
