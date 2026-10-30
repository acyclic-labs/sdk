import { WasmObjects, WasmStream, type WasmAgentRegistry, type WasmBrowserAgentOptions, type WasmAgentAuthority,
  type WasmJournalStreamLimits } from "../generated/wasm/acyclic_harness_wasm.js";
import { EventAgentHost, type EventHostAccess } from "./event-host.js";
import { EventStreamLedger, type StreamLedger } from "./event-ledger.js";
import { streamProviderPort } from "./stream-provider.js";
import { SqliteEventStorage, SqliteJournal, SqliteMedium, type SqliteStorage } from "./cloudflare-storage.js";

export { SqliteJournal, SqliteMedium } from "./cloudflare-storage.js";

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

/** The one Objects v1 account the domain's volumes keep their blobs in. */
const FILESYSTEM_ACCOUNT = new TextEncoder().encode("filesystem");

/** One SQLite consistency domain, shared by actors requiring atomic forks. */
export function openCloudflareLedger(ctx: CloudflareContext, wake: (actor: string) => Promise<void>,
  limits: WasmJournalStreamLimits): EventStreamLedger {
  return new EventStreamLedger(new SqliteEventStorage(ctx.storage, "wakes"),
    new WasmObjects({ medium: new SqliteMedium(ctx.storage, "filesystem"), account: FILESYSTEM_ACCOUNT }),
    changed => WasmStream.openJournal(new SqliteJournal(ctx.storage, "streams", changed), limits)
      .then(streamProviderPort), wake, limits);
}
