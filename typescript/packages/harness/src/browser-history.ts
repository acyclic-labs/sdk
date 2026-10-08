/** Typed forwarding to the existing Rust Stream owner; no JS history admission. */
import { WasmBrowserAggregate, WasmBrowserHistoryReader } from "../generated/wasm/acyclic_harness_wasm.js";
import { ensureHarnessWasm } from "./wasm-runtime.js";
import type { Authority, Command, EventReference, HarnessOptions, OperationId, RecordedScope, Snapshot } from "./index.js";

export interface BrowserHistoryOptions extends HarnessOptions {
  readonly database: string;
  readonly maximumCommands: number;
  readonly maximumJournalBytes: bigint;
  readonly memoryLimits?: BrowserStreamMemoryLimits;
}
export interface BrowserStreamMemoryLimits {
  readonly paths: number;
  readonly path_bytes: number;
  readonly records: number;
  readonly payload_bytes: number;
  readonly commits: number;
  readonly idempotency_results: number;
}
export interface HistoryCursor {
  readonly authority: Authority;
  readonly after_revision: bigint;
  readonly through_revision: bigint;
}
export interface HistoryReadLimits {
  readonly maximum_events: number;
  readonly maximum_bytes: bigint;
}
/** Rust-verified canonical event. An extension supplies its own payload parser. */
export interface CanonicalEvent<Payload = unknown> {
  readonly revision: bigint;
  readonly operation_id: OperationId;
  readonly intent_digest: readonly number[];
  readonly scope: RecordedScope;
  readonly attestation: readonly number[];
  readonly causal_parent: EventReference | null;
  readonly payload: Payload;
}
export interface HistoryPage {
  readonly events: readonly CanonicalEvent[];
  readonly cursor: HistoryCursor;
}
export type CanonicalApplyResult = Readonly<{ result: "applied" | "replayed"; event: CanonicalEvent }>;
/** Canonical command body; the aggregate handle supplies its owning authority. */
export type CanonicalCommand<Action = unknown> = Omit<Command<Action>, "authority">;

function providerOptions(options: BrowserHistoryOptions): unknown {
  return { database: options.database, maximum_commands: options.maximumCommands,
    maximum_journal_bytes: options.maximumJournalBytes, authority: options.authority,
    issuer_id: options.issuerId, issuer_key: options.issuerKey,
    ...(options.memoryLimits === undefined ? {} : { memory: options.memoryLimits }) };
}

export class BrowserHistoryReader {
  constructor(private readonly core: WasmBrowserHistoryReader) {}
  static async open(options: BrowserHistoryOptions): Promise<BrowserHistoryReader> {
    await ensureHarnessWasm();
    return new BrowserHistoryReader(await WasmBrowserHistoryReader.openBrowser(providerOptions(options)));
  }
  pin(afterRevision = 0n): Promise<HistoryCursor> { return this.core.pin(afterRevision); }
  readPage(cursor: HistoryCursor, limits: HistoryReadLimits): Promise<HistoryPage> {
    return this.core.readPage(cursor, limits);
  }
  operationEvent(operation: OperationId): Promise<CanonicalEvent | null> { return this.core.operationEvent(operation); }
  free(): void { this.core.free(); }
}

export class BrowserAggregate {
  private constructor(private readonly core: WasmBrowserAggregate) {}
  static async open(options: BrowserHistoryOptions, snapshot?: Snapshot): Promise<BrowserAggregate> {
    await ensureHarnessWasm();
    return new BrowserAggregate(await WasmBrowserAggregate.openBrowser(providerOptions(options), options.schemas ?? [], snapshot));
  }
  head(): readonly [authority: Authority, revision: bigint] { return this.core.head(); }
  snapshot(): Snapshot { return this.core.snapshot(); }
  execute(command: CanonicalCommand): Promise<CanonicalApplyResult> { return this.core.execute(command); }
  reconcile(command: CanonicalCommand): Promise<CanonicalApplyResult | null> { return this.core.reconcile(command); }
  refreshThrough(revision: bigint, maximumEvents: number): Promise<boolean> {
    return this.core.refreshThrough(revision, maximumEvents);
  }
  historyReader(): BrowserHistoryReader { return new BrowserHistoryReader(this.core.historyReader()); }
  free(): void { this.core.free(); }
}
