/** Typed forwarding to the existing Rust Stream owner; no JS history admission. */
import { WasmBrowserAggregate, WasmBrowserHistoryReader } from "../generated/wasm/acyclic_harness_wasm.js";
import { ensureHarnessWasm } from "./wasm-runtime.js";
import type { Authority, ConversationMessage, Command, EventReference, HarnessOptions, OperationId, RecordedScope, Snapshot } from "./index.js";

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

export interface BrowserHistoryReader {
  pin(afterRevision: bigint): Promise<HistoryCursor>;
  readPage(cursor: HistoryCursor, limits: HistoryReadLimits): Promise<HistoryPage>;
  latestConversationMessage(maximumBytes: bigint): Promise<ConversationMessage | null>;
  operationEvent(operation: OperationId): Promise<CanonicalEvent | null>;
  free(): void;
}

export async function openBrowserHistoryReader(options: BrowserHistoryOptions): Promise<BrowserHistoryReader> {
  await ensureHarnessWasm();
  return WasmBrowserHistoryReader.openBrowser(providerOptions(options));
}

export interface BrowserAggregate {
  head(): readonly [authority: Authority, revision: bigint];
  snapshot(): Snapshot;
  execute(command: CanonicalCommand): Promise<CanonicalApplyResult>;
  reconcile(command: CanonicalCommand): Promise<CanonicalApplyResult | null>;
  refreshThrough(revision: bigint, maximumEvents: number): Promise<boolean>;
  historyReader(): BrowserHistoryReader;
  free(): void;
}

export async function openBrowserAggregate(options: BrowserHistoryOptions, snapshot?: Snapshot): Promise<BrowserAggregate> {
  await ensureHarnessWasm();
  return WasmBrowserAggregate.openBrowser(providerOptions(options), options.schemas ?? [], snapshot);
}
