import { WasmClientViews } from "../generated/wasm/acyclic_harness_wasm.js";
import { ensureHarnessWasm } from "./wasm-runtime.js";
import { SnapshotStore, type ClientSnapshot } from "./client-snapshot.js";

export type BranchId = `${string}:${string}`;
export type PredictionOutcome = "Pending" | "Confirmed" | "Replaced" | "Invalidated" | "Removed";
export type OperationOutcome = "Unknown" | "Admitted" | "Indeterminate" | "Completed" | "Rejected" | "Cancelled";
export interface ClientViewLimits {
  readonly records: number;
  readonly branches: number;
  readonly edges: number;
  readonly bytes: number;
  readonly work: number;
  readonly retention: bigint;
  readonly visible: number;
}
export interface ClientFact<Value> {
  readonly key: string;
  /** Exact authority/generation/revision/content pin, encoded by the host. */
  readonly basis: string;
  readonly value: Value;
  readonly bytes: number;
}
export interface ClientObservation<Value> {
  readonly fact: ClientFact<Value> | null;
  readonly operation: readonly [string, OperationOutcome] | null;
  readonly work: number;
}
/** Trusted in-process host adapter. Never accept a remote `verified: true` flag.
 * Callbacks must be pure, deterministic and bounded by work; reject stale evidence
 * using existing domain validation/reducers. bytes accounts all retained deep
 * values, assumptions, keys and operation clones, not just serialized wire size.
 * Values/assumptions must be transitively immutable; large bodies use references.
 */
export interface ClientDomain<Value, Assumption, Evidence> {
  readonly identity: string;
  validate(fact: ClientFact<Value>, predicted: Value, assumption: Assumption, work: number): { bytes: number; work: number };
  observe(evidence: Evidence, current: ClientFact<Value> | null, work: number): ClientObservation<Value>;
  corresponds(predicted: Value, canonical: Value, work: number): { matches: boolean; work: number };
}
export interface ClientPrediction<Value, Assumption> {
  readonly key: string;
  readonly basis: string;
  readonly operation: string | null;
  readonly predicted: Value;
  readonly assumption: Assumption;
  readonly dependencies: readonly { readonly branch: BranchId; readonly requirement: "Prediction" | "Confirmed" }[];
  readonly expires: bigint;
}
export interface ClientView<Value> {
  readonly value: Value;
  readonly basis: string;
  readonly branch: BranchId | null;
  readonly adapter: string | null;
  /** Selected operation outcome remains distinct from prediction correspondence. */
  readonly prediction: PredictionOutcome | null;
  readonly outcome: OperationOutcome | null;
  readonly hypotheses: readonly Readonly<{ branch: BranchId; prediction: PredictionOutcome; outcome: OperationOutcome }>[];
}
export interface ClientViewStore<Value> extends ClientSnapshot<ClientView<Value>> { dispose(): void }

type Selection<Value> = {
  key: string;
  overlays: readonly BranchId[];
  store: SnapshotStore<ClientView<Value>>;
};

function tick(value: bigint): void {
  if (typeof value !== "bigint" || value < 0n || value > 0xffff_ffff_ffff_ffffn) throw new RangeError("client tick must be u64");
}

/** Demand-scoped Rust-backed hypotheses. Construction starts no IO or timers.
 * Explicitly initialize the shared Harness WASM before constructing synchronously.
 */
export class ClientViews<Value, Assumption, Evidence> {
  readonly #kernel: WasmClientViews;
  readonly #selections = new Set<Selection<Value>>();
  #disposed = false;
  readonly limits: ClientViewLimits;

  static async initialize(): Promise<void> { await ensureHarnessWasm(); }

  constructor(domain: ClientDomain<Value, Assumption, Evidence>, namespace: string, sequence: bigint,
    limits: ClientViewLimits) {
    tick(sequence);
    this.limits = Object.freeze({ ...limits });
    this.#kernel = new WasmClientViews(domain.identity, namespace, sequence, this.limits,
      domain.validate.bind(domain), domain.observe.bind(domain), domain.corresponds.bind(domain));
  }

  #live(): void { if (this.#disposed) throw new Error("client views are disposed"); }

  begin(request: ClientPrediction<Value, Assumption>): BranchId {
    this.#live();
    return this.#kernel.begin({ key: request.key, basis: request.basis, operation: request.operation,
      dependencies: request.dependencies, expires: request.expires }, request.predicted, request.assumption) as BranchId;
  }

  view(key: string, overlays: readonly BranchId[] = []): ClientView<Value> {
    this.#live();
    const selected = this.#kernel.view(key, overlays) as Omit<ClientView<Value>, "prediction" | "outcome" | "hypotheses">;
    const [prediction, outcome] = selected.branch === null ? [null, null]
      : this.#kernel.status(selected.branch) as readonly [PredictionOutcome, OperationOutcome];
    const hypotheses = overlays.map(branch => {
      const [prediction, outcome] = this.#kernel.status(branch) as readonly [PredictionOutcome, OperationOutcome];
      return Object.freeze({ branch, prediction, outcome });
    });
    return Object.freeze({ ...selected, prediction, outcome, hypotheses: Object.freeze(hypotheses) });
  }

  /** Explicit bounded selection; dispose releases its snapshot cache/listeners. */
  select(key: string, overlays: readonly BranchId[] = []): ClientViewStore<Value> {
    this.#live();
    if (this.#selections.size >= this.limits.records + this.limits.branches) {
      throw new RangeError("client selection capacity exceeded");
    }
    const initial = this.view(key, overlays);
    const selection: Selection<Value> = { key, overlays: Object.freeze([...overlays]), store: new SnapshotStore(initial) };
    this.#selections.add(selection);
    return {
      getSnapshot: selection.store.getSnapshot,
      subscribe: selection.store.subscribe,
      dispose: () => { this.#selections.delete(selection); selection.store.dispose(); },
    };
  }

  #publish(key: string | null, branches: readonly BranchId[]): void {
    const notifications: (() => void)[] = [];
    for (const selection of this.#selections) {
      if (selection.key !== key && !selection.overlays.some(branch => branches.includes(branch))) continue;
      const previous = selection.store.getSnapshot();
      selection.overlays = selection.overlays.filter(branch => this.#kernel.status(branch) !== null);
      const next = this.view(selection.key, selection.overlays);
      if (previous.value !== next.value || previous.basis !== next.basis || previous.branch !== next.branch ||
        previous.adapter !== next.adapter || previous.prediction !== next.prediction || previous.outcome !== next.outcome ||
        previous.hypotheses.length !== next.hypotheses.length || previous.hypotheses.some((status, index) => {
          const other = next.hypotheses[index];
          return status.branch !== other?.branch || status.prediction !== other?.prediction || status.outcome !== other?.outcome;
        })) {
        const notify = selection.store.stage(next);
        if (notify) notifications.push(notify);
      }
    }
    for (const notify of notifications) notify();
  }

  observe(key: string, evidence: Evidence): Readonly<{ authoritative: boolean; hypotheses: readonly BranchId[]; work: number }> {
    this.#live();
    const [authoritative, hypotheses, work] = this.#kernel.observe(key, evidence) as [boolean, BranchId[], number];
    this.#publish(authoritative ? key : null, hypotheses);
    return Object.freeze({ authoritative, hypotheses: Object.freeze(hypotheses), work });
  }

  discard(branch: BranchId): readonly BranchId[] {
    this.#live();
    const changed = this.#kernel.discard(branch) as BranchId[];
    this.#publish(null, changed);
    return Object.freeze(changed);
  }

  advance(now: bigint): readonly BranchId[] {
    this.#live();
    tick(now);
    const changed = this.#kernel.advance(now) as BranchId[];
    this.#publish(null, changed);
    return Object.freeze(changed);
  }

  release(key: string): void {
    this.#live();
    if ([...this.#selections].some(selection => selection.key === key)) throw new Error("dispose selected views before release");
    this.#kernel.release(key);
  }

  residency(): readonly [records: number, branches: number, edges: number, bytes: number] {
    this.#live();
    const [bytes, records, branches, edges] = this.#kernel.residency() as [number, number, number, number];
    return Object.freeze([Number(records), Number(branches), Number(edges), Number(bytes)]);
  }

  dispose(): void {
    if (this.#disposed) return;
    this.#disposed = true;
    for (const selection of this.#selections) selection.store.dispose();
    this.#selections.clear();
    this.#kernel.free();
  }
}
