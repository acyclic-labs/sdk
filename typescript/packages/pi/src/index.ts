import type { BrowserAgentRegistry } from "@acyclic-labs/harness/browser";

/** Derive the provider boundary from the actual Rust agent registry. */
type ConfigureModel = Parameters<BrowserAgentRegistry["configureModel"]>;
type Generate = ConfigureModel[1];
export type PiModelEvent = ReturnType<Generate> extends AsyncIterator<infer Event> ? Event : never;
export type PiDispatch = Parameters<Generate>[1];
export type PiAttempt = Parameters<ConfigureModel[2]>[0];

export interface PiBridge<Request, Event> {
  readonly contextCapacity: NonNullable<ConfigureModel[3]>;
  readonly countTokens: NonNullable<ConfigureModel[4]>;
  /** Rust supplies the exact prepared request and authorized, verified file bytes. */
  readonly project: (request: Uint8Array, files: readonly Uint8Array[]) => Request | Promise<Request>;
  readonly run: (request: Request, options: { readonly dispatch: PiDispatch; readonly signal: AbortSignal }) => AsyncIterable<Event>;
  readonly event: (event: Event) => PiModelEvent;
  /** Reconcile the original identity; this callback must never start another run. */
  readonly reconcile: (attempt: PiAttempt) => Promise<readonly Event[] | null>;
}

/** Install a Pi implementation in the canonical host; Rust owns the agent turn. */
export function configurePiModel<Request, Event>(registry: BrowserAgentRegistry,
  model: ConfigureModel[0], compaction: ConfigureModel[5], bridge: PiBridge<Request, Event>): void {
  registry.configureModel(model,
    async function* (request, dispatch, signal, files) {
      signal.throwIfAborted();
      const projected = await bridge.project(request, files);
      signal.throwIfAborted();
      for await (const event of bridge.run(projected, {dispatch, signal})) {
        signal.throwIfAborted();
        yield bridge.event(event);
      }
    },
    async attempt => {
      const events = await bridge.reconcile(attempt);
      return events === null ? null : events.map(event => bridge.event(event));
    },
    model => bridge.contextCapacity(model),
    (request, digest) => bridge.countTokens(request, digest),
    compaction);
}
