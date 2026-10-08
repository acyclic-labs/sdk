// @generated from typescript/packages/actors/src/observe.ts by scripts/generate-observe.mjs; do not edit.

// Optional per-call observation (docs/observability.md). Packages are published
// separately, so scripts/generate-observe.mjs copies this source into each.

/** One finished call. It never carries bodies, tokens, paths, or contents. */
export interface OperationEvent {
  /** `acyclic.<family>.<op>` names the call; `op` is a static method or route name. */
  readonly family: string;
  readonly op: string;
  readonly durationMs: number;
  readonly ok: boolean;
  /** Stable non-zero error code, else the HTTP status, of a failure; never its message. */
  readonly code?: string | number;
  readonly requestBytes?: number;
  readonly responseBytes?: number;
  /** The work receipt the call already returned. */
  readonly work?: unknown;
}

/** Receives one event per call. Without one, calls are not wrapped at all. */
export interface AcyclicObserver { onOperation(event: OperationEvent): void }

export interface OperationSizes { requestBytes?: number; responseBytes?: number }

/** Records each call as a `performance.measure` entry named `acyclic.<family>.<op>`. */
export function performanceObserver(): AcyclicObserver {
  return {
    onOperation({ family, op, durationMs, ...detail }) {
      const end = performance.now();
      performance.measure(`acyclic.${family}.${op}`, { start: end - durationMs, end, detail });
    },
  };
}

/** `observer`, else `performanceObserver()` when `ACYCLIC_PERF=1` (Node/Bun) or `globalThis.ACYCLIC_PERF === true`. */
export function resolveObserver(observer?: AcyclicObserver): AcyclicObserver | undefined {
  const scope = globalThis as { ACYCLIC_PERF?: unknown; process?: { env?: Record<string, string | undefined> } };
  return observer ?? (scope.ACYCLIC_PERF === true || scope.process?.env?.ACYCLIC_PERF === "1" ? performanceObserver() : undefined);
}

/** Reports one event for `run`; without an observer it returns `run()` untouched. */
export function observed<T>(observer: AcyclicObserver | undefined, family: string, op: string, run: (sizes?: OperationSizes) => Promise<T>): Promise<T> {
  if (observer === undefined) return run();
  const sizes: OperationSizes = {};
  const start = performance.now();
  const report = (ok: boolean, outcome: unknown) => {
    const { code, status, work } = (typeof outcome === "object" && outcome !== null ? outcome : {}) as { code?: unknown; status?: unknown; work?: unknown };
    const failure = ok ? undefined : (typeof code === "string" || typeof code === "number") && code !== "" && code !== 0 ? code : typeof status === "number" ? status : "unknown";
    observer.onOperation({ family, op, durationMs: performance.now() - start, ok, ...sizes, ...(failure === undefined ? {} : { code: failure }), ...(work === undefined ? {} : { work }) });
  };
  return run(sizes).then(value => { report(true, value); return value; }, (error: unknown) => { report(false, error); throw error; });
}

/** `interceptors` itself without an observer; otherwise a copy ending in one timing interceptor. */
export function observeInterceptors<Request extends { readonly method: { readonly localName: string } }, Response>(
  interceptors: ((next: (request: Request) => Promise<Response>) => (request: Request) => Promise<Response>)[],
  observer: AcyclicObserver | undefined,
  family: string,
) {
  return observer === undefined ? interceptors : [...interceptors, (next: (request: Request) => Promise<Response>) => (request: Request) => observed(observer, family, request.method.localName, () => next(request))];
}
