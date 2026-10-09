// Optional per-call observation for the Rust-backed Actors client.

/** One completed call. It never carries bodies, tokens, paths, or contents. */
export interface OperationEvent {
  readonly family: string;
  readonly op: string;
  readonly durationMs: number;
  readonly ok: boolean;
  readonly code?: string | number;
  readonly requestBytes?: number;
  readonly responseBytes?: number;
  readonly work?: unknown;
}

/** Receives one event per call. */
export interface AcyclicObserver {
  onOperation(event: OperationEvent): void;
}

export interface OperationSizes {
  requestBytes?: number;
  responseBytes?: number;
}

/** Records each call as an `acyclic.<family>.<op>` performance measure. */
export function performanceObserver(): AcyclicObserver {
  return {
    onOperation({ family, op, durationMs, ...detail }) {
      const end = performance.now();
      performance.measure(`acyclic.${family}.${op}`, {
        start: end - durationMs,
        end,
        detail,
      });
    },
  };
}

/** Resolves an explicit observer or the opt-in environment observer. */
export function resolveObserver(observer?: AcyclicObserver): AcyclicObserver | undefined {
  const scope = globalThis as {
    ACYCLIC_PERF?: unknown;
    process?: { env?: Record<string, string | undefined> };
  };
  return observer ?? (scope.ACYCLIC_PERF === true || scope.process?.env?.ACYCLIC_PERF === "1"
    ? performanceObserver()
    : undefined);
}

/** Reports one event per operation while leaving the unobserved path untouched. */
export function observed<T>(
  observer: AcyclicObserver | undefined,
  family: string,
  op: string,
  run: (sizes?: OperationSizes) => Promise<T>,
): Promise<T> {
  if (observer === undefined) return run();
  const sizes: OperationSizes = {};
  const start = performance.now();
  const report = (ok: boolean, outcome: unknown) => {
    const { code, status, work } = (typeof outcome === "object" && outcome !== null
      ? outcome
      : {}) as { code?: unknown; status?: unknown; work?: unknown };
    const failure = ok
      ? undefined
      : typeof code === "string" || typeof code === "number"
        ? code === "" || code === 0 ? "unknown" : code
        : typeof status === "number" ? status : "unknown";
    observer.onOperation({
      family,
      op,
      durationMs: performance.now() - start,
      ok,
      ...sizes,
      ...(failure === undefined ? {} : { code: failure }),
      ...(work === undefined ? {} : { work }),
    });
  };
  return run(sizes).then(
    value => {
      report(true, value);
      return value;
    },
    error => {
      report(false, error);
      throw error;
    },
  );
}
