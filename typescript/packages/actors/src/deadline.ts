import type { Transport } from "@connectrpc/connect";

/** One absolute deadline, split only at the host timer's signed 32-bit boundary. */
export function deadlineSignal(timeoutMs: number | bigint | undefined, expired: () => unknown, parent?: AbortSignal) {
  if (typeof timeoutMs === "number" && !Number.isFinite(timeoutMs)) throw new RangeError("timeoutMs must be finite");
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  const cleanup = () => { clearTimeout(timer); parent?.removeEventListener("abort", abort); };
  const abort = () => { cleanup(); controller.abort(parent?.reason); };
  parent?.addEventListener("abort", abort, { once: true });
  if (parent?.aborted) abort();
  if (!controller.signal.aborted && timeoutMs !== undefined && timeoutMs > 0) {
    const end = typeof timeoutMs === "bigint"
      ? BigInt(Math.ceil(performance.now())) + timeoutMs : performance.now() + timeoutMs;
    const arm = () => {
      const remaining = typeof end === "bigint"
        ? end - BigInt(Math.floor(performance.now())) : end - performance.now();
      if (remaining <= 0) { cleanup(); controller.abort(expired()); }
      else timer = setTimeout(arm, remaining > 0x7fff_ffff ? 0x7fff_ffff : Number(remaining));
    };
    arm();
  }
  return { signal: controller.signal, cleanup: () => { cleanup(); controller.abort(); } };
}

function deadlineHeader(header: HeadersInit | undefined, timeoutMs: number | undefined): Headers {
  const result = new Headers(header);
  if (timeoutMs !== undefined && timeoutMs > 0) {
    // gRPC carries at most eight digits. Round up for the remote owner; the
    // local monotonic deadline retains the caller's exact duration.
    const units = [[1, "m"], [1000, "S"], [60_000, "M"], [3_600_000, "H"]] as const;
    result.delete("grpc-timeout");
    for (const [scale, unit] of units) {
      const value = Math.ceil(timeoutMs / scale);
      if (value <= 99_999_999) { result.set("grpc-timeout", `${value}${unit}`); break; }
    }
  }
  return result;
}

/** Own deadlines outside transports whose host timer overflows long durations. */
export function grpcDeadlines(transport: Transport, expired: () => unknown, defaultTimeoutMs?: number): Transport {
  return {
    async unary(method, signal, timeoutMs, header, input, contextValues) {
      timeoutMs ??= defaultTimeoutMs;
      const deadline = deadlineSignal(timeoutMs, expired, signal);
      try {
        return await transport.unary(method, deadline.signal, 0, deadlineHeader(header, timeoutMs), input, contextValues);
      } finally { deadline.cleanup(); }
    },
    async stream(method, signal, timeoutMs, header, input, contextValues) {
      timeoutMs ??= defaultTimeoutMs;
      const deadline = deadlineSignal(timeoutMs, expired, signal);
      try {
        const response = await transport.stream(method, deadline.signal, 0, deadlineHeader(header, timeoutMs), input, contextValues);
        return { ...response, message: (async function* () {
          try { yield* response.message; }
          finally { deadline.cleanup(); }
        })() };
      } catch (error) { deadline.cleanup(); throw error; }
    },
  };
}
