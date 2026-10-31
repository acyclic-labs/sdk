/** Two bounded observations emitted only at actual model entry and abort. */
export class ModelLifecycle {
  private phase = 0;
  private claimed = false;
  private controller!: ReadableStreamDefaultController<Uint8Array>;
  private readonly events = new ReadableStream<Uint8Array>({
    start: controller => { this.controller = controller; },
  }, { highWaterMark: 2, size: chunk => chunk.byteLength });

  started() { this.emit(1); }
  aborted() { this.emit(2); }

  observe(): ReadableStream<Uint8Array> {
    if (this.claimed) throw new Error("STEP_FAILED model-observation: already claimed; recovery: use a fresh named conformance host");
    this.claimed = true;
    return this.events;
  }

  private emit(phase: number) {
    if (phase !== this.phase + 1) throw new Error("STEP_FAILED model-observation: invalid lifecycle order; recovery: bash scripts/qualify-cloudflare.sh");
    this.phase = phase;
    this.controller.enqueue(Uint8Array.of(phase));
    if (phase === 2) this.controller.close();
  }
}

/** A single stream replaces readiness probes. Each actual event has one bound;
 * missing entry/abort, premature EOF and invalid event order remain failures.
 */
export async function* modelObservations(stream: ReadableStream<Uint8Array>, deadlineMillis = 2000) {
  const reader = stream.getReader();
  let phase = 0;
  try {
    while (phase < 2) {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const timeout = new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("STEP_FAILED model-observation: missing lifecycle event; recovery: bash scripts/qualify-cloudflare.sh")), deadlineMillis);
      });
      let result: ReadableStreamReadResult<Uint8Array>;
      try { result = await Promise.race([reader.read(), timeout]); }
      finally { clearTimeout(timer); }
      if (result.done || result.value.byteLength === 0 || result.value.byteLength > 2 - phase) {
        throw new Error("STEP_FAILED model-observation: incomplete or oversized lifecycle; recovery: bash scripts/qualify-cloudflare.sh");
      }
      for (const event of result.value) {
        if (event !== phase + 1) throw new Error("STEP_FAILED model-observation: lifecycle order differs; recovery: bash scripts/qualify-cloudflare.sh");
        phase = event;
        yield phase === 1 ? "started" : "aborted";
      }
    }
  } finally {
    await reader.cancel();
    reader.releaseLock();
  }
}
