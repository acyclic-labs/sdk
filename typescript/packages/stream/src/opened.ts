import type { SessionOpened, SessionSelection } from "../generated/proto/stream/v1/session_pb.js";
import { StreamError, type Record, type Revision } from "./types.js";

/** The original selection, available even when a read is empty or a follow is quiet. */
export interface StreamOpening {
  readonly snapshot: Revision;
  readonly selection: Readonly<Omit<SessionSelection, "$typeName" | "$unknown">>;
}

export interface OpenedSource<Value> {
  readonly opening: StreamOpening;
  readonly records: AsyncIterator<Record<Value>>;
  close(): void | Promise<void>;
}

/** One selected cursor. Closing also cancels an opening that is still pending. */
export class Opened<Value = Uint8Array> implements AsyncIterableIterator<Record<Value>>, AsyncDisposable {
  readonly opened: Promise<StreamOpening>;
  readonly #abort = new AbortController();
  readonly #source: Promise<OpenedSource<Value>>;
  #closing: Promise<void> | undefined;

  constructor(open: (signal: AbortSignal) => Promise<OpenedSource<Value>>, signal?: AbortSignal) {
    const abort = (): void => { this.#abort.abort(signal?.reason); void this.close().catch(() => {}); };
    if (signal?.aborted) this.#abort.abort(signal.reason);
    else signal?.addEventListener("abort", abort, { once: true });
    this.#source = Promise.resolve().then(() => {
      // Setup owns disposal of an already-created upstream cursor. It must
      // receive cancellation even when close wins before this queued turn.
      return open(this.#abort.signal);
    });
    this.opened = this.#source.then(source => source.opening);
    // Keep the original rejection observable through opened/next even when a
    // caller closes without awaiting setup.
    void this.opened.catch(() => {});
    void this.#source.catch(() => signal?.removeEventListener("abort", abort));
    this.#abort.signal.addEventListener("abort", () => signal?.removeEventListener("abort", abort), { once: true });
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<Record<Value>> { return this; }

  /** Retain the original opening and cursor while adapting a provider facade. */
  static defer<Value>(open: (signal: AbortSignal) => Promise<Opened<Value>>, signal?: AbortSignal): Opened<Value> {
    return new Opened(async signal => forwardOpening(await open(signal), signal, source => source), signal);
  }

  /** Transform records without replacing the selected receipt or its owner. */
  through<Next>(adapt: (records: AsyncIterable<Record<Value>>) => AsyncIterable<Record<Next>>): Opened<Next> {
    return new Opened(signal => forwardOpening(this, signal, source => adapt(source)[Symbol.asyncIterator]()));
  }

  map<Next>(project: (record: Record<Value>) => Record<Next>): Opened<Next> {
    return this.through(async function* (records) { for await (const record of records) yield project(record); });
  }

  async next(): Promise<IteratorResult<Record<Value>>> {
    if (this.#closing !== undefined) { await this.#closing; return { done: true, value: undefined }; }
    try {
      const source = await this.#source;
      if (this.#abort.signal.aborted) { await this.close(); return { done: true, value: undefined }; }
      const result = await source.records.next();
      if (result.done) {
        await this.close();
        return result;
      }
      return result;
    } catch (error) {
      const cancelled = this.#abort.signal.aborted;
      try { await this.close(); }
      catch (cleanup) {
        if (cleanup === error) throw error;
        throw new AggregateError([error, cleanup], "stream failed while closing its original cursor");
      }
      if (cancelled) return { done: true, value: undefined };
      throw error;
    }
  }

  async return(_value?: unknown): Promise<IteratorResult<Record<Value>>> {
    await this.close();
    return { done: true, value: undefined };
  }

  close(): Promise<void> {
    if (this.#closing === undefined) {
      this.#abort.abort();
      this.#closing = this.#source.then(async source => {
        let failed = false;
        let failure: unknown;
        try { await source.close(); } catch (error) { failed = true; failure = error; }
        try { await source.records.return?.(); }
        catch (error) {
          if (failed) throw new AggregateError([failure, error], "stream cursor and adapter failed while closing");
          throw error;
        }
        if (failed) throw failure;
      }, error => {
        // Only this owner's cancellation establishes a quiet close. Setup
        // failures retain their original cause, including failed disposal.
        if (error !== this.#abort.signal.reason) throw error;
      });
    }
    return this.#closing;
  }

  [Symbol.asyncDispose](): Promise<void> { return this.close(); }
}

async function forwardOpening<Value, Next>(
  source: Opened<Value>, signal: AbortSignal, records: (source: Opened<Value>) => AsyncIterator<Record<Next>>,
): Promise<OpenedSource<Next>> {
  const abort = (): void => { void source.close().catch(() => {}); };
  signal.addEventListener("abort", abort, { once: true });
  const close = async (): Promise<void> => {
    signal.removeEventListener("abort", abort);
    await source.close();
  };
  try {
    signal.throwIfAborted();
    return { opening: await source.opened, records: records(source), close };
  } catch (error) {
    try { await close(); }
    catch (cleanup) { throw new AggregateError([error, cleanup], "stream facade failed while closing its original cursor"); }
    throw error;
  }
}

/** Project metadata already admitted by Rust; never sample a later revision. */
export function streamOpening(opened: SessionOpened): StreamOpening {
  if (opened.applied === undefined || opened.selection === undefined) {
    throw new StreamError("invalid_response", "stream open omitted its selected revision or bounds");
  }
  return { snapshot: opened.applied, selection: opened.selection };
}
