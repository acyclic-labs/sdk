import { WasmStream, type WasmJournalStreamLimits } from "../generated/wasm/acyclic_harness_wasm.js";
import { WakeIndex, type EventStorage, type StreamChange, type Placement } from "./event-storage.js";

export type ProviderReply<T> = { value: T } | { code: string };
export interface ImmutableObjects {
  length(key: Uint8Array): Promise<number | null>;
  read(key: Uint8Array, maximum: bigint): Promise<Uint8Array | null>;
  put(keys: Uint8Array[], values: Uint8Array[]): Promise<void>;
}
/** Private provider RPC contract, independent of its transport. */
export interface StreamLedger {
  register(actor: string, paths: string[]): Promise<Placement>;
  dispatch(actor: string, operation: string, input: Uint8Array): Promise<ProviderReply<Uint8Array>>;
  open(operation: string, input: Uint8Array, preceding: Uint8Array): Promise<ProviderReply<ReadableStream<Uint8Array>>>;
  objectLength(key: Uint8Array): Promise<number | null>;
  objectRead(key: Uint8Array, maximum: bigint): Promise<Uint8Array | null>;
  objectPut(keys: Uint8Array[], values: Uint8Array[]): Promise<void>;
}

function failure(error: unknown): { code: string } {
  return { code: typeof error === "object" && error !== null && "code" in error
    && typeof error.code === "string" ? error.code : "unavailable" };
}

/** Shared event-owned ledger, including durable wake custody and cursor lifetime. */
export class EventStreamLedger implements StreamLedger {
  private readonly stream: Promise<WasmStream>;
  private serial: Promise<unknown> = Promise.resolve();
  private source = "";
  private readonly wakes: WakeIndex;

  constructor(private readonly storage: EventStorage, private readonly objects: ImmutableObjects,
    journal: (changed: (changes: readonly StreamChange[]) => void) => object,
    private readonly wake: (actor: string) => Promise<void>, limits: WasmJournalStreamLimits) {
    this.wakes = new WakeIndex(storage, limits.paths);
    this.stream = WasmStream.openJournal(journal(changes => this.wakes.changed(this.source, changes)), limits);
  }

  private exclusive<T>(operation: () => Promise<T>): Promise<T> {
    const next = this.serial.then(operation);
    this.serial = next.catch(() => undefined);
    return next;
  }

  register(actor: string, paths: string[]) { return this.wakes.register(actor, paths); }
  objectLength(key: Uint8Array) { return this.objects.length(key); }
  objectRead(key: Uint8Array, maximum: bigint) { return this.objects.read(key, maximum); }
  objectPut(keys: Uint8Array[], values: Uint8Array[]) { return this.objects.put(keys, values); }
  pending(): number { return this.wakes.pending().length; }

  dispatch(actor: string, operation: string, input: Uint8Array): Promise<ProviderReply<Uint8Array>> {
    // A wait parks until later writes apply, so it never holds the write order.
    if (operation === "wait_applied" || operation === "heads") return this.observe(operation, input);
    return this.exclusive(async () => {
      try {
        const mutation = operation === "append" || operation === "fork" || operation === "commit";
        if (mutation) await this.storage.alarm(Date.now());
        this.source = actor;
        try { return { value: await (await this.stream).dispatch(operation, input) }; }
        finally {
          this.source = "";
          if (mutation) await this.schedule();
        }
      } catch (error) { return failure(error); }
    });
  }

  private async observe(operation: string, input: Uint8Array): Promise<ProviderReply<Uint8Array>> {
    try { return { value: await (await this.stream).dispatch(operation, input) }; }
    catch (error) { return failure(error); }
  }

  open(operation: string, input: Uint8Array, preceding: Uint8Array): Promise<ProviderReply<ReadableStream<Uint8Array>>> {
    return this.exclusive(async () => {
      try {
        const cursor = await (await this.stream).openProviderCursor(operation, input, preceding);
        let closed = false;
        const close = () => { if (!closed) { closed = true; cursor.close(); cursor.free(); } };
        return { value: new ReadableStream<Uint8Array>({
          async pull(controller) {
            try {
              const value = await cursor.next();
              if (value === null) { controller.close(); close(); }
              else controller.enqueue(value);
            } catch (error) { controller.error(error); close(); }
          },
          cancel: close,
        }, { highWaterMark: 0 }) };
      } catch (error) { return failure(error); }
    });
  }

  private schedule(): Promise<void> {
    return this.storage.alarm(this.pending() === 0 ? undefined : Date.now());
  }

  async alarm(): Promise<void> {
    await this.storage.alarm(Date.now() + 1000);
    const failures: unknown[] = [];
    for (const [actor, generation] of this.wakes.pending()) {
      try {
        await this.wake(actor);
        this.wakes.acknowledge(actor, generation);
      } catch (error) { failures.push(error); }
    }
    await this.exclusive(async () => {
      await this.storage.flush();
      await this.schedule();
    });
    if (failures.length !== 0) {
      throw new AggregateError(failures,
        "STEP actor-wake delivery failed; recovery: retry the ledger alarm from its retained storage");
    }
  }
}

/** Convert any pull-based RPC stream into the canonical injected-provider port. */
export function streamLedgerPort(ledger: StreamLedger, actor: string) {
  return {
    dispatch: (operation: string, input: Uint8Array) => ledger.dispatch(actor, operation, input),
    async open(operation: string, input: Uint8Array, preceding: Uint8Array) {
      const reply = await ledger.open(operation, input, preceding);
      if ("code" in reply) return reply;
      const reader = reply.value.getReader();
      return { value: {
        async next() {
          try {
            const { value, done } = await reader.read();
            return { value: done ? null : value };
          } catch (error) { return failure(error); }
        },
        close() { void reader.cancel().catch(() => undefined); },
      } };
    },
  };
}
