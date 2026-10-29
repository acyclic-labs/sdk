import type { EventStorage, StreamChange } from "./event-storage.js";

/** Structural view of the public Actors `ActorStateBinding` (Binding.stateTransaction). */
export interface ActorState {
  transaction<T>(operation: (state: ActorStateView) => T): T;
  flush(): Promise<void>;
  readJournal(from: bigint, limit: number): Promise<readonly Uint8Array[]>;
}
export interface ActorStateView {
  get(key: string): Uint8Array | undefined;
  put(key: string, value: Uint8Array): void;
  entries(): Iterable<readonly [string, Uint8Array]>;
  readonly journalTail: bigint;
  appendJournal(expected: bigint, frames: readonly Uint8Array[]): void;
  alarm(deadlineUnixMs: bigint | null): void;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });
const CONFIGURATION = "journal/configuration", AUTHORITY = "journal/authority", BYTES = "journal/bytes";
/** Every domain's outstanding deadline, committed with the actor alarm it sets. */
const ALARMS = "alarms";

function equal(a: Uint8Array, b: Uint8Array): boolean {
  return a.length === b.length && a.every((byte, index) => byte === b[index]);
}

/**
 * Event-host metadata and the ledger journal over ONE executing-actor state
 * binding. Metadata, wake rows, journal frames and the alarm commit together:
 * every storage call inside an open transaction joins that same transaction.
 * Journal history stays in the actor's journal, paged; metadata holds only its
 * configuration, authority and byte total.
 */
export class ActorStateStorage {
  private scope: ActorStateView | undefined;
  private alarms = new Map<string, number>();
  private durable = 0;
  private bytes = 0;

  constructor(private readonly state: ActorState) {
    this.run(view => {
      this.durable = Number(view.journalTail);
      const total = view.get(BYTES);
      this.bytes = total === undefined ? 0 : Number(decoder.decode(total));
      const alarms = view.get(ALARMS);
      if (alarms !== undefined) this.alarms = new Map(Object.entries(JSON.parse(decoder.decode(alarms)) as Record<string, number>));
    });
  }

  private run<T>(operation: (view: ActorStateView) => T): T {
    if (this.scope) return operation(this.scope);
    return this.state.transaction(view => {
      this.scope = view;
      try { return operation(view); } finally { this.scope = undefined; }
    });
  }

  /** One prefixed metadata domain. The single actor alarm is the earliest any domain requested. */
  domain(prefix: string): EventStorage {
    const key = (name: string) => `${prefix}/${name}`;
    return {
      get: name => {
        const value = this.run(view => view.get(key(name)));
        return value === undefined ? undefined : decoder.decode(value);
      },
      put: (name, value) => this.run(view => view.put(key(name), encoder.encode(value))),
      entries: () => this.run(view => Array.from(view.entries()).flatMap(([name, value]): [string, string][] =>
        name.startsWith(`${prefix}/`) ? [[name.slice(prefix.length + 1), decoder.decode(value)]] : [])),
      transaction: operation => this.run(() => operation()),
      flush: () => this.state.flush(),
      alarm: time => {
        const alarms = new Map(this.alarms);
        if (time === undefined) alarms.delete(prefix); else alarms.set(prefix, time);
        const earliest = alarms.size === 0 ? null : BigInt(Math.max(1, Math.trunc(Math.min(...alarms.values()))));
        this.run(view => {
          view.put(ALARMS, encoder.encode(JSON.stringify(Object.fromEntries(alarms))));
          view.alarm(earliest);
        });
        this.alarms = alarms;
        return this.state.flush();
      },
    };
  }

  /** The original EventStreamLedger journal port for this actor's one journal. */
  journal(appended?: (changes: readonly StreamChange[]) => void) {
    const subscribers = new Set<{ after: number; ready: () => void }>();
    let maximumFrameBytes = 0;
    return {
      configure: async (configuration: Uint8Array, maximum: number, authority: Uint8Array): Promise<Uint8Array> => {
        maximumFrameBytes = maximum;
        const retained = this.run(view => {
          const existing = view.get(CONFIGURATION);
          if (existing === undefined) {
            view.put(CONFIGURATION, configuration);
            view.put(AUTHORITY, authority);
            return Uint8Array.from(authority);
          }
          if (!equal(existing, configuration)) {
            throw new Error("STEP actor-journal-open failed: journal configuration differs; recovery: reopen with the original stream limits");
          }
          return view.get(AUTHORITY)!;
        });
        await this.state.flush();
        return retained;
      },
      flush: () => this.state.flush(),
      read: async (from: number, page: number): Promise<Uint8Array[]> => {
        const head = this.durable;
        if (!Number.isSafeInteger(from) || !Number.isSafeInteger(page) || from < 0 || page <= 0 || from > head) {
          throw new Error("STEP actor-journal-read failed: invalid page; recovery: replay from the retained cursor");
        }
        const header = new Uint8Array(16);
        const view = new DataView(header.buffer);
        view.setBigUint64(0, BigInt(head), true);
        view.setBigUint64(8, BigInt(this.bytes), true);
        const count = Math.min(page, head - from);
        return [header, ...(count === 0 ? [] : await this.state.readJournal(BigInt(from), count))];
      },
      changed: (after: number, ready: () => void): (() => void) => {
        if (this.durable > after) { ready(); return () => {}; }
        const subscriber = { after, ready };
        subscribers.add(subscriber);
        return () => { subscribers.delete(subscriber); };
      },
      append: async (index: number, admit: () => Uint8Array[], changes: readonly StreamChange[]): Promise<number> => {
        const accepted = this.run(view => {
          const tail = Number(view.journalTail);
          if (tail !== index) return undefined;
          const frames = admit();
          if (frames.length === 0) throw new Error("STEP actor-journal-append failed: empty batch; recovery: submit the original nonempty command batch");
          let total = Number(decoder.decode(view.get(BYTES) ?? encoder.encode("0")));
          for (const frame of frames) {
            if (frame.length < 18 || frame.length > maximumFrameBytes) {
              throw new Error("STEP actor-journal-append failed: invalid frame; recovery: retry the exact original Stream request");
            }
            total += frame.length;
          }
          view.appendJournal(BigInt(index), frames);
          view.put(BYTES, encoder.encode(String(total)));
          appended?.(changes);
          return { head: index + frames.length, total };
        });
        if (accepted === undefined) return this.run(view => Number(view.journalTail));
        await this.state.flush();
        if (accepted.head > this.durable) { this.durable = accepted.head; this.bytes = accepted.total; }
        for (const subscriber of subscribers) {
          if (this.durable > subscriber.after) { subscribers.delete(subscriber); subscriber.ready(); }
        }
        return accepted.head;
      },
    };
  }
}
