/** Runtime materialization of the canonical Binding.stateTransaction kind.
 * The host binds the executing actor and epoch before entering guest code.
 * All selected names for this kind share that SAME owner and transaction group.
 * There is no caller-selected stream, account, credential or publication key.
 */
export interface ActorStateBinding {
  /** Synchronous, non-nesting callback. A thrown error rolls back only this
   * callback. Returning a Promise is refused; transaction handles expire at return.
   * No mutation is durable before flush resolves. */
  transaction<T>(operation: (state: ActorStateTransaction) => T extends PromiseLike<unknown> ? never : T): T;
  /** Original conditional commit of all pending metadata, journal appends and
   * alarm. Resolves only after every packet staged before the call is durable.
   * Packets commit strictly in order, one at a time; a transaction made while a
   * flush is in flight stages the next packet. Rejection/cancellation fences this
   * activation; the host retains any unknown publication and NEVER retries it
   * by re-entering the guest callback. */
  flush(): Promise<void>;
  /** Paged journal frames at the original committed actor cut, excluding staged
   * and in-flight writes and later owner epochs. A missing/corrupt frame rejects,
   * never EOF. */
  readJournal(from: bigint, limit: number): Promise<readonly Uint8Array[]>;
}

/** Borrowed synchronous view, valid ONLY during its transaction callback.
 * Binary input/output is copied at the guest boundary; no mutable alias can
 * change a staged or committed value after admission. */
export interface ActorStateTransaction {
  get(key: string): Uint8Array | undefined;
  put(key: string, value: Uint8Array): void;
  entries(): Iterable<readonly [string, Uint8Array]>;
  /** Exact retained plus staged frame count. */
  readonly journalTail: bigint;
  /** Frames use the existing canonical Stream journal format. The original
   * provider interprets them; Actors only retains the opaque admitted bytes. */
  appendJournal(expected: bigint, frames: readonly Uint8Array[]): void;
  /** Positive Unix-ms deadline, or null to clear. Delivery is GuestEvent.alarm
   * from the original Actor alarm owner, never a guest setTimeout. */
  alarm(deadlineUnixMs: bigint | null): void;
}

/** Host seam for the ONE admitted native ActorStateBinding. commit applies the
 * canonical packet to that same native binding and awaits its original flush.
 * Its stable publication identity, epoch and provider never enter guest code. */
export interface ActorStateHost {
  readonly seed: import("./generated/guest.js").ActorStateSeed;
  commit(request: import("./generated/guest.js").ActorStateCommit): Promise<void>;
  readJournal(from: bigint, limit: number): Promise<readonly Uint8Array[]>;
  /** Permanently revoke this activation on uncertain/failed publication. */
  fence(reason: unknown): void;
}

let activeTransaction = false;
const U64_MAX = (1n << 64n) - 1n;
function position(value: bigint): bigint {
  if (typeof value !== "bigint" || value < 0n || value > U64_MAX) throw new RangeError("invalid actor journal position");
  return value;
}
function copy(value: Uint8Array): Uint8Array {
  if (!(value instanceof Uint8Array)) throw new TypeError("actor state requires bytes");
  return new Uint8Array(value);
}

/** Same JavaScript projection on local and hosted runtimes. Storage decisions,
 * conditional commit and acknowledgement custody stay with the original native
 * producer. Reuse this object for every name selecting the same state kind. */
export function materializeActorState(host: ActorStateHost): ActorStateBinding {
  let metadata = new Map(host.seed.metadata.map(({key, value}) => [key, copy(value)]));
  // durable: acknowledged journal cut. staged: the cut after every dispatched packet.
  let durable = position(BigInt(host.seed.journalTail)), staged = durable;
  let writes = new Map<string, Uint8Array>();
  let journal: Uint8Array[] = [];
  let alarm: import("./generated/guest.js").ActorStateAlarm = {type:"keep"};
  let pending = false, fenced = false;
  let committing: Promise<void> = Promise.resolve();
  const usable = () => { if (fenced) throw new Error("actor state binding fenced"); };
  return Object.freeze({
    transaction<T>(operation: (state: ActorStateTransaction) => T extends PromiseLike<unknown> ? never : T): T {
      usable();
      if (activeTransaction) throw new Error("nested actor state transaction");
      activeTransaction = true;
      let live = true;
      const scoped = () => { usable(); if (!live) throw new Error("actor state transaction expired"); };
      // Copy on first write: a read-only transaction copies nothing and stages no packet.
      let candidate: Map<string, Uint8Array> | undefined, changed: Map<string, Uint8Array> | undefined;
      let frames: Uint8Array[] | undefined, nextAlarm: import("./generated/guest.js").ActorStateAlarm | undefined;
      const state: ActorStateTransaction = Object.freeze({
        get(key: string) { scoped(); const value = (candidate ?? metadata).get(key); return value === undefined ? undefined : copy(value); },
        put(key: string, value: Uint8Array) { scoped(); if (typeof key !== "string") throw new TypeError("actor state key must be a string"); const retained = copy(value); (candidate ??= new Map(metadata)).set(key, retained); (changed ??= new Map(writes)).set(key, retained); },
        *entries() { scoped(); for (const [key,value] of candidate ?? metadata) { scoped(); yield [key,copy(value)] as const; } },
        get journalTail() { scoped(); return staged + BigInt((frames ?? journal).length); },
        appendJournal(expected: bigint, appended: readonly Uint8Array[]) {
          scoped();
          if (position(expected) !== staged + BigInt((frames ?? journal).length)) throw new Error("actor journal conflict");
          if (appended.length === 0 || expected + BigInt(appended.length) > U64_MAX) throw new RangeError("invalid actor journal append");
          const owned = appended.map(copy);
          frames ??= journal.slice();
          for (const frame of owned) frames.push(frame);
        },
        alarm(deadline: bigint | null) { scoped(); if (deadline !== null && position(deadline) === 0n) throw new RangeError("actor alarm deadline must be positive"); nextAlarm = deadline === null ? {type:"clear"} : {type:"set",deadline:deadline.toString()}; },
      });
      try {
        const value = operation(state);
        if (value !== null && (typeof value === "object" || typeof value === "function") && "then" in value) throw new TypeError("actor state transaction must finish synchronously");
        if (candidate && changed) { metadata = candidate; writes = changed; }
        if (frames) journal = frames;
        if (nextAlarm) alarm = nextAlarm;
        pending ||= candidate !== undefined || frames !== undefined || nextAlarm !== undefined;
        return value;
      } finally { live = false; activeTransaction = false; }
    },
    async flush(): Promise<void> {
      usable();
      if (activeTransaction) throw new Error("flush inside actor state transaction");
      if (pending) {
        const request = {writes:Array.from(writes, ([key,value]) => ({key,value})), journalFrom:staged.toString(), journal, alarm};
        const appended = BigInt(journal.length), previous = committing;
        staged += appended; writes = new Map(); journal = []; alarm = {type:"keep"}; pending = false;
        // A packet dispatches only after its predecessor is durable; a failed
        // predecessor rejects every successor with its original cause, undispatched.
        committing = (async () => {
          await previous;
          try { await host.commit(request); }
          catch (error) { fenced = true; host.fence(error); throw error; }
          durable += appended;
        })();
      }
      await committing;
    },
    async readJournal(from: bigint, limit: number): Promise<readonly Uint8Array[]> {
      usable(); position(from);
      if (!Number.isSafeInteger(limit) || limit <= 0 || limit > 0xffff_ffff || from > durable) throw new RangeError("invalid actor journal page");
      return (await host.readJournal(from, limit)).map(copy);
    },
  });
}
