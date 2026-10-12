import { type Authority, type OperationId } from "./index.js";
import { type FileRef, type ReferencedAttachments } from "./conversation.js";
import { type ClientCheckpoint, type OutboxStore, MemoryOutbox, type CursorStore, MemoryCursorStore, type AtomicClientStateStore, ClientCheckpointConflict } from "./client-store.js";
import { SnapshotStore } from "./client-snapshot.js";
import { RequestScheduler, requestBound, abortableWait } from "./client-requests.js";
import { retainedDataBytes, assertOutboxSafe, cloneStructuredValue } from "./client-data.js";

export interface ReplayCursor {
  readonly generation: string;
  readonly revision: bigint;
}
export interface ClientEvent<Event = unknown> {
  readonly authority: Authority;
  readonly revision: bigint;
  readonly operationId: OperationId;
  readonly event: Event;
}
export interface Delivery<Event = unknown> {
  readonly authority: Authority;
  readonly generation: string;
  readonly fromRevision: bigint;
  readonly throughRevision: bigint;
  readonly events: readonly ClientEvent<Event>[];
  readonly live: boolean;
}
export interface OfflinePayload {
  readonly content?: FileRef;
  readonly attachments?: ReferencedAttachments;
  readonly artifacts?: readonly FileRef[];
  readonly references?: readonly FileRef[];
  /** Numeric/boolean routing hints only; free-form strings could persist credentials. */
  readonly metadata?: Readonly<Record<string, number | boolean | bigint | null>>;
}
export type ClientCommand = Readonly<{ operationId: OperationId; authority: Authority; kind: string }> & (
  | Readonly<{ payload: OfflinePayload; offlineSafe: true }>
  | Readonly<{ payload: unknown; offlineSafe?: false }>
);
export interface Connection<Event = unknown> extends AsyncIterable<Delivery<Event>> {
  send(command: ClientCommand): Promise<void>;
  close(): Promise<void> | void;
}
export interface Transport<Event = unknown> {
  connect(cursors: ReadonlyMap<string, ReplayCursor>, signal?: AbortSignal): Promise<Connection<Event>>;
}
export interface ProjectionRecovery<State> {
  readonly authority: Authority;
  /** Includes reducer, schema, adapter and security namespace identities. */
  readonly identity: string;
  /** Pure bounded encoder; large content remains immutable references. */
  encode(state: State): unknown;
  /** Validate data and its exact authority/generation/revision before restoring. */
  decode(state: unknown, cursor: ReplayCursor): State;
}
interface RecoveryProjection<Event> {
  checkpoint(): ClientCheckpoint;
  prepare(event: ClientEvent<Event>): { checkpoint: ClientCheckpoint; publish(): void };
  restore(checkpoint: ClientCheckpoint, cursor: ReplayCursor): () => void;
}
export interface ClientAdmissionLimits {
  /** Includes the running mutation and callers waiting for it. */
  readonly requests: number;
  readonly bytes: number;
  readonly commandBytes: number;
}

export class ProjectionStore<State, Event = unknown> {
  readonly #store: SnapshotStore<State>;
  #unsubscribe: (() => void) | undefined;
  #disposed = false;

  constructor(readonly client: Pick<HarnessClient<Event>, "subscribe"> & Partial<Pick<HarnessClient<Event>, "registerProjection">>, initial: State,
    readonly reduce: (state: State, event: ClientEvent<Event>) => State,
    readonly recovery?: ProjectionRecovery<State>) {
    this.#store = new SnapshotStore(initial);
  }

  /** Explicitly attach before delivering events. Construction/SSR start no effects. */
  start(): void {
    if (this.#disposed) throw new Error("projection store is disposed");
    if (this.#unsubscribe !== undefined) return;
    if (this.recovery !== undefined) {
      const recovery = this.recovery;
      if (this.client.registerProjection === undefined) throw new Error("client does not support projection recovery");
      this.#unsubscribe = this.client.registerProjection(recovery.authority, {
        checkpoint: () => ({ identity: recovery.identity, state: recovery.encode(this.#store.getSnapshot()) }),
        prepare: event => {
          const state = this.reduce(this.#store.getSnapshot(), event);
          return { checkpoint: { identity: recovery.identity, state: recovery.encode(state) },
            publish: () => this.#store.publish(state) };
        },
        restore: (checkpoint, cursor) => {
          if (checkpoint.identity !== recovery.identity) throw new Error("projection checkpoint identity mismatch");
          const state = recovery.decode(checkpoint.state, cursor);
          return () => this.#store.publish(state);
        },
      });
    } else {
      this.#unsubscribe = this.client.subscribe(event => {
        this.#store.publish(this.reduce(this.#store.getSnapshot(), event));
      });
    }
  }

  /** React-compatible immutable snapshot accessor. */
  get getSnapshot() { return this.#store.getSnapshot; }

  /** Svelte/React-compatible subscription. */
  get subscribe() { return this.#store.subscribe; }

  /** Detaches the store from its client. */
  dispose(): void {
    if (this.#disposed) return;
    this.#unsubscribe?.();
    this.#disposed = true;
    this.#unsubscribe = undefined;
    this.#store.dispose();
  }
}

export class HarnessClient<Event = unknown> {
  readonly #cuts = new Map<string, { cursor?: ReplayCursor; checkpointVersion?: bigint }>();
  readonly #listeners = new Set<(event: ClientEvent<Event>) => void>();
  readonly #errors = new SnapshotStore<unknown | null>(null);
  /** Latest replay/reconnect failure; retry does not hide it from the host. */
  readonly failure = { getSnapshot: this.#errors.getSnapshot, subscribe: this.#errors.subscribe };
  readonly #projections = new Map<string, RecoveryProjection<Event>>();
  #connection: Connection<Event> | undefined;
  #cursorsLoaded = false;
  readonly #mutations: RequestScheduler;
  readonly admission: ClientAdmissionLimits;
  #replayEpoch = 0;
  #disposed = false;
  #runController: AbortController | undefined;

  constructor(
    readonly transport: Transport<Event>,
    readonly outbox: OutboxStore = new MemoryOutbox(),
    readonly cursorStore: CursorStore = isCursorStore(outbox) ? outbox : new MemoryCursorStore(),
    admission: ClientAdmissionLimits = { requests: 128, bytes: 16 * 1024 * 1024, commandBytes: 1024 * 1024 },
  ) {
    requestBound(admission.commandBytes, "commandBytes");
    this.admission = Object.freeze({ ...admission });
    this.#mutations = new RequestScheduler({ concurrent: 1, requests: admission.requests, bytes: admission.bytes });
  }

  /** Registers at-least-once projection delivery; callbacks must be idempotent. */
  subscribe(listener: (event: ClientEvent<Event>) => void): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  /** Attach all durable projections before the first replay/rebase. */
  registerProjection(authority: Authority, projection: RecoveryProjection<Event>): () => void {
    if (this.#cursorsLoaded) throw new Error("register projections before client recovery");
    if (!isAtomicClientStateStore(this.outbox) || this.outbox !== this.cursorStore) {
      throw new Error("durable projection requires one atomic recovery store");
    }
    const key = authorityKey(authority);
    if (this.#projections.has(key)) throw new Error("duplicate durable projection authority");
    if (this.#projections.size >= 64) throw new RangeError("projection capacity exceeded");
    this.#projections.set(key, projection);
    return () => {
      if (this.#runController !== undefined && !this.#disposed) throw new Error("stop replay before disposing its durable projection");
      this.#projections.delete(key);
      this.#cursorsLoaded = false;
      this.#cuts.clear();
    };
  }

  async submit(input: ClientCommand): Promise<void> {
    const bytes = retainedDataBytes(input, this.admission.commandBytes, true);
    let command!: ClientCommand;
    await this.#mutations.schedule(bytes, async () => {
      const safelyReplayable = command.offlineSafe && command.kind !== "interaction.resolve.approval";
      const admitted = safelyReplayable ? await assertOutboxSafe(command) : command;
      if (this.#connection !== undefined) {
        if (safelyReplayable) await this.outbox.put(admitted);
        try {
          return await this.#connection.send(admitted);
        } catch (error) {
          if (safelyReplayable && error instanceof TerminalAdmissionError) {
            await this.outbox.delete(admitted.operationId);
          }
          throw error;
        }
      }
      if (!safelyReplayable) {
        throw new Error("command is not safe for the offline outbox");
      }
      await this.outbox.put(admitted);
    }, undefined, () => { command = cloneStructuredValue(input, new Set(), true) as ClientCommand; });
  }

  /** Installs an authoritative snapshot cursor after synchronously resetting projections. */
  async rebase(authority: Authority, cursor: ReplayCursor, resetProjections: (() => void) | ClientCheckpoint): Promise<void> {
    validateCursor(cursor);
    const bytes = retainedDataBytes({ authority, cursor,
      checkpoint: typeof resetProjections === "function" ? null : resetProjections }, this.admission.commandBytes, true);
    let connection: Connection<Event> | undefined;
    try {
      await this.#mutations.schedule(bytes, async () => {
        const recovered = await this.#hydrateCursors(typeof resetProjections === "function" ? undefined : authorityKey(authority));
        this.#replayEpoch += 1;
        connection = this.#connection;
        this.#connection = undefined;
        const key = authorityKey(authority);
        const previous = this.#cuts.get(key)?.cursor ?? recovered;
        const projection = this.#projections.get(key);
        if (projection !== undefined) {
          if (typeof resetProjections === "function") throw new Error("durable rebase requires an authoritative checkpoint");
          const publish = projection.restore(resetProjections, cursor);
          const version = await (this.outbox as AtomicClientStateStore).commit(authority, cursor, undefined,
            resetProjections, previous ?? null, this.#cuts.get(key)?.checkpointVersion ?? null);
          this.#cuts.set(key, { cursor, checkpointVersion: version });
          this.#cursorsLoaded = true;
          publish();
          return;
        }
        if (isAtomicClientStateStore(this.outbox) && this.outbox === this.cursorStore) {
          throw new Error("durable rebase requires a registered projection");
        }
        if (typeof resetProjections !== "function") throw new Error("checkpoint has no registered projection");
        await this.cursorStore.putCursor(authority, cursor);
        try {
          resetProjections();
        } catch (error) {
          if (previous === undefined) await this.cursorStore.deleteCursor(authority);
          else await this.cursorStore.putCursor(authority, previous);
          throw error;
        }
        this.#cuts.set(key, { ...this.#cuts.get(key), cursor });
      }, undefined, () => {
        authority = { ...authority };
        cursor = { ...cursor };
        if (typeof resetProjections !== "function") resetProjections = cloneStructuredValue(resetProjections, new Set(), true) as ClientCheckpoint;
      });
    } finally {
      await connection?.close();
    }
  }

  /** Runs reconnect/replay until aborted; transport failures use bounded exponential backoff. */
  async run(signal?: AbortSignal): Promise<void> {
    if (this.#disposed) throw new Error("client is disposed");
    if (this.#runController !== undefined) throw new Error("client replay is already running");
    const controller = new AbortController();
    this.#runController = controller;
    try { await this.#run(signal === undefined ? controller.signal : AbortSignal.any([signal, controller.signal])); }
    finally { this.#runController = undefined; }
  }

  /** Cancel interest; started storage/send reservations last until settlement. */
  dispose(): void {
    if (this.#disposed) return;
    this.#disposed = true;
    this.#runController?.abort();
    this.#mutations.dispose();
    this.#listeners.clear();
    this.#errors.dispose();
  }

  async #run(signal: AbortSignal): Promise<void> {
    await this.#mutations.schedule(256, () => this.#hydrateCursors());
    let delay = 50;
    while (!signal.aborted) {
      let unreceived: Connection<Event> | undefined;
      try {
        const { cursors, epoch } = await this.#mutations.schedule(256, async () => {
          const cursors = new Map<string, ReplayCursor>();
          for (const [key, cut] of this.#cuts) {
            if (cut.cursor !== undefined) cursors.set(key, cut.cursor);
          }
          return { cursors, epoch: this.#replayEpoch };
        });
        const connection = await this.transport.connect(cursors, signal);
        unreceived = connection;
        if (signal.aborted) throw new DOMException("Client replay cancelled", "AbortError");
        const current = await this.#mutations.schedule(256, async () => {
          if (epoch !== this.#replayEpoch) return false;
          this.#connection = connection;
          return true;
        });
        if (!current) {
          await connection.close();
          unreceived = undefined;
          throw new Error();
        }
        unreceived = undefined;
        await this.#flushOutbox(connection, epoch, signal);
        for await (const delivery of connection) {
          if (signal.aborted) break;
          await this.#accept(delivery, epoch);
        }
        delay = 50;
      } catch (error) {
        this.#errors.publish(error);
        if (signal.aborted) break;
        if (error instanceof ClientCheckpointConflict) throw error;
        if (error instanceof ReplayError) {
          if (this.#projections.has(authorityKey(error.authority))) throw error;
          await this.#mutations.schedule(256, async () => {
            await this.cursorStore.deleteCursor(error.authority);
            const key = authorityKey(error.authority);
            const cut = this.#cuts.get(key);
            if (cut?.checkpointVersion === undefined) this.#cuts.delete(key);
            else delete cut.cursor;
          });
        }
        await abortableWait(delay, signal);
        delay = Math.min(delay * 2, 5_000);
      } finally {
        const connection = this.#connection ?? unreceived;
        this.#connection = undefined;
        await connection?.close();
      }
    }
  }

  async #flushOutbox(connection: Connection<Event>, epoch: number, signal: AbortSignal): Promise<void> {
    let after = 0;
    let through: number | undefined;
    do {
      if (signal.aborted) return;
      const page = this.outbox.loadPage === undefined ? undefined : await this.outbox.loadPage(after, through);
      const commands = page?.commands ?? await this.outbox.load();
      for (const command of commands) {
        await this.#mutations.schedule(256, async () => {
          if (signal.aborted) throw new DOMException("Client replay cancelled", "AbortError");
          if (epoch !== this.#replayEpoch || connection !== this.#connection) {
            throw new Error();
          }
          try {
            await connection.send(command);
          } catch (error) {
            if (error instanceof TerminalAdmissionError) {
              await this.outbox.delete(command.operationId);
              return;
            }
            throw error;
          }
        });
      }
      if (page === undefined) return;
      after = page.afterSequence;
      through = page.throughSequence;
    } while (after < through);
  }

  async #accept(delivery: Delivery<Event>, epoch: number): Promise<void> {
    const bytes = retainedDataBytes(delivery, this.admission.commandBytes, true);
    await this.#mutations.schedule(bytes, async () => {
      if (epoch !== this.#replayEpoch) throw new Error();
      const key = authorityKey(delivery.authority);
      const projection = this.#projections.get(key);
      if (isAtomicClientStateStore(this.outbox) && this.outbox === this.cursorStore && projection === undefined) {
        throw new ClientCheckpointConflict("durable delivery requires a registered recoverable projection");
      }
      const previous = this.#cuts.get(key)?.cursor;
      const expected = previous?.revision ?? 0n;
      if (previous !== undefined && previous.generation !== delivery.generation) {
        throw new ReplayError(delivery.authority, "replay generation changed");
      }
      if (delivery.fromRevision !== expected || delivery.throughRevision < delivery.fromRevision) {
        throw new ReplayError(delivery.authority, "non-contiguous delivery");
      }
      let revision = expected;
      for (const event of delivery.events) {
        revision += 1n;
        if (authorityKey(event.authority) !== key || event.revision !== revision) {
          throw new ReplayError(delivery.authority, "event authority or revision mismatch");
        }
      }
      if (revision !== delivery.throughRevision) {
        throw new ReplayError(delivery.authority, "delivery coverage mismatch");
      }
      for (const event of delivery.events) {
        if (this.#disposed) return;
        const cursor = { generation: delivery.generation, revision: event.revision };
        const prepared = projection?.prepare(event);
        // Listeners run before commit; throwing leaves the event retryable.
        for (const listener of this.#listeners) listener(event);
        const cut = { ...this.#cuts.get(key), cursor };
        if (isAtomicClientStateStore(this.outbox) && this.outbox === this.cursorStore) {
          cut.checkpointVersion = await this.outbox.commit(delivery.authority, cursor, event.operationId, prepared?.checkpoint,
            prepared === undefined ? undefined : this.#cuts.get(key)?.cursor ?? null,
            prepared === undefined ? undefined : this.#cuts.get(key)?.checkpointVersion ?? null);
        } else {
          await this.outbox.delete(event.operationId);
          await this.cursorStore.putCursor(delivery.authority, cursor);
        }
        this.#cuts.set(key, cut);
        if (this.#disposed) return;
        prepared?.publish();
      }
      if (delivery.events.length === 0) {
        const cursor = { generation: delivery.generation, revision };
        const cut = { ...this.#cuts.get(key), cursor };
        // Empty delivery cannot erase the checkpoint paired with an unchanged cursor.
        if (previous?.generation !== cursor.generation || previous.revision !== cursor.revision) {
          const projection = this.#projections.get(key);
          if (projection !== undefined) {
            cut.checkpointVersion = await (this.outbox as AtomicClientStateStore).commit(delivery.authority, cursor, undefined,
              projection.checkpoint(), previous ?? null, this.#cuts.get(key)?.checkpointVersion ?? null);
          } else await this.cursorStore.putCursor(delivery.authority, cursor);
        }
        this.#cuts.set(key, cut);
      }
    }, undefined, () => { delivery = cloneStructuredValue(delivery, new Set(), true) as Delivery<Event>; });
  }

  async #hydrateCursors(replacedAuthority?: string): Promise<ReplayCursor | undefined> {
    if (this.#cursorsLoaded) return replacedAuthority === undefined ? undefined : this.#cuts.get(replacedAuthority)?.cursor;
    if (isAtomicClientStateStore(this.outbox) && this.outbox === this.cursorStore) {
      const records = await this.outbox.loadRecovery();
      const publications: (() => void)[] = [];
      for (const record of records) {
        if (record.authority === replacedAuthority) continue;
        const projection = this.#projections.get(record.authority);
        if (projection === undefined || record.checkpoint === undefined) throw new Error("cursor has no matching recoverable projection");
        publications.push(projection.restore(record.checkpoint, record.cursor));
      }
      for (const publish of publications) publish();
      for (const record of records) {
        this.#cuts.set(record.authority, {
          ...this.#cuts.get(record.authority),
          checkpointVersion: record.checkpointVersion,
          ...(record.authority === replacedAuthority ? {} : { cursor: record.cursor }),
        });
      }
      // A replacement cut becomes resumable only after its checkpoint commits.
      this.#cursorsLoaded = replacedAuthority === undefined;
      return records.find(record => record.authority === replacedAuthority)?.cursor;
    }
    for (const [key, cursor] of await this.cursorStore.loadCursors()) {
      if (this.#cuts.get(key)?.cursor === undefined) this.#cuts.set(key, { ...this.#cuts.get(key), cursor });
    }
    this.#cursorsLoaded = true;
  }

}

export function validateCursor(cursor: ReplayCursor): void {
  if (typeof cursor.generation !== "string" || cursor.generation === "" ||
      typeof cursor.revision !== "bigint" || cursor.revision < 0n || cursor.revision > 0xffff_ffff_ffff_ffffn) {
    throw new RangeError("invalid cursor");
  }
}

export class ReplayError extends Error {
  constructor(
    readonly authority: Authority,
    message: string,
  ) {
    super(message);
  }
}

export class TerminalAdmissionError extends Error {}

export function authorityKey(authority: Authority): string {
  return `${authority.kind}:${authority.id}`;
}

function isCursorStore(store: OutboxStore): store is OutboxStore & CursorStore {
  return "loadCursors" in store && "putCursor" in store && "deleteCursor" in store;
}

function isAtomicClientStateStore(store: OutboxStore): store is AtomicClientStateStore {
  return isCursorStore(store) && "commit" in store && "loadRecovery" in store;
}
