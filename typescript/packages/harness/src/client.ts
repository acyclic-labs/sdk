import type { AggregateKind, Authority, OperationId } from "./index.js";
import type { FileRef, ReferencedAttachments } from "./conversation.js";
import { NativeContracts } from "./native-contracts.js";
import { isSafeAuthorityId } from "./rust-policy.js";
import { SnapshotStore } from "./client-snapshot.js";
import { RequestScheduler } from "./client-requests.js";

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

/** Only immutable addresses and bounded routing metadata may enter a retry outbox. */
export interface OfflinePayload {
  readonly content?: FileRef;
  readonly attachments?: ReferencedAttachments;
  readonly artifacts?: readonly FileRef[];
  readonly references?: readonly FileRef[];
  /** Numeric/boolean routing hints only; free-form strings could persist credentials. */
  readonly metadata?: Readonly<Record<string, number | boolean | bigint | null>>;
}

interface ClientCommandBase {
  readonly operationId: OperationId;
  readonly authority: Authority;
  readonly kind: string;
}

export type ClientCommand<Payload = unknown> =
  | (ClientCommandBase & Readonly<{ payload: OfflinePayload; offlineSafe: true }>)
  | (ClientCommandBase & Readonly<{ payload: Payload; offlineSafe?: false }>);
type OmitAuthority<Command> = Command extends unknown ? Omit<Command, "authority"> : never;

export interface Connection<Event = unknown> extends AsyncIterable<Delivery<Event>> {
  send(command: ClientCommand): Promise<void>;
  close(): Promise<void> | void;
}

export interface Transport<Event = unknown> {
  connect(cursors: ReadonlyMap<string, ReplayCursor>, signal?: AbortSignal): Promise<Connection<Event>>;
}

export interface OutboxStore {
  load(): Promise<readonly ClientCommand[]>;
  /** A fixed enqueue cut prevents an active producer from extending one flush. */
  loadPage?(afterSequence?: number, throughSequence?: number): Promise<OutboxPage>;
  put(command: ClientCommand): Promise<void>;
  delete(operationId: OperationId): Promise<void>;
}

export interface OutboxPage {
  readonly commands: readonly ClientCommand[];
  readonly afterSequence: number;
  readonly throughSequence: number;
}

export interface CursorStore {
  loadCursors(): Promise<ReadonlyMap<string, ReplayCursor>>;
  putCursor(authority: Authority, cursor: ReplayCursor): Promise<void>;
  deleteCursor(authority: Authority): Promise<void>;
}

export interface AtomicClientStateStore extends OutboxStore, CursorStore {
  commit(authority: Authority, cursor: ReplayCursor, operationId?: OperationId, checkpoint?: ClientCheckpoint,
    expectedCursor?: ReplayCursor | null): Promise<void>;
  loadRecovery?(): Promise<readonly ClientRecoveryRecord[]>;
}

/** Data only. The projection adapter validates its exact reducer/schema identity. */
export interface ClientCheckpoint {
  readonly identity: string;
  readonly state: unknown;
}

export class ClientCheckpointConflict extends Error {}

export interface ClientRecoveryRecord {
  readonly authority: string;
  readonly cursor: ReplayCursor;
  readonly checkpoint?: ClientCheckpoint | undefined;
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

export class MemoryOutbox implements OutboxStore {
  readonly #commands = new Map<OperationId, ClientCommand>();
  readonly #sizes = new Map<OperationId, number>();
  #bytes = 0;

  constructor(readonly maximumCommands = 1024, readonly maximumBytes = 16 * 1024 * 1024) {
    positiveBound(maximumCommands, "maximumCommands");
    positiveBound(maximumBytes, "maximumBytes");
  }

  async load(): Promise<readonly ClientCommand[]> {
    return [...this.#commands.values()].map(command => cloneStructuredValue(command, new Set()) as ClientCommand);
  }

  async put(command: ClientCommand): Promise<void> {
    const bytes = retainedDataBytes(command, this.maximumBytes);
    if ((!this.#commands.has(command.operationId) && this.#commands.size >= this.maximumCommands) ||
        bytes > this.maximumBytes - this.#bytes + (this.#sizes.get(command.operationId) ?? 0)) {
      throw new RangeError("memory outbox capacity exceeded");
    }
    const admitted = await assertOutboxSafe(command);
    // Validation awaits WASM; another caller may have admitted work meanwhile.
    const ownedBytes = retainedDataBytes(admitted, this.maximumBytes);
    if ((!this.#commands.has(admitted.operationId) && this.#commands.size >= this.maximumCommands) ||
        ownedBytes > this.maximumBytes - this.#bytes + (this.#sizes.get(admitted.operationId) ?? 0)) {
      throw new RangeError("memory outbox capacity exceeded");
    }
    this.#bytes += ownedBytes - (this.#sizes.get(admitted.operationId) ?? 0);
    this.#sizes.set(admitted.operationId, ownedBytes);
    this.#commands.set(admitted.operationId, admitted);
  }

  async delete(operationId: OperationId): Promise<void> {
    this.#bytes -= this.#sizes.get(operationId) ?? 0;
    this.#sizes.delete(operationId);
    this.#commands.delete(operationId);
  }
}

export class MemoryCursorStore implements CursorStore {
  readonly #cursors = new Map<string, ReplayCursor>();

  constructor(readonly maximumCursors = 64) { positiveBound(maximumCursors, "maximumCursors"); }

  async loadCursors(): Promise<ReadonlyMap<string, ReplayCursor>> {
    return new Map(this.#cursors);
  }

  async putCursor(authority: Authority, cursor: ReplayCursor): Promise<void> {
    validateCursor(cursor);
    retainedDataBytes({ authority, cursor }, 4096);
    if (!this.#cursors.has(authorityKey(authority)) && this.#cursors.size >= this.maximumCursors) {
      throw new RangeError("cursor capacity exceeded");
    }
    this.#cursors.set(authorityKey(authority), Object.freeze({ ...cursor }));
  }

  async deleteCursor(authority: Authority): Promise<void> {
    this.#cursors.delete(authorityKey(authority));
  }
}

export interface IndexedDbClientStoreOptions {
  /** Required tenant/application namespace. Never share this name across security boundaries. */
  readonly databaseName: string;
  readonly maximumCommands?: number;
  readonly maximumBytes?: number;
  /** Separate from encoded storage bytes; accounts retained object metadata. */
  readonly maximumResidentBytes?: number;
  readonly maximumCursors?: number;
  readonly maximumCheckpointBytes?: number;
  readonly indexedDB?: IDBFactory;
}

/** Durable browser state with atomic cursor advancement and outbox acknowledgement. */
export class IndexedDbClientStore implements AtomicClientStateStore {
  #database: Promise<IDBDatabase> | undefined;
  readonly #factory: IDBFactory | undefined;
  readonly #name: string;
  readonly #maximumCommands: number;
  readonly #maximumBytes: number;
  readonly #maximumResidentBytes: number;
  readonly #maximumCursors: number;
  readonly #maximumCheckpointBytes: number;

  constructor(options: IndexedDbClientStoreOptions) {
    this.#maximumCommands = positiveBound(options.maximumCommands ?? 1024, "maximumCommands");
    this.#maximumBytes = positiveBound(options.maximumBytes ?? 16 * 1024 * 1024, "maximumBytes");
    this.#maximumResidentBytes = positiveBound(options.maximumResidentBytes ?? 16 * 1024 * 1024, "maximumResidentBytes");
    this.#maximumCursors = positiveBound(options.maximumCursors ?? 64, "maximumCursors");
    this.#maximumCheckpointBytes = positiveBound(options.maximumCheckpointBytes ?? 256 * 1024, "maximumCheckpointBytes");
    this.#factory = options.indexedDB ?? globalThis.indexedDB;
    if (options.databaseName.trim() === "") throw new TypeError("databaseName is required");
    this.#name = options.databaseName;
  }

  #open(): Promise<IDBDatabase> {
    if (this.#factory === undefined) return Promise.reject(new Error("IndexedDB is not available"));
    return this.#database ??= openClientDatabase(this.#factory, this.#name);
  }

  async load(): Promise<readonly ClientCommand[]> {
    const commands: ClientCommand[] = [];
    let after = 0;
    let through: number | undefined;
    do {
      const page = await this.loadPage(after, through);
      through = page.throughSequence;
      after = page.afterSequence;
      commands.push(...page.commands);
      if (commands.length > this.#maximumCommands) throw new RangeError("outbox capacity exceeded");
      retainedDataBytes(commands, this.#maximumResidentBytes);
    } while (after < through);
    return commands;
  }

  async loadPage(afterSequence = 0, throughSequence?: number): Promise<OutboxPage> {
    if (!Number.isSafeInteger(afterSequence) || afterSequence < 0 ||
        (throughSequence !== undefined && (!Number.isSafeInteger(throughSequence) || throughSequence < afterSequence))) {
      throw new RangeError("invalid outbox page cut");
    }
    const database = await this.#open();
    const store = database.transaction("outbox").objectStore("outbox");
    const index = store.index("sequence");
    const [count, indexedCount, last] = await Promise.all([request(store.count()), request(index.count()), request(index.openCursor(null, "prev"))]);
    if (count > this.#maximumCommands) throw new RangeError("outbox capacity exceeded");
    if (count !== indexedCount) throw new Error("outbox contains an unindexed record");
    const through = throughSequence ?? (last === null ? 0 : outboxRecord(last.value).sequence);
    if (afterSequence >= through) return { commands: [], afterSequence: through, throughSequence: through };
    const records: OutboxRecord[] = [];
    let bytes = 0;
    let after = afterSequence;
    await visitOutbox(index, IDBKeyRange.bound(afterSequence, through, true, false), record => {
      bytes += retainedDataBytes(record.command, this.#maximumResidentBytes - bytes);
      records.push(record);
      after = record.sequence;
      return records.length < Math.min(64, this.#maximumCommands);
    });
    // An empty tail (e.g. another tab acknowledged it) completes this fixed cut.
    if (records.length < Math.min(64, this.#maximumCommands)) after = through;
    return { commands: await Promise.all(records.map(record => assertOutboxSafe(record.command))),
      afterSequence: after, throughSequence: through };
  }

  async put(command: ClientCommand): Promise<void> {
    retainedDataBytes(command, this.#maximumResidentBytes);
    const storedCommand = await assertOutboxSafe(command);
    const residentBytes = retainedDataBytes(storedCommand, this.#maximumResidentBytes);
    const bytes = await structuredSize(storedCommand);
    const contracts = await NativeContracts.create();
    const database = await this.#open();
    const transaction = database.transaction("outbox", "readwrite");
    const store = transaction.objectStore("outbox");
    const [storedCount, indexedCount] = await Promise.all([request(store.count()), request(store.index("sequence").count())]);
    if (storedCount !== indexedCount || storedCount > this.#maximumCommands) {
      transaction.abort();
      throw new Error("invalid outbox record count");
    }
    let existing: OutboxRecord | undefined;
    let count = 0;
    let retained = residentBytes;
    let encoded = bytes;
    let maximum = 0;
    await visitOutbox(store.index("sequence"), null, record => {
      count += 1;
      maximum = record.sequence;
      const size = retainedDataBytes(record.command, this.#maximumResidentBytes);
      const encodedSize = contracts.encodeCanonicalJson(canonicalStructuredValue(record.command, new Set())).byteLength;
      if (record.bytes !== encodedSize) throw new Error("invalid outbox byte accounting");
      if (record.operationId === storedCommand.operationId) existing = record;
      else { retained += size; encoded += encodedSize; }
      if (count > this.#maximumCommands || retained > this.#maximumResidentBytes || encoded > this.#maximumBytes) {
        throw new RangeError("IndexedDB outbox capacity exceeded");
      }
      return true;
    });
    if (count + (existing === undefined ? 1 : 0) > this.#maximumCommands ||
        retained > this.#maximumResidentBytes || encoded > this.#maximumBytes) {
      transaction.abort();
      throw new RangeError("IndexedDB outbox capacity exceeded");
    }
    const sequence = existing?.sequence ?? maximum + 1;
    if (!Number.isSafeInteger(sequence)) { transaction.abort(); throw new RangeError("outbox sequence exhausted"); }
    store.put({ operationId: storedCommand.operationId, bytes, sequence, command: storedCommand });
    await transactionDone(transaction);
  }

  async delete(operationId: OperationId): Promise<void> {
    const database = await this.#open();
    const transaction = database.transaction("outbox", "readwrite");
    transaction.objectStore("outbox").delete(operationId);
    await transactionDone(transaction);
  }

  async loadCursors(): Promise<ReadonlyMap<string, ReplayCursor>> {
    return new Map((await this.loadRecovery()).map(record => [record.authority, record.cursor]));
  }

  async loadRecovery(): Promise<readonly ClientRecoveryRecord[]> {
    const database = await this.#open();
    const records = await request<Array<{ authority: string; generation: string; revision: bigint; checkpoint?: ClientCheckpoint }>>(
      database.transaction("cursors").objectStore("cursors").getAll(undefined, this.#maximumCursors + 1));
    if (records.length > this.#maximumCursors) throw new RangeError("cursor capacity exceeded");
    return records.map(record => {
      validateCursor(record);
      retainedDataBytes({ authority: record.authority, generation: record.generation, revision: record.revision }, 4096);
      if (record.checkpoint !== undefined) retainedDataBytes(record.checkpoint, this.#maximumCheckpointBytes);
      return { authority: record.authority, cursor: { generation: record.generation, revision: record.revision },
        checkpoint: record.checkpoint };
    });
  }

  async putCursor(authority: Authority, cursor: ReplayCursor): Promise<void> {
    await this.commit(authority, cursor);
  }

  async deleteCursor(authority: Authority): Promise<void> {
    const database = await this.#open();
    const transaction = database.transaction("cursors", "readwrite");
    transaction.objectStore("cursors").delete(authorityKey(authority));
    await transactionDone(transaction);
  }

  async commit(authority: Authority, cursor: ReplayCursor, operationId?: OperationId, checkpoint?: ClientCheckpoint,
    expectedCursor?: ReplayCursor | null): Promise<void> {
    validateCursor(cursor);
    retainedDataBytes({ authority, cursor }, 4096);
    if (checkpoint !== undefined) retainedDataBytes(checkpoint, this.#maximumCheckpointBytes);
    const ownedCheckpoint = checkpoint === undefined ? undefined : cloneStructuredValue(checkpoint, new Set());
    const database = await this.#open();
    const transaction = database.transaction(["outbox", "cursors"], "readwrite");
    const cursors = transaction.objectStore("cursors");
    const key = authorityKey(authority);
    const [count, existing] = await Promise.all([request(cursors.count()),
      request<{ generation: string; revision: bigint; checkpoint?: ClientCheckpoint } | undefined>(cursors.get(key))]);
    if (expectedCursor !== undefined && (expectedCursor === null ? existing !== undefined :
      existing?.generation !== expectedCursor.generation || existing.revision !== expectedCursor.revision)) {
      transaction.abort();
      throw new ClientCheckpointConflict("durable projection cut changed concurrently");
    }
    if (checkpoint === undefined && existing?.checkpoint !== undefined &&
        (existing.generation !== cursor.generation || existing.revision !== cursor.revision)) {
      transaction.abort();
      throw new ClientCheckpointConflict("cursor advancement requires its projection checkpoint");
    }
    if (existing === undefined && count >= this.#maximumCursors) {
      transaction.abort();
      throw new RangeError("cursor capacity exceeded");
    }
    if (operationId !== undefined) transaction.objectStore("outbox").delete(operationId);
    cursors.put({
      authority: key,
      generation: cursor.generation,
      revision: cursor.revision,
      checkpoint: ownedCheckpoint ?? existing?.checkpoint,
    });
    await transactionDone(transaction);
  }
}

export type ClientListener<Event> = (event: ClientEvent<Event>) => void;

export interface ClientAdmissionLimits {
  /** Includes the running mutation and callers waiting for it. */
  readonly requests: number;
  readonly bytes: number;
  readonly commandBytes: number;
}

/** Framework-neutral external store derived only from authoritative events. */
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
  getSnapshot = (): State => this.#store.getSnapshot();

  /** Svelte/React-compatible subscription. */
  subscribe = (listener: () => void): (() => void) => this.#store.subscribe(listener);

  /** Detaches the store from its client. */
  dispose(): void {
    if (this.#disposed) return;
    this.#unsubscribe?.();
    this.#disposed = true;
    this.#unsubscribe = undefined;
    this.#store.dispose();
  }
}

/** Framework-neutral reconnecting client with authoritative cursor validation and a safe outbox. */
export class HarnessClient<Event = unknown> {
  readonly #cursors = new Map<string, ReplayCursor>();
  readonly #listeners = new Set<ClientListener<Event>>();
  readonly #errors = new SnapshotStore<unknown | null>(null);
  /** Latest replay/reconnect failure; retry does not hide it from the host. */
  readonly failure = { getSnapshot: this.#errors.getSnapshot, subscribe: this.#errors.subscribe };
  readonly #projections = new Map<string, RecoveryProjection<Event>>();
  #connection: Connection<Event> | undefined;
  #cursorsLoaded = false;
  readonly #mutations: RequestScheduler;
  readonly admission: ClientAdmissionLimits;
  #replayEpoch = 0;
  #running = false;
  #disposed = false;
  #runController: AbortController | undefined;

  constructor(
    readonly transport: Transport<Event>,
    readonly outbox: OutboxStore = new MemoryOutbox(),
    readonly cursorStore: CursorStore = isCursorStore(outbox) ? outbox : new MemoryCursorStore(),
    admission: ClientAdmissionLimits = { requests: 128, bytes: 16 * 1024 * 1024, commandBytes: 1024 * 1024 },
  ) {
    positiveBound(admission.commandBytes, "commandBytes");
    this.admission = Object.freeze({ ...admission });
    this.#mutations = new RequestScheduler({ concurrent: 1, requests: admission.requests, bytes: admission.bytes });
  }

  async handle(kind: AggregateKind, id: string): Promise<AggregateHandle<Event>> {
    return new AggregateHandle(this, await authority(kind, id));
  }

  async agent(id: string): Promise<AgentHandle<Event>> {
    return new AgentHandle(this, await authority("agent", id));
  }

  async conversation(id: string): Promise<ConversationHandle<Event>> {
    return new ConversationHandle(this, await authority("conversation", id));
  }

  async session(id: string): Promise<SessionHandle<Event>> {
    return new SessionHandle(this, await authority("session", id));
  }

  async turn(id: string): Promise<TurnHandle<Event>> {
    return new TurnHandle(this, await authority("turn", id));
  }

  async task(id: string): Promise<TaskHandle<Event>> {
    return new TaskHandle(this, await authority("task", id));
  }

  /** Registers at-least-once projection delivery; callbacks must be idempotent. */
  subscribe(listener: ClientListener<Event>): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  /** Attach all durable projections before the first replay/rebase. */
  registerProjection(authority: Authority, projection: RecoveryProjection<Event>): () => void {
    if (this.#cursorsLoaded) throw new Error("register projections before client recovery");
    if (!isAtomicClientStateStore(this.outbox) || this.outbox !== this.cursorStore || this.outbox.loadRecovery === undefined) {
      throw new Error("durable projection requires one atomic recovery store");
    }
    const key = authorityKey(authority);
    if (this.#projections.has(key)) throw new Error("duplicate durable projection authority");
    if (this.#projections.size >= 64) throw new RangeError("projection capacity exceeded");
    this.#projections.set(key, projection);
    return () => {
      if (this.#running && !this.#disposed) throw new Error("stop replay before disposing its durable projection");
      this.#projections.delete(key);
      this.#cursorsLoaded = false;
      this.#cursors.clear();
    };
  }

  async submit(input: ClientCommand): Promise<void> {
    const bytes = retainedDataBytes(input, this.admission.commandBytes);
    const command = cloneStructuredValue(input, new Set()) as ClientCommand;
    await this.#serializeState(async () => {
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
    }, bytes);
  }

  /** Installs an authoritative snapshot cursor after synchronously resetting projections. */
  async rebase(authority: Authority, cursor: ReplayCursor, resetProjections: (() => void) | ClientCheckpoint): Promise<void> {
    validateCursor(cursor);
    const bytes = retainedDataBytes({ authority, cursor,
      checkpoint: typeof resetProjections === "function" ? null : resetProjections }, this.admission.commandBytes);
    authority = { ...authority };
    cursor = { ...cursor };
    if (typeof resetProjections !== "function") resetProjections = cloneStructuredValue(resetProjections, new Set()) as ClientCheckpoint;
    let connection: Connection<Event> | undefined;
    try {
      await this.#serializeState(async () => {
        const recovered = await this.#hydrateCursors(typeof resetProjections === "function" ? undefined : authorityKey(authority));
        this.#replayEpoch += 1;
        connection = this.#connection;
        this.#connection = undefined;
        const key = authorityKey(authority);
        const previous = this.#cursors.get(key) ?? recovered;
        const projection = this.#projections.get(key);
        if (projection !== undefined) {
          if (typeof resetProjections === "function") throw new Error("durable rebase requires an authoritative checkpoint");
          const publish = projection.restore(resetProjections, cursor);
          await (this.outbox as AtomicClientStateStore).commit(authority, cursor, undefined, resetProjections, previous ?? null);
          this.#cursors.set(key, cursor);
          this.#cursorsLoaded = true;
          publish();
          return;
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
        this.#cursors.set(key, cursor);
      }, bytes);
    } finally {
      await connection?.close();
    }
  }

  /** Runs reconnect/replay until aborted; transport failures use bounded exponential backoff. */
  async run(signal?: AbortSignal): Promise<void> {
    if (this.#disposed) throw new Error("client is disposed");
    if (this.#running) throw new Error("client replay is already running");
    this.#running = true;
    const controller = new AbortController();
    this.#runController = controller;
    try { await this.#run(signal === undefined ? controller.signal : AbortSignal.any([signal, controller.signal])); }
    finally { this.#running = false; this.#runController = undefined; }
  }

  /** Cancel owned interest; started storage/send work retains its reservation
   * until it settles and may have an indeterminate authoritative outcome. */
  dispose(): void {
    if (this.#disposed) return;
    this.#disposed = true;
    this.#runController?.abort();
    this.#mutations.dispose();
    this.#listeners.clear();
    this.#errors.dispose();
  }

  async #run(signal?: AbortSignal): Promise<void> {
    await this.#serializeState(() => this.#hydrateCursors());
    let delay = 50;
    while (!signal?.aborted) {
      let unreceived: Connection<Event> | undefined;
      try {
        const { cursors, epoch } = await this.#serializeState(async () => ({
          cursors: new Map(this.#cursors),
          epoch: this.#replayEpoch,
        }));
        const connection = await this.transport.connect(cursors, signal);
        unreceived = connection;
        if (signal?.aborted) throw new DOMException("Client replay cancelled", "AbortError");
        const current = await this.#serializeState(async () => {
          if (epoch !== this.#replayEpoch) return false;
          this.#connection = connection;
          return true;
        });
        if (!current) {
          await connection.close();
          unreceived = undefined;
          throw new StaleReplayConnectionError();
        }
        unreceived = undefined;
        await this.#flushOutbox(connection, epoch, signal);
        for await (const delivery of connection) {
          if (signal?.aborted) break;
          await this.#accept(delivery, epoch);
        }
        delay = 50;
      } catch (error) {
        this.#errors.publish(error);
        if (signal?.aborted) break;
        if (error instanceof ClientCheckpointConflict) throw error;
        if (error instanceof ReplayError) {
          if (this.#projections.has(authorityKey(error.authority))) throw error;
          await this.#serializeState(async () => {
            await this.cursorStore.deleteCursor(error.authority);
            this.#cursors.delete(authorityKey(error.authority));
          });
        }
        await abortableDelay(delay, signal);
        delay = Math.min(delay * 2, 5_000);
      } finally {
        const connection = this.#connection ?? unreceived;
        this.#connection = undefined;
        await connection?.close();
      }
    }
  }

  async #flushOutbox(connection: Connection<Event>, epoch: number, signal?: AbortSignal): Promise<void> {
    let after = 0;
    let through: number | undefined;
    do {
      if (signal?.aborted) return;
      const page = this.outbox.loadPage === undefined ? undefined : await this.outbox.loadPage(after, through);
      const commands = page?.commands ?? await this.outbox.load();
      for (const command of commands) {
        await this.#serializeState(async () => {
          if (signal?.aborted) throw new DOMException("Client replay cancelled", "AbortError");
          if (epoch !== this.#replayEpoch || connection !== this.#connection) {
            throw new StaleReplayConnectionError();
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
    const bytes = retainedDataBytes(delivery, this.admission.commandBytes);
    const admitted = cloneStructuredValue(delivery, new Set()) as Delivery<Event>;
    await this.#serializeState(async () => {
      if (epoch !== this.#replayEpoch) throw new StaleReplayConnectionError();
      await this.#acceptSerialized(admitted);
    }, bytes);
  }

  async #acceptSerialized(delivery: Delivery<Event>): Promise<void> {
    const key = authorityKey(delivery.authority);
    const previous = this.#cursors.get(key);
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
    let committed = expected;
    for (const event of delivery.events) {
      committed = event.revision;
      const cursor = { generation: delivery.generation, revision: committed };
      const prepared = this.#projections.get(key)?.prepare(event);
      if (prepared === undefined) for (const listener of this.#listeners) listener(event);
      if (isAtomicClientStateStore(this.outbox) && this.outbox === this.cursorStore) {
        await this.outbox.commit(delivery.authority, cursor, event.operationId, prepared?.checkpoint,
          prepared === undefined ? undefined : this.#cursors.get(key) ?? null);
      } else {
        await this.outbox.delete(event.operationId);
        await this.cursorStore.putCursor(delivery.authority, cursor);
      }
      this.#cursors.set(key, cursor);
      if (prepared !== undefined && !this.#disposed) {
        prepared.publish();
        for (const listener of this.#listeners) listener(event);
      }
    }
    if (delivery.events.length === 0) {
      const cursor = { generation: delivery.generation, revision };
      // Empty delivery cannot erase the checkpoint paired with an unchanged cursor.
      if (previous?.generation !== cursor.generation || previous.revision !== cursor.revision) {
        const projection = this.#projections.get(key);
        if (projection !== undefined) {
          await (this.outbox as AtomicClientStateStore).commit(delivery.authority, cursor, undefined,
            projection.checkpoint(), previous ?? null);
        } else await this.cursorStore.putCursor(delivery.authority, cursor);
      }
      this.#cursors.set(key, cursor);
    }
  }

  async #hydrateCursors(replacedAuthority?: string): Promise<ReplayCursor | undefined> {
    if (this.#cursorsLoaded) return replacedAuthority === undefined ? undefined : this.#cursors.get(replacedAuthority);
    if (isAtomicClientStateStore(this.outbox) && this.outbox === this.cursorStore && this.outbox.loadRecovery !== undefined) {
      const records = await this.outbox.loadRecovery();
      const publications: (() => void)[] = [];
      for (const record of records) {
        if (record.authority === replacedAuthority) continue;
        const projection = this.#projections.get(record.authority);
        if (projection !== undefined || record.checkpoint !== undefined) {
          if (projection === undefined || record.checkpoint === undefined) throw new Error("cursor has no matching recoverable projection");
          publications.push(projection.restore(record.checkpoint, record.cursor));
        }
      }
      for (const publish of publications) publish();
      for (const record of records) {
        if (record.authority !== replacedAuthority) this.#cursors.set(record.authority, record.cursor);
      }
      // A replacement cut becomes resumable only after its checkpoint commits.
      this.#cursorsLoaded = replacedAuthority === undefined;
      return records.find(record => record.authority === replacedAuthority)?.cursor;
    }
    for (const [key, cursor] of await this.cursorStore.loadCursors()) {
      if (!this.#cursors.has(key)) this.#cursors.set(key, cursor);
    }
    this.#cursorsLoaded = true;
  }

  #serializeState<T>(operation: () => Promise<T>, bytes = 256): Promise<T> {
    return this.#mutations.schedule(bytes, operation);
  }
}

/** Conservative retained-data accounting with bounded traversal, before queueing. */
function retainedDataBytes(value: unknown, maximum: number): number {
  let bytes = 0;
  const ancestors = new Set<object>();
  const charge = (amount: number): void => {
    if (amount > maximum - bytes) throw new RangeError("client command byte capacity exceeded");
    bytes += amount;
  };
  const visit = (data: unknown, depth: number): void => {
    if (depth > 64) throw new RangeError("client command nesting exceeded");
    charge(32);
    if (typeof data === "string") { charge(data.length * 2); return; }
    if (data === null || typeof data === "boolean" || typeof data === "number") return;
    if (typeof data === "bigint") { charge(data.toString().length * 2); return; }
    if (typeof data !== "object") throw new TypeError("command contains a non-data value");
    if (ancestors.has(data)) throw new TypeError("command contains a cycle");
    if (data instanceof ArrayBuffer) { charge(data.byteLength); return; }
    if (ArrayBuffer.isView(data)) { charge(data.buffer.byteLength); return; }
    if (Array.isArray(data)) charge(data.length * 8);
    if (!Array.isArray(data) && Object.getPrototypeOf(data) !== Object.prototype && Object.getPrototypeOf(data) !== null) {
      throw new TypeError("command contains a non-canonical structured value");
    }
    ancestors.add(data);
    try {
      for (const key in data) {
        if (!Object.hasOwn(data, key)) continue;
        charge(32 + key.length * 2);
        const descriptor = Object.getOwnPropertyDescriptor(data, key)!;
        if (!("value" in descriptor)) throw new TypeError("command contains an accessor");
        visit(descriptor.value, depth + 1);
      }
    } finally { ancestors.delete(data); }
  };
  visit(value, 0);
  return bytes;
}

function validateCursor(cursor: ReplayCursor): void {
  if (typeof cursor.generation !== "string" || cursor.generation === "" ||
      typeof cursor.revision !== "bigint" || cursor.revision < 0n || cursor.revision > 0xffff_ffff_ffff_ffffn) {
    throw new RangeError("invalid cursor");
  }
}

export class AggregateHandle<Event = unknown> {
  constructor(
    readonly client: HarnessClient<Event>,
    readonly authority: Authority,
  ) {}

  submit<Payload>(command: OmitAuthority<ClientCommand<Payload>>): Promise<void> {
    return this.client.submit({ ...command, authority: this.authority } as ClientCommand<Payload>);
  }
}

export class AgentHandle<Event = unknown> extends AggregateHandle<Event> {}
export class ConversationHandle<Event = unknown> extends AggregateHandle<Event> {}
export class SessionHandle<Event = unknown> extends AggregateHandle<Event> {}
export class TurnHandle<Event = unknown> extends AggregateHandle<Event> {}
export class TaskHandle<Event = unknown> extends AggregateHandle<Event> {}

export class ReplayError extends Error {
  constructor(
    readonly authority: Authority,
    message: string,
  ) {
    super(message);
  }
}

/** A command was authoritatively rejected and must never remain retryable. */
export class TerminalAdmissionError extends Error {}

class StaleReplayConnectionError extends Error {}

function authorityKey(authority: Authority): string {
  return `${authority.kind}:${authority.id}`;
}

async function authority(kind: AggregateKind, id: string): Promise<Authority> {
  if (!(await isSafeAuthorityId(id))) throw new TypeError("aggregate identity is not a safe path segment");
  return { kind, id };
}

function isCursorStore(store: OutboxStore): store is OutboxStore & CursorStore {
  return "loadCursors" in store && "putCursor" in store && "deleteCursor" in store;
}

function isAtomicClientStateStore(store: OutboxStore): store is AtomicClientStateStore {
  return isCursorStore(store) && "commit" in store;
}

function positiveBound(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) throw new RangeError(`${name} must be a positive safe integer`);
  return value;
}

/** An offline retry record may carry refs and routing metadata, never a bearer or inline body. */
async function assertOutboxSafe(command: ClientCommand): Promise<ClientCommand> {
  const admitted = cloneStructuredValue(command, new Set()) as ClientCommand;
  if (admitted.offlineSafe !== true || admitted.kind === "interaction.resolve.approval") {
    throw new TypeError("command is not safe for the offline outbox");
  }
  const fields = Object.keys(admitted);
  if (fields.length !== 5 || fields.some(field => !["operationId", "authority", "kind", "payload", "offlineSafe"].includes(field))) {
    throw new TypeError("offline outbox command contains an unsupported field");
  }
  if (admitted.authority === null || typeof admitted.authority !== "object"
    || Object.getPrototypeOf(admitted.authority) !== Object.prototype
    || Object.keys(admitted.authority).length !== 2
    || !Object.keys(admitted.authority).every(field => field === "kind" || field === "id")) {
    throw new TypeError("offline outbox authority contains an unsupported field");
  }
  if (!(await isSafeAuthorityId(admitted.authority.id))) {
    throw new TypeError("offline outbox authority contains an unsafe identity");
  }
  const forbidden = /(?:token|authorization|credential|secret|password|api[_-]?key|(?:^|[_-])(?:scope|proof|body|text|bytes|base64|data)(?:$|[_-]))/i;
  const visit = (value: unknown, ancestors: Set<object>): void => {
    if (value === null || typeof value !== "object") return;
    if (value instanceof ArrayBuffer || ArrayBuffer.isView(value)) {
      throw new TypeError("offline outbox cannot persist inline bytes or credentials");
    }
    if (ancestors.has(value)) throw new TypeError("command contains a cycle");
    ancestors.add(value);
    try {
      for (const [key, descriptor] of Object.entries(Object.getOwnPropertyDescriptors(value))) {
        if (!descriptor.enumerable) continue;
        if (!("value" in descriptor)) throw new TypeError("command contains an accessor");
        if (forbidden.test(key)) throw new TypeError("offline outbox cannot persist inline bytes or credentials");
        visit(descriptor.value, ancestors);
      }
    } finally { ancestors.delete(value); }
  };
  visit(admitted, new Set());
  const payload = admitted.payload;
  if (payload === null || typeof payload !== "object" || Array.isArray(payload) ||
      Object.getPrototypeOf(payload) !== Object.prototype) {
    throw new TypeError("offline outbox payload must be a ref-only record");
  }
  for (const key of Object.keys(payload)) {
    if (!["content", "attachments", "artifacts", "references", "metadata"].includes(key)) {
      throw new TypeError("offline outbox payload contains an unsupported field");
    }
  }
  const contracts = await NativeContracts.create();
  if (payload.content !== undefined) contracts.validate("file_ref", payload.content);
  if (payload.attachments !== undefined) contracts.validate("attachments", payload.attachments);
  for (const refs of [payload.artifacts, payload.references]) {
    if (refs === undefined) continue;
    if (!Array.isArray(refs)) throw new TypeError("offline outbox references must be a list");
    for (const reference of refs) contracts.validate("file_ref", reference);
  }
  if (payload.metadata !== undefined) {
    if (payload.metadata === null || typeof payload.metadata !== "object" || Array.isArray(payload.metadata) ||
        Object.getPrototypeOf(payload.metadata) !== Object.prototype) {
      throw new TypeError("offline outbox metadata must be a record");
    }
    for (const [key, value] of Object.entries(payload.metadata)) {
      if (forbidden.test(key) || !(value === null || ["number", "boolean", "bigint"].includes(typeof value))) {
        throw new TypeError("offline outbox metadata contains an unsafe value");
      }
    }
  }
  return admitted;
}

async function structuredSize(value: unknown): Promise<number> {
  const transportSafe = canonicalStructuredValue(value, new Set());
  return (await NativeContracts.create()).encodeCanonicalJson(transportSafe).byteLength;
}

function cloneStructuredValue(value: unknown, ancestors: Set<object>): unknown {
  if (
    value === null || typeof value === "string" || typeof value === "boolean" ||
    typeof value === "bigint"
  ) return value;
  if (typeof value === "number") {
    if (!Number.isFinite(value) || (Number.isInteger(value) && !Number.isSafeInteger(value))) {
      throw new TypeError("command contains a non-finite number or inexact integer");
    }
    return value;
  }
  if (typeof value !== "object") throw new TypeError("command contains a non-data value");
  if (ancestors.has(value)) throw new TypeError("command contains a cycle");
  if (value instanceof ArrayBuffer) return value.slice(0);
  if (value instanceof Uint8Array) return value.slice();
  if (ArrayBuffer.isView(value)) {
    throw new TypeError("command contains an unsupported binary view");
  }
  if (!Array.isArray(value) && Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null) {
    throw new TypeError("command contains a non-canonical structured value");
  }
  const descriptors = Object.getOwnPropertyDescriptors(value);
  const keys = Reflect.ownKeys(descriptors);
  if (keys.some(key => typeof key !== "string")) {
    throw new TypeError("command contains symbol properties");
  }
  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      const cloned = new Array<unknown>(value.length);
      for (const key of keys as string[]) {
        if (key === "length" || descriptors[key]?.enumerable !== true) continue;
        const index = Number(key);
        if (!Number.isSafeInteger(index) || index < 0 || index >= value.length || String(index) !== key) {
          throw new TypeError("command array contains custom properties");
        }
        const descriptor = descriptors[key];
        if (descriptor === undefined || !("value" in descriptor)) {
          throw new TypeError("command contains an accessor");
        }
        cloned[index] = cloneStructuredValue(descriptor.value, ancestors);
      }
      return cloned;
    }
    const cloned: Record<string, unknown> = {};
    for (const key of keys as string[]) {
      const descriptor = descriptors[key];
      if (descriptor?.enumerable !== true) continue;
      if (!("value" in descriptor)) throw new TypeError("command contains an accessor");
      Object.defineProperty(cloned, key, { value: cloneStructuredValue(descriptor.value, ancestors),
        enumerable: true, configurable: true, writable: true });
    }
    return cloned;
  } finally {
    ancestors.delete(value);
  }
}

function canonicalStructuredValue(value: unknown, ancestors: Set<object>): unknown {
  if (value === null || typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number") {
    if (!Number.isFinite(value) || (Number.isInteger(value) && !Number.isSafeInteger(value))) {
      throw new TypeError("command contains a non-finite number or inexact integer");
    }
    return value;
  }
  if (typeof value === "bigint") return { $bigint: value.toString() };
  if (typeof value !== "object") throw new TypeError("command contains a non-data value");
  if (ancestors.has(value)) throw new TypeError("command contains a cycle");
  if (value instanceof ArrayBuffer) return { $bytes: [...new Uint8Array(value)] };
  if (ArrayBuffer.isView(value)) {
    return { $bytes: [...new Uint8Array(value.buffer, value.byteOffset, value.byteLength)] };
  }
  if (!Array.isArray(value) && Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null) {
    throw new TypeError("command contains a non-canonical structured value");
  }
  ancestors.add(value);
  try {
    if (Array.isArray(value)) return value.map(child => canonicalStructuredValue(child, ancestors));
    return Object.fromEntries(Object.entries(value).map(([key, child]) => [
      key,
      canonicalStructuredValue(child, ancestors),
    ]));
  } finally {
    ancestors.delete(value);
  }
}

function openClientDatabase(
  factory: IDBFactory,
  name: string,
): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    let unsupportedState: Error | undefined;
    const opening = factory.open(name, 3);
    opening.onupgradeneeded = event => {
      if ((event as IDBVersionChangeEvent).oldVersion !== 0) {
        unsupportedState = new Error("pre-v2 Harness outbox state is unsupported");
        opening.transaction?.abort();
        return;
      }
      const database = opening.result;
      database.createObjectStore("outbox", { keyPath: "operationId" }).createIndex("sequence", "sequence", { unique: true });
      database.createObjectStore("cursors", { keyPath: "authority" });
    };
    opening.onsuccess = () => {
      const database = opening.result;
      try {
        const index = database.transaction("outbox").objectStore("outbox").index("sequence");
        if (index.keyPath !== "sequence" || !index.unique) throw new Error("invalid sequence index");
        if (!database.objectStoreNames.contains("cursors")) throw new Error("missing cursors store");
        resolve(database);
      } catch {
        database.close();
        reject(new Error("Harness outbox sequence index is unsupported"));
      }
    };
    opening.onerror = () => reject(unsupportedState ?? opening.error ?? new Error("IndexedDB open failed"));
    opening.onblocked = () => reject(new Error("IndexedDB upgrade is blocked"));
  });
}

interface OutboxRecord {
  readonly operationId: string;
  readonly command: ClientCommand;
  readonly bytes: number;
  readonly sequence: number;
}

function outboxRecord(value: OutboxRecord): OutboxRecord {
  if (typeof value.operationId !== "string" || value.command?.operationId !== value.operationId ||
      !Number.isSafeInteger(value.bytes) || value.bytes < 0 ||
      !Number.isSafeInteger(value.sequence) || value.sequence <= 0) throw new Error("invalid outbox record");
  return value;
}

/** Cursor callbacks keep the transaction alive and retain only the caller's page. */
function visitOutbox(index: IDBIndex, range: IDBKeyRange | null, visit: (record: OutboxRecord) => boolean): Promise<void> {
  return new Promise((resolve, reject) => {
    const reading = index.openCursor(range);
    reading.onerror = () => reject(reading.error ?? new Error("outbox page read failed"));
    reading.onsuccess = () => {
      try {
        const cursor = reading.result;
        if (cursor === null || !visit(outboxRecord(cursor.value))) { resolve(); return; }
        cursor.continue();
      } catch (error) {
        index.objectStore.transaction.abort();
        reject(error);
      }
    };
  });
}

function request<T>(operation: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    operation.onsuccess = () => resolve(operation.result);
    operation.onerror = () => reject(operation.error ?? new Error("IndexedDB request failed"));
  });
}

function transactionDone(transaction: IDBTransaction): Promise<void> {
  return new Promise((resolve, reject) => {
    transaction.oncomplete = () => resolve();
    transaction.onabort = () => reject(transaction.error ?? new Error("IndexedDB transaction aborted"));
    transaction.onerror = () => reject(transaction.error ?? new Error("IndexedDB transaction failed"));
  });
}

async function abortableDelay(milliseconds: number, signal?: AbortSignal): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    if (signal?.aborted) { reject(signal.reason); return; }
    const aborted = () => { clearTimeout(timeout); reject(signal?.reason); };
    const timeout = setTimeout(() => { signal?.removeEventListener("abort", aborted); resolve(); }, milliseconds);
    signal?.addEventListener("abort", aborted, { once: true });
  });
}
