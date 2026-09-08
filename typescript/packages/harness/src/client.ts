import type { AggregateKind, Authority, OperationId } from "./index.js";

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

export interface ClientCommand<Payload = unknown> {
  readonly operationId: OperationId;
  readonly authority: Authority;
  readonly kind: string;
  readonly payload: Payload;
  readonly offlineSafe?: boolean;
}

export interface Connection<Event = unknown> extends AsyncIterable<Delivery<Event>> {
  send(command: ClientCommand): Promise<void>;
  close(): Promise<void> | void;
}

export interface Transport<Event = unknown> {
  connect(cursors: ReadonlyMap<string, ReplayCursor>, signal?: AbortSignal): Promise<Connection<Event>>;
}

export interface OutboxStore {
  load(): Promise<readonly ClientCommand[]>;
  put(command: ClientCommand): Promise<void>;
  delete(operationId: OperationId): Promise<void>;
}

export interface CursorStore {
  loadCursors(): Promise<ReadonlyMap<string, ReplayCursor>>;
  putCursor(authority: Authority, cursor: ReplayCursor): Promise<void>;
  deleteCursor(authority: Authority): Promise<void>;
}

export interface AtomicClientStateStore extends OutboxStore, CursorStore {
  commit(authority: Authority, cursor: ReplayCursor, operationId?: OperationId): Promise<void>;
}

export class MemoryOutbox implements OutboxStore {
  readonly #commands = new Map<OperationId, ClientCommand>();

  async load(): Promise<readonly ClientCommand[]> {
    return [...this.#commands.values()];
  }

  async put(command: ClientCommand): Promise<void> {
    this.#commands.set(command.operationId, command);
  }

  async delete(operationId: OperationId): Promise<void> {
    this.#commands.delete(operationId);
  }
}

export class MemoryCursorStore implements CursorStore {
  readonly #cursors = new Map<string, ReplayCursor>();

  async loadCursors(): Promise<ReadonlyMap<string, ReplayCursor>> {
    return new Map(this.#cursors);
  }

  async putCursor(authority: Authority, cursor: ReplayCursor): Promise<void> {
    this.#cursors.set(authorityKey(authority), cursor);
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
  readonly indexedDB?: IDBFactory;
}

/** Durable browser state with atomic cursor advancement and outbox acknowledgement. */
export class IndexedDbClientStore implements AtomicClientStateStore {
  readonly #database: Promise<IDBDatabase>;
  readonly #maximumCommands: number;
  readonly #maximumBytes: number;

  constructor(options: IndexedDbClientStoreOptions) {
    this.#maximumCommands = positiveBound(options.maximumCommands ?? 1_024, "maximumCommands");
    this.#maximumBytes = positiveBound(options.maximumBytes ?? 16 * 1024 * 1024, "maximumBytes");
    const factory = options.indexedDB ?? globalThis.indexedDB;
    if (factory === undefined) throw new Error("IndexedDB is not available");
    if (options.databaseName.trim() === "") throw new TypeError("databaseName is required");
    this.#database = openClientDatabase(
      factory,
      options.databaseName,
      this.#maximumCommands,
      this.#maximumBytes,
    );
  }

  async load(): Promise<readonly ClientCommand[]> {
    const database = await this.#database;
    const records = await request<Array<{ command: ClientCommand; sequence?: number }>>(
      database.transaction("outbox").objectStore("outbox").getAll(),
    );
    return records
      .sort((left, right) => (left.sequence ?? Number.MAX_SAFE_INTEGER) - (right.sequence ?? Number.MAX_SAFE_INTEGER))
      .map(record => record.command);
  }

  async put(command: ClientCommand): Promise<void> {
    const database = await this.#database;
    const transaction = database.transaction("outbox", "readwrite");
    const store = transaction.objectStore("outbox");
    const records = await request<Array<{ operationId: string; bytes: number; sequence?: number }>>(store.getAll());
    const storedCommand = cloneStructuredValue(command, new Set()) as ClientCommand;
    const bytes = structuredSize(storedCommand);
    const existing = records.find(record => record.operationId === command.operationId);
    const previous = existing?.bytes ?? 0;
    if (records.length + (existing === undefined ? 1 : 0) > this.#maximumCommands ||
      records.reduce((total, record) => total + record.bytes, bytes - previous) > this.#maximumBytes) {
      transaction.abort();
      throw new RangeError("IndexedDB outbox capacity exceeded");
    }
    const sequence = existing?.sequence ?? records.reduce(
      (maximum, record) => Math.max(maximum, record.sequence ?? 0),
      0,
    ) + 1;
    store.put({ operationId: command.operationId, bytes, sequence, command: storedCommand });
    await transactionDone(transaction);
  }

  async delete(operationId: OperationId): Promise<void> {
    const database = await this.#database;
    const transaction = database.transaction("outbox", "readwrite");
    transaction.objectStore("outbox").delete(operationId);
    await transactionDone(transaction);
  }

  async loadCursors(): Promise<ReadonlyMap<string, ReplayCursor>> {
    const database = await this.#database;
    const records = await request<Array<{ authority: string; generation: string; revision: bigint }>>(
      database.transaction("cursors").objectStore("cursors").getAll(),
    );
    return new Map(records.map(record => [record.authority, {
      generation: record.generation,
      revision: record.revision,
    }]));
  }

  async putCursor(authority: Authority, cursor: ReplayCursor): Promise<void> {
    await this.commit(authority, cursor);
  }

  async deleteCursor(authority: Authority): Promise<void> {
    const database = await this.#database;
    const transaction = database.transaction("cursors", "readwrite");
    transaction.objectStore("cursors").delete(authorityKey(authority));
    await transactionDone(transaction);
  }

  async commit(authority: Authority, cursor: ReplayCursor, operationId?: OperationId): Promise<void> {
    const database = await this.#database;
    const transaction = database.transaction(["outbox", "cursors"], "readwrite");
    if (operationId !== undefined) transaction.objectStore("outbox").delete(operationId);
    transaction.objectStore("cursors").put({
      authority: authorityKey(authority),
      generation: cursor.generation,
      revision: cursor.revision,
    });
    await transactionDone(transaction);
  }
}

export type ClientListener<Event> = (event: ClientEvent<Event>) => void;

/** Framework-neutral external store derived only from authoritative events. */
export class ProjectionStore<State, Event = unknown> {
  #state: State;
  readonly #listeners = new Set<() => void>();
  readonly #unsubscribe: () => void;

  constructor(client: HarnessClient<Event>, initial: State, reduce: (state: State, event: ClientEvent<Event>) => State) {
    this.#state = initial;
    this.#unsubscribe = client.subscribe(event => {
      this.#state = reduce(this.#state, event);
      for (const listener of this.#listeners) listener();
    });
  }

  /** React-compatible immutable snapshot accessor. */
  getSnapshot = (): State => this.#state;

  /** Svelte/React-compatible subscription. */
  subscribe = (listener: () => void): (() => void) => {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  };

  /** Detaches the store from its client. */
  dispose(): void {
    this.#unsubscribe();
    this.#listeners.clear();
  }
}

/** Framework-neutral reconnecting client with authoritative cursor validation and a safe outbox. */
export class HarnessClient<Event = unknown> {
  readonly #cursors = new Map<string, ReplayCursor>();
  readonly #listeners = new Set<ClientListener<Event>>();
  #connection: Connection<Event> | undefined;
  #cursorsLoaded = false;
  #stateMutation: Promise<void> = Promise.resolve();
  #replayEpoch = 0;

  constructor(
    readonly transport: Transport<Event>,
    readonly outbox: OutboxStore = new MemoryOutbox(),
    readonly cursorStore: CursorStore = isCursorStore(outbox) ? outbox : new MemoryCursorStore(),
  ) {}

  handle(kind: AggregateKind, id: string): AggregateHandle<Event> {
    return new AggregateHandle(this, { kind, id });
  }

  agent(id: string): AgentHandle<Event> {
    return new AgentHandle(this, { kind: "agent", id });
  }

  conversation(id: string): ConversationHandle<Event> {
    return new ConversationHandle(this, { kind: "conversation", id });
  }

  session(id: string): SessionHandle<Event> {
    return new SessionHandle(this, { kind: "session", id });
  }

  turn(id: string): TurnHandle<Event> {
    return new TurnHandle(this, { kind: "turn", id });
  }

  task(id: string): TaskHandle<Event> {
    return new TaskHandle(this, { kind: "task", id });
  }

  /** Registers at-least-once projection delivery; callbacks must be idempotent. */
  subscribe(listener: ClientListener<Event>): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  async submit(command: ClientCommand): Promise<void> {
    await this.#serializeState(async () => {
      const safelyReplayable = command.offlineSafe && command.kind !== "interaction.resolve.approval";
      if (this.#connection !== undefined) {
        if (safelyReplayable) await this.outbox.put(command);
        try {
          return await this.#connection.send(command);
        } catch (error) {
          if (safelyReplayable && error instanceof TerminalAdmissionError) {
            await this.outbox.delete(command.operationId);
          }
          throw error;
        }
      }
      if (!safelyReplayable) {
        throw new Error("command is not safe for the offline outbox");
      }
      await this.outbox.put(command);
    });
  }

  /** Installs an authoritative snapshot cursor after synchronously resetting projections. */
  async rebase(authority: Authority, cursor: ReplayCursor, resetProjections: () => void): Promise<void> {
    if (cursor.generation === "" || cursor.revision < 0n) throw new RangeError("invalid cursor");
    let connection: Connection<Event> | undefined;
    try {
      await this.#serializeState(async () => {
        await this.#hydrateCursors();
        this.#replayEpoch += 1;
        connection = this.#connection;
        this.#connection = undefined;
        const key = authorityKey(authority);
        const previous = this.#cursors.get(key);
        await this.cursorStore.putCursor(authority, cursor);
        try {
          resetProjections();
        } catch (error) {
          if (previous === undefined) await this.cursorStore.deleteCursor(authority);
          else await this.cursorStore.putCursor(authority, previous);
          throw error;
        }
        this.#cursors.set(key, cursor);
      });
    } finally {
      await connection?.close();
    }
  }

  /** Runs reconnect/replay until aborted; transport failures use bounded exponential backoff. */
  async run(signal?: AbortSignal): Promise<void> {
    await this.#serializeState(() => this.#hydrateCursors());
    let delay = 50;
    while (!signal?.aborted) {
      try {
        const { cursors, epoch } = await this.#serializeState(async () => ({
          cursors: new Map(this.#cursors),
          epoch: this.#replayEpoch,
        }));
        const connection = await this.transport.connect(cursors, signal);
        const current = await this.#serializeState(async () => {
          if (epoch !== this.#replayEpoch) return false;
          this.#connection = connection;
          return true;
        });
        if (!current) {
          await connection.close();
          throw new StaleReplayConnectionError();
        }
        await this.#flushOutbox(connection, epoch);
        for await (const delivery of connection) await this.#accept(delivery, epoch);
        delay = 50;
      } catch (error) {
        if (signal?.aborted) break;
        if (error instanceof ReplayError) {
          await this.#serializeState(async () => {
            await this.cursorStore.deleteCursor(error.authority);
            this.#cursors.delete(authorityKey(error.authority));
          });
        }
        await abortableDelay(delay, signal);
        delay = Math.min(delay * 2, 5_000);
      } finally {
        const connection = this.#connection;
        this.#connection = undefined;
        await connection?.close();
      }
    }
  }

  async #flushOutbox(connection: Connection<Event>, epoch: number): Promise<void> {
    for (const command of await this.outbox.load()) {
      await this.#serializeState(async () => {
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
  }

  async #accept(delivery: Delivery<Event>, epoch: number): Promise<void> {
    await this.#serializeState(async () => {
      if (epoch !== this.#replayEpoch) throw new StaleReplayConnectionError();
      await this.#acceptSerialized(delivery);
    });
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
      for (const listener of this.#listeners) listener(event);
      committed = event.revision;
      const cursor = { generation: delivery.generation, revision: committed };
      if (isAtomicClientStateStore(this.outbox) && this.outbox === this.cursorStore) {
        await this.outbox.commit(delivery.authority, cursor, event.operationId);
      } else {
        await this.outbox.delete(event.operationId);
        await this.cursorStore.putCursor(delivery.authority, cursor);
      }
      this.#cursors.set(key, cursor);
    }
    if (delivery.events.length === 0) {
      const cursor = { generation: delivery.generation, revision };
      await this.cursorStore.putCursor(delivery.authority, cursor);
      this.#cursors.set(key, cursor);
    }
  }

  async #hydrateCursors(): Promise<void> {
    if (this.#cursorsLoaded) return;
    for (const [key, cursor] of await this.cursorStore.loadCursors()) {
      if (!this.#cursors.has(key)) this.#cursors.set(key, cursor);
    }
    this.#cursorsLoaded = true;
  }

  #serializeState<T>(operation: () => Promise<T>): Promise<T> {
    const result = this.#stateMutation.then(operation, operation);
    this.#stateMutation = result.then(() => undefined, () => undefined);
    return result;
  }
}

export class AggregateHandle<Event = unknown> {
  constructor(
    readonly client: HarnessClient<Event>,
    readonly authority: Authority,
  ) {}

  submit<Payload>(command: Omit<ClientCommand<Payload>, "authority">): Promise<void> {
    return this.client.submit({ ...command, authority: this.authority });
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

function structuredSize(value: unknown): number {
  const encoded = JSON.stringify(canonicalStructuredValue(value, new Set()));
  return new TextEncoder().encode(encoded).byteLength;
}

function cloneStructuredValue(value: unknown, ancestors: Set<object>): unknown {
  if (
    value === null || typeof value === "string" || typeof value === "boolean" ||
    typeof value === "bigint"
  ) return value;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new TypeError("command contains a non-finite number");
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
      cloned[key] = cloneStructuredValue(descriptor.value, ancestors);
    }
    return cloned;
  } finally {
    ancestors.delete(value);
  }
}

function canonicalStructuredValue(value: unknown, ancestors: Set<object>): unknown {
  if (value === null || typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new TypeError("command contains a non-finite number");
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
  maximumCommands: number,
  maximumBytes: number,
): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const opening = factory.open(name, 2);
    opening.onupgradeneeded = event => {
      const database = opening.result;
      if (!database.objectStoreNames.contains("outbox")) database.createObjectStore("outbox", { keyPath: "operationId" });
      if (!database.objectStoreNames.contains("cursors")) database.createObjectStore("cursors", { keyPath: "authority" });
      if ((event as IDBVersionChangeEvent).oldVersion === 1) {
        migrateVersionOneOutbox(
          opening.transaction!.objectStore("outbox"),
          opening.transaction!,
          maximumCommands,
          maximumBytes,
        );
      }
    };
    opening.onsuccess = () => resolve(opening.result);
    opening.onerror = () => reject(opening.error ?? new Error("IndexedDB open failed"));
    opening.onblocked = () => reject(new Error("IndexedDB upgrade is blocked"));
  });
}

function migrateVersionOneOutbox(
  store: IDBObjectStore,
  transaction: IDBTransaction,
  maximumCommands: number,
  maximumBytes: number,
): void {
  let sequence = 0;
  let totalBytes = 0;
  const cursorRequest = store.openCursor();
  cursorRequest.onsuccess = () => {
    const cursor = cursorRequest.result;
    if (cursor === null) return;
    try {
      const record = cursor.value as { operationId: string; command: ClientCommand };
      const command = cloneStructuredValue(record.command, new Set()) as ClientCommand;
      const bytes = structuredSize(command);
      sequence += 1;
      totalBytes += bytes;
      if (sequence > maximumCommands || totalBytes > maximumBytes) {
        transaction.abort();
        return;
      }
      cursor.update({
        operationId: record.operationId,
        bytes,
        sequence,
        command,
      });
      cursor.continue();
    } catch {
      transaction.abort();
    }
  };
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
    const timeout = setTimeout(resolve, milliseconds);
    signal?.addEventListener(
      "abort",
      () => {
        clearTimeout(timeout);
        reject(signal.reason);
      },
      { once: true },
    );
  });
}
