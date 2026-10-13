import { type ClientCommand, type ReplayCursor, validateCursor, authorityKey } from "./client.js";
import { type OperationId, type Authority } from "./index.js";
import { retainedDataBytes, cloneStructuredValue, assertOutboxSafe } from "./client-data.js";
import { requestBound } from "./client-requests.js";
import { NativeContracts } from "./native-contracts.js";

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
    expectedCursor?: ReplayCursor | null, expectedCheckpointVersion?: bigint | null): Promise<bigint>;
  loadRecovery(): Promise<readonly ClientRecoveryRecord[]>;
}
export interface ClientCheckpoint {
  readonly identity: string;
  readonly state: unknown;
}

export class ClientCheckpointConflict extends Error {}
export interface ClientRecoveryRecord {
  readonly authority: string;
  readonly cursor: ReplayCursor;
  readonly checkpointVersion: bigint;
  readonly checkpoint?: ClientCheckpoint | undefined;
}

const clientMetadataKey = "@harness/outbox-sequence";
function clientMetadata(value: unknown) {
  retainedDataBytes(value, 4096);
  const record = value as { readonly authority: string; readonly sequence: number; readonly checkpointVersion: bigint };
  if (record?.authority !== clientMetadataKey || Object.keys(record).length !== 3 ||
      !Number.isSafeInteger(record.sequence) || record.sequence < 0 ||
      typeof record.checkpointVersion !== "bigint" || record.checkpointVersion < 0n ||
      record.checkpointVersion > 0xffff_ffff_ffff_ffffn) throw new Error("invalid client allocation metadata");
  return record;
}

export class MemoryOutbox implements OutboxStore {
  readonly #commands = new Map<OperationId, Readonly<{ command: ClientCommand; bytes: number }>>();
  #bytes = 0;

  constructor(readonly maximumCommands = 1024, readonly maximumBytes = 16 * 1024 * 1024) {
    requestBound(maximumCommands, "maximumCommands");
    requestBound(maximumBytes, "maximumBytes");
  }

  async load(): Promise<readonly ClientCommand[]> {
    return [...this.#commands.values()].map(({ command }) => cloneStructuredValue(command, new Set()) as ClientCommand);
  }

  async put(command: ClientCommand): Promise<void> {
    const bytes = retainedDataBytes(command, this.maximumBytes);
    if ((!this.#commands.has(command.operationId) && this.#commands.size >= this.maximumCommands) ||
        bytes > this.maximumBytes - this.#bytes + (this.#commands.get(command.operationId)?.bytes ?? 0)) {
      throw new RangeError("memory outbox capacity exceeded");
    }
    const admitted = await assertOutboxSafe(command);
    // Validation awaits WASM; another caller may have admitted work meanwhile.
    const ownedBytes = retainedDataBytes(admitted, this.maximumBytes);
    if ((!this.#commands.has(admitted.operationId) && this.#commands.size >= this.maximumCommands) ||
        ownedBytes > this.maximumBytes - this.#bytes + (this.#commands.get(admitted.operationId)?.bytes ?? 0)) {
      throw new RangeError("memory outbox capacity exceeded");
    }
    this.#bytes += ownedBytes - (this.#commands.get(admitted.operationId)?.bytes ?? 0);
    this.#commands.set(admitted.operationId, { command: admitted, bytes: ownedBytes });
  }

  async delete(operationId: OperationId): Promise<void> {
    this.#bytes -= this.#commands.get(operationId)?.bytes ?? 0;
    this.#commands.delete(operationId);
  }
}

export class MemoryCursorStore implements CursorStore {
  readonly #cursors = new Map<string, ReplayCursor>();

  constructor(readonly maximumCursors = 64) { requestBound(maximumCursors, "maximumCursors"); }

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

export class IndexedDbClientStore implements AtomicClientStateStore {
  #database: Promise<IDBDatabase> | undefined;
  readonly #factory: IDBFactory | undefined;
  readonly #name: string;
  readonly #limits;

  constructor(options: IndexedDbClientStoreOptions) {
    this.#limits = Object.freeze({
      maximumCommands: options.maximumCommands ?? 1024,
      maximumBytes: options.maximumBytes ?? 16 * 1024 * 1024,
      maximumResidentBytes: options.maximumResidentBytes ?? 16 * 1024 * 1024,
      maximumCursors: options.maximumCursors ?? 64,
      maximumCheckpointBytes: options.maximumCheckpointBytes ?? 256 * 1024,
    });
    for (const [name, value] of Object.entries(this.#limits)) requestBound(value, name);
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
      if (commands.length > this.#limits.maximumCommands) throw new RangeError("outbox capacity exceeded");
      retainedDataBytes(commands, this.#limits.maximumResidentBytes);
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
    if (count > this.#limits.maximumCommands) throw new RangeError("outbox capacity exceeded");
    if (count !== indexedCount) throw new Error("outbox contains an unindexed record");
    const through = throughSequence ?? (last === null ? 0 : outboxRecord(last.value).sequence);
    if (afterSequence >= through) return { commands: [], afterSequence: through, throughSequence: through };
    const records: OutboxRecord[] = [];
    let bytes = 0;
    let after = afterSequence;
    await visitOutbox(index, IDBKeyRange.bound(afterSequence, through, true, false), record => {
      bytes += retainedDataBytes(record.command, this.#limits.maximumResidentBytes - bytes);
      records.push(record);
      after = record.sequence;
      return records.length < Math.min(64, this.#limits.maximumCommands);
    });
    // An empty tail (e.g. another tab acknowledged it) completes this fixed cut.
    if (records.length < Math.min(64, this.#limits.maximumCommands)) after = through;
    return { commands: await Promise.all(records.map(record => assertOutboxSafe(record.command))),
      afterSequence: after, throughSequence: through };
  }

  async put(command: ClientCommand): Promise<void> {
    retainedDataBytes(command, this.#limits.maximumResidentBytes);
    const storedCommand = await assertOutboxSafe(command);
    const residentBytes = retainedDataBytes(storedCommand, this.#limits.maximumResidentBytes);
    const contracts = await NativeContracts.create();
    const bytes = contracts.encodeCanonicalJson(cloneStructuredValue(storedCommand, new Set(), false, true)).byteLength;
    const database = await this.#open();
    const transaction = database.transaction(["outbox", "cursors"], "readwrite");
    const store = transaction.objectStore("outbox");
    const cursors = transaction.objectStore("cursors");
    const [storedCount, indexedCount, rawMetadata] = await Promise.all([request(store.count()),
      request(store.index("sequence").count()), request(cursors.get(clientMetadataKey))]);
    const metadata = clientMetadata(rawMetadata);
    if (storedCount !== indexedCount || storedCount > this.#limits.maximumCommands) {
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
      const size = retainedDataBytes(record.command, this.#limits.maximumResidentBytes);
      const encodedSize = contracts.encodeCanonicalJson(cloneStructuredValue(record.command, new Set(), false, true)).byteLength;
      if (record.bytes !== encodedSize) throw new Error("invalid outbox byte accounting");
      if (record.operationId === storedCommand.operationId) existing = record;
      else { retained += size; encoded += encodedSize; }
      if (count > this.#limits.maximumCommands || retained > this.#limits.maximumResidentBytes || encoded > this.#limits.maximumBytes) {
        throw new RangeError("IndexedDB outbox capacity exceeded");
      }
      return true;
    });
    if (count + (existing === undefined ? 1 : 0) > this.#limits.maximumCommands ||
        retained > this.#limits.maximumResidentBytes || encoded > this.#limits.maximumBytes) {
      transaction.abort();
      throw new RangeError("IndexedDB outbox capacity exceeded");
    }
    if (metadata.sequence < maximum) { transaction.abort(); throw new Error("outbox allocation watermark regressed"); }
    const sequence = existing?.sequence ?? metadata.sequence + 1;
    if (!Number.isSafeInteger(sequence)) { transaction.abort(); throw new RangeError("outbox sequence exhausted"); }
    store.put({ operationId: storedCommand.operationId, bytes, sequence, command: storedCommand });
    if (existing === undefined) cursors.put({ ...metadata, sequence });
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
    const all = await request<Array<{ authority: string; generation: string; revision: bigint; checkpointVersion: bigint; checkpoint?: ClientCheckpoint }>>(
      database.transaction("cursors").objectStore("cursors").getAll(undefined, this.#limits.maximumCursors + 2));
    const metadata = clientMetadata(all.find(record => record.authority === clientMetadataKey));
    const records = all.filter(record => record.authority !== clientMetadataKey);
    if (records.length > this.#limits.maximumCursors) throw new RangeError("cursor capacity exceeded");
    return records.map(record => {
      validateCursor(record);
      if (typeof record.checkpointVersion !== "bigint" || record.checkpointVersion <= 0n ||
          record.checkpointVersion > metadata.checkpointVersion) throw new Error("invalid checkpoint version");
      retainedDataBytes({ authority: record.authority, generation: record.generation, revision: record.revision }, 4096);
      if (record.checkpoint !== undefined) retainedDataBytes(record.checkpoint, this.#limits.maximumCheckpointBytes, true);
      return { authority: record.authority, cursor: { generation: record.generation, revision: record.revision },
        checkpointVersion: record.checkpointVersion, checkpoint: record.checkpoint };
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
    expectedCursor?: ReplayCursor | null, expectedCheckpointVersion?: bigint | null): Promise<bigint> {
    validateCursor(cursor);
    retainedDataBytes({ authority, cursor }, 4096);
    if (checkpoint !== undefined) retainedDataBytes(checkpoint, this.#limits.maximumCheckpointBytes, true);
    const ownedCheckpoint = checkpoint === undefined ? undefined : cloneStructuredValue(checkpoint, new Set(), true);
    const database = await this.#open();
    const transaction = database.transaction(["outbox", "cursors"], "readwrite");
    const cursors = transaction.objectStore("cursors");
    const key = authorityKey(authority);
    const [count, existing, rawMetadata] = await Promise.all([request(cursors.count()),
      request<{ generation: string; revision: bigint; checkpointVersion: bigint; checkpoint?: ClientCheckpoint } | undefined>(cursors.get(key)),
      request(cursors.get(clientMetadataKey))]);
    const metadata = clientMetadata(rawMetadata);
    if (expectedCursor !== undefined && (expectedCursor === null ? existing !== undefined :
      existing?.generation !== expectedCursor.generation || existing.revision !== expectedCursor.revision)) {
      transaction.abort();
      throw new ClientCheckpointConflict("durable projection cut changed concurrently");
    }
    if (expectedCheckpointVersion !== undefined && (expectedCheckpointVersion === null ? existing !== undefined :
      existing?.checkpointVersion !== expectedCheckpointVersion)) {
      transaction.abort();
      throw new ClientCheckpointConflict("durable checkpoint was replaced concurrently");
    }
    if (checkpoint === undefined && existing?.checkpoint !== undefined &&
        (existing.generation !== cursor.generation || existing.revision !== cursor.revision)) {
      transaction.abort();
      throw new ClientCheckpointConflict("cursor advancement requires its projection checkpoint");
    }
    if (existing === undefined && count - 1 >= this.#limits.maximumCursors) {
      transaction.abort();
      throw new RangeError("cursor capacity exceeded");
    }
    if (metadata.checkpointVersion === 0xffff_ffff_ffff_ffffn) {
      transaction.abort(); throw new RangeError("checkpoint allocation exhausted");
    }
    const checkpointVersion = metadata.checkpointVersion + 1n;
    if (operationId !== undefined) transaction.objectStore("outbox").delete(operationId);
    cursors.put({
      authority: key,
      generation: cursor.generation,
      revision: cursor.revision,
      checkpointVersion,
      checkpoint: ownedCheckpoint ?? existing?.checkpoint,
    });
    cursors.put({ ...metadata, checkpointVersion });
    await transactionDone(transaction);
    return checkpointVersion;
  }
}

function openClientDatabase(
  factory: IDBFactory,
  name: string,
): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    let rejected = false;
    const fail = (error: unknown): void => { rejected = true; reject(error); };
    const opening = factory.open(name, 3);
    opening.onupgradeneeded = event => {
      if ((event as IDBVersionChangeEvent).oldVersion !== 0) {
        opening.transaction?.abort();
        return;
      }
      const database = opening.result;
      database.createObjectStore("outbox", { keyPath: "operationId" }).createIndex("sequence", "sequence", { unique: true });
      database.createObjectStore("cursors", { keyPath: "authority" }).put({
        authority: clientMetadataKey, sequence: 0, checkpointVersion: 0n,
      });
    };
    opening.onsuccess = async () => {
      const database = opening.result;
      if (rejected) { database.close(); return; }
      try {
        const index = database.transaction("outbox").objectStore("outbox").index("sequence");
        if (index.keyPath !== "sequence" || !index.unique) throw new Error("invalid sequence index");
        clientMetadata(await request(database.transaction("cursors").objectStore("cursors").get(clientMetadataKey)));
        resolve(database);
      } catch (error) {
        database.close();
        fail(error);
      }
    };
    opening.onerror = () => fail(opening.error ?? new Error("IndexedDB open failed"));
    opening.onblocked = () => fail(new Error("IndexedDB upgrade is blocked"));
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
