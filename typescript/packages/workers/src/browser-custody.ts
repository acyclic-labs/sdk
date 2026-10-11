/// <reference lib="es2024.promise" />
import { encodeAccountPublicKey, inspectAccountHolder, prepareAccountBearer, type AccountIssuedCredential } from "./account.js";
const DATABASE_NAME = "acyclic-customer-credentials";
const DATABASE_VERSION = 1;
const ACTIVE = "active";
const CREDENTIALS = "credentials";
const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });

type Namespace = readonly [origin: string, accountId: string];
interface ActiveRecord { readonly revision: string; readonly credentialId: string | null }
interface StoredCredential {
  readonly revision: string;
  readonly credentialId: string;
  readonly birth: string;
  readonly certificate: string;
  readonly certificateExpiresAtUnixMillis: bigint;
  readonly publicKey: string;
  readonly signingKey: CryptoKey;
  readonly wrappingKey: CryptoKey;
  readonly sessionIv: Uint8Array<ArrayBuffer>;
  readonly encryptedSession: ArrayBuffer;
}

export interface BrowserCustodyOptions {
  readonly accountId: string;
  /** Shared across credentials; origin and account are separate IndexedDB keys. */
  readonly databaseName?: string;
}

/** Identity's signed public data plus a SQL login session, never a Workers bearer. */
export interface BrowserLoginResult {
  readonly birth: string;
  readonly certificate: string;
  readonly credentialId: string;
  readonly sqlSession: string;
}

export interface BrowserLeafIdentity {
  readonly accountId: string;
  readonly publicKey: string;
}

export type BrowserLoginIssuer = (identity: BrowserLeafIdentity, signal: AbortSignal) => Promise<BrowserLoginResult>;
export type BrowserRenewalIssuer = (identity: BrowserLeafIdentity & { readonly sqlSession: string }, signal: AbortSignal) => Promise<BrowserLoginResult>;

export class BrowserCustodyError extends Error {
  constructor(readonly code: "unavailable" | "closed" | "cancelled" | "superseded" | "invalid_credential", message: string) {
    super(message);
    this.name = "BrowserCustodyError";
  }
}

function namespace(options: BrowserCustodyOptions): Namespace {
  if (!globalThis.isSecureContext || !globalThis.crypto?.subtle || !globalThis.indexedDB
      || !globalThis.location || globalThis.location.origin === "null") {
    throw new BrowserCustodyError("unavailable", "Browser custody requires a secure origin, WebCrypto and IndexedDB");
  }
  if (!options.accountId || options.accountId.length > 256 || /[\u0000-\u001f\u007f]/u.test(options.accountId)) {
    throw new BrowserCustodyError("invalid_credential", "An explicit customer account is required");
  }
  return [globalThis.location.origin, options.accountId];
}

function cancelled(signal?: AbortSignal): void {
  if (signal?.aborted) throw new BrowserCustodyError("cancelled", "Browser credential operation cancelled");
}

function key(namespace: Namespace, credentialId: string): IDBValidKey {
  return [...namespace, credentialId];
}

function sessionBinding(namespace: Namespace, credentialId: string): Uint8Array<ArrayBuffer> {
  // This is storage AAD, not a certificate, bearer, claim or authorization schema.
  return encoder.encode(JSON.stringify(["acyclic-sql-session-v1", ...namespace, credentialId]));
}

function openDatabase(name: string, signal?: AbortSignal): Promise<IDBDatabase> {
  cancelled(signal);
  const { promise, resolve, reject } = Promise.withResolvers<IDBDatabase>();
  const request = indexedDB.open(name, DATABASE_VERSION);
  let settled = false;
  const cleanup = () => signal?.removeEventListener("abort", abort);
  const fail = (error: unknown) => {
    if (settled) return;
    settled = true;
    cleanup();
    reject(error);
  };
  const abort = () => fail(new BrowserCustodyError("cancelled", "Browser credential operation cancelled"));
  signal?.addEventListener("abort", abort, { once: true });
  request.onupgradeneeded = () => {
    if (settled || signal?.aborted) { request.transaction?.abort(); return; }
    request.result.createObjectStore(ACTIVE);
    request.result.createObjectStore(CREDENTIALS);
  };
  request.onblocked = () => fail(new BrowserCustodyError("unavailable", "Browser credential database upgrade is blocked"));
  request.onerror = () => fail(request.error);
  request.onsuccess = () => {
    if (settled || signal?.aborted) { request.result.close(); abort(); return; }
    settled = true;
    cleanup();
    const database = request.result;
    database.onversionchange = () => database.close();
    if (!database.objectStoreNames.contains(ACTIVE) || !database.objectStoreNames.contains(CREDENTIALS)) {
      database.close();
      reject(new BrowserCustodyError("invalid_credential", "Browser credential database schema mismatch"));
      return;
    }
    resolve(database);
  };
  return promise;
}

function requestResult<T>(request: IDBRequest<T>): Promise<T> {
  const { promise, resolve, reject } = Promise.withResolvers<T>();
  request.onsuccess = () => resolve(request.result);
  request.onerror = () => reject(request.error);
  return promise;
}

async function transaction<T>(database: IDBDatabase, mode: IDBTransactionMode,
  run: (transaction: IDBTransaction) => Promise<T>, signal?: AbortSignal): Promise<T> {
  cancelled(signal);
  const tx = database.transaction([ACTIVE, CREDENTIALS], mode, mode === "readwrite" ? { durability: "strict" } : {});
  const abort = () => { try { tx.abort(); } catch { /* The transaction has already completed. */ } };
  signal?.addEventListener("abort", abort, { once: true });
  const { promise: done, resolve, reject } = Promise.withResolvers<void>();
  tx.oncomplete = () => resolve();
  tx.onabort = () => reject(signal?.aborted
    ? new BrowserCustodyError("cancelled", "Browser credential operation cancelled")
    : tx.error ?? new BrowserCustodyError("invalid_credential", "Browser credential transaction aborted"));
  tx.onerror = () => { /* onabort owns the terminal failure. */ };
  // Attach a handler now: request failure can occur before run reaches its await.
  void done.catch(() => undefined);
  try {
    const result = await run(tx);
    await done;
    // A completed write is the publication point; never report cancellation after commit.
    if (mode === "readonly") cancelled(signal);
    return result;
  } catch (error) {
    abort();
    await done.catch(() => undefined);
    throw error;
  } finally {
    signal?.removeEventListener("abort", abort);
  }
}

function readActive(database: IDBDatabase, namespace: Namespace, signal?: AbortSignal): Promise<ActiveRecord | undefined> {
  return transaction(database, "readonly", tx => requestResult<ActiveRecord | undefined>(tx.objectStore(ACTIVE).get([...namespace])), signal);
}

function sameRevision(left: ActiveRecord | undefined, right: ActiveRecord | undefined): boolean {
  return left?.revision === right?.revision && left?.credentialId === right?.credentialId;
}

function assertKeys(record: StoredCredential): void {
  if (!(record.signingKey instanceof CryptoKey) || record.signingKey.type !== "private"
      || record.signingKey.extractable || record.signingKey.algorithm.name !== "Ed25519"
      || record.signingKey.usages.length !== 1 || record.signingKey.usages[0] !== "sign"
      || !(record.wrappingKey instanceof CryptoKey) || record.wrappingKey.type !== "secret"
      || record.wrappingKey.extractable || record.wrappingKey.algorithm.name !== "AES-GCM"
      || !record.wrappingKey.usages.includes("encrypt") || !record.wrappingKey.usages.includes("decrypt")
      || typeof record.publicKey !== "string"
      || !(record.sessionIv instanceof Uint8Array) || record.sessionIv.byteLength !== 12
      || !(record.encryptedSession instanceof ArrayBuffer) || record.encryptedSession.byteLength < 16) {
    throw new BrowserCustodyError("invalid_credential", "Browser credential contains invalid or extractable custody keys");
  }
}

async function encryptSession(namespace: Namespace, credentialId: string, wrappingKey: CryptoKey, session: string): Promise<Pick<StoredCredential, "sessionIv" | "encryptedSession">> {
  if (!session) throw new BrowserCustodyError("invalid_credential", "SQL login session is missing");
  const plaintext = encoder.encode(session);
  const sessionIv = crypto.getRandomValues(new Uint8Array(12));
  try {
    const encryptedSession = await crypto.subtle.encrypt({ name: "AES-GCM", iv: sessionIv,
      additionalData: sessionBinding(namespace, credentialId) }, wrappingKey, plaintext);
    return { sessionIv, encryptedSession };
  } finally { plaintext.fill(0); }
}

async function decryptSession(namespace: Namespace, record: StoredCredential): Promise<string> {
  const plaintext = new Uint8Array(await crypto.subtle.decrypt({ name: "AES-GCM", iv: record.sessionIv,
    additionalData: sessionBinding(namespace, record.credentialId) }, record.wrappingKey, record.encryptedSession));
  try { return decoder.decode(plaintext); }
  finally { plaintext.fill(0); }
}

function awaitWithCancellation<T>(pending: Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) {
    void pending.catch(() => undefined);
    return Promise.reject(new BrowserCustodyError("cancelled", "Browser credential operation cancelled"));
  }
  const { promise, resolve, reject } = Promise.withResolvers<T>();
  const abort = () => reject(new BrowserCustodyError("cancelled", "Browser credential operation cancelled"));
  signal.addEventListener("abort", abort, { once: true });
  void pending.then(value => { signal.removeEventListener("abort", abort); resolve(value); },
    error => { signal.removeEventListener("abort", abort); reject(error); });
  return promise;
}

async function publishCredential(database: IDBDatabase, namespace: Namespace,
  expected: ActiveRecord | undefined, record: StoredCredential, signal: AbortSignal): Promise<void> {
  await transaction(database, "readwrite", async tx => {
    const active = tx.objectStore(ACTIVE);
    const current = await requestResult<ActiveRecord | undefined>(active.get([...namespace]));
    if (!sameRevision(current, expected)) {
      throw new BrowserCustodyError("superseded", "Another login or deletion superseded this credential operation");
    }
    if (record.certificateExpiresAtUnixMillis <= BigInt(Date.now())) {
      throw new BrowserCustodyError("invalid_credential", "Identity certificate expired before credential publication");
    }
    const credentials = tx.objectStore(CREDENTIALS);
    if (current?.credentialId !== null && current?.credentialId !== undefined) {
      credentials.delete(key(namespace, current.credentialId));
    }
    credentials.put(record, key(namespace, record.credentialId));
    active.put({ revision: record.revision, credentialId: record.credentialId } satisfies ActiveRecord, [...namespace]);
  }, signal);
}

/** Opaque browser leaf custody. No method returns the private signing or wrapping key. */
export class BrowserCustomerCredential {
  readonly #database: IDBDatabase;
  readonly #namespace: Namespace;
  readonly #closing = new AbortController();
  #record: StoredCredential;
  #renewal: Promise<void> | undefined;

  private constructor(database: IDBDatabase, namespace: Namespace, record: StoredCredential) {
    this.#database = database;
    this.#namespace = namespace;
    this.#record = record;
  }

  /** Prepare the replacement entirely before one strict IndexedDB publication. */
  static async login(options: BrowserCustodyOptions, issue: BrowserLoginIssuer, signal?: AbortSignal): Promise<BrowserCustomerCredential> {
    const scope = namespace(options);
    const operation = signal ?? new AbortController().signal;
    const database = await openDatabase(options.databaseName ?? DATABASE_NAME, operation);
    try {
      const previous = await readActive(database, scope, operation);
      const [pair, wrappingKey] = await awaitWithCancellation(Promise.all([
        crypto.subtle.generateKey("Ed25519", false, ["sign", "verify"]),
        crypto.subtle.generateKey({ name: "AES-GCM", length: 256 }, false, ["encrypt", "decrypt"]),
      ]), operation);
      const publicKey = await encodeAccountPublicKey(new Uint8Array(await crypto.subtle.exportKey("raw", pair.publicKey)));
      cancelled(operation);
      const issued = await awaitWithCancellation(issue({ accountId: scope[1], publicKey }, operation), operation);
      const { birth, certificate, credentialId, sqlSession } = issued;
      const prepared = await prepareAccountBearer(publicKey, birth, certificate, credentialId, 1n);
      let certificateExpiresAtUnixMillis: bigint;
      try {
        if (prepared.accountId !== scope[1]) throw new BrowserCustodyError("invalid_credential", "Identity certificate names another account");
        certificateExpiresAtUnixMillis = prepared.certificateExpiresAtUnixMillis;
      } finally { prepared.dispose(); }
      cancelled(operation);
      const encrypted = await awaitWithCancellation(encryptSession(scope, credentialId, wrappingKey, sqlSession), operation);
      const record: StoredCredential = { revision: crypto.randomUUID(), credentialId, birth, certificate,
        certificateExpiresAtUnixMillis, publicKey, signingKey: pair.privateKey, wrappingKey, ...encrypted };
      assertKeys(record);
      await publishCredential(database, scope, previous, record, operation);
      return new BrowserCustomerCredential(database, scope, record);
    } catch (error) { database.close(); throw error; }
  }

  /** Restore keys by structured clone; even an expired certificate may renew using SQL. */
  static async open(options: BrowserCustodyOptions, signal?: AbortSignal): Promise<BrowserCustomerCredential | undefined> {
    const scope = namespace(options);
    const database = await openDatabase(options.databaseName ?? DATABASE_NAME, signal);
    try {
      const record = await transaction(database, "readonly", async tx => {
        const active = await requestResult<ActiveRecord | undefined>(tx.objectStore(ACTIVE).get([...scope]));
        if (!active?.credentialId) return undefined;
        const stored = await requestResult<StoredCredential | undefined>(tx.objectStore(CREDENTIALS).get(key(scope, active.credentialId)));
        if (!stored || stored.revision !== active.revision || stored.credentialId !== active.credentialId) {
          throw new BrowserCustodyError("invalid_credential", "Browser credential database is inconsistent");
        }
        return stored;
      }, signal);
      if (record === undefined) { database.close(); return undefined; }
      assertKeys(record);
      const metadata = await inspectAccountHolder(record.publicKey, record.birth, record.certificate);
      if (metadata.accountId !== scope[1]) throw new BrowserCustodyError("invalid_credential", "Identity certificate names another account");
      cancelled(signal);
      return new BrowserCustomerCredential(database, scope,
        { ...record, certificateExpiresAtUnixMillis: metadata.certificateExpiresAtUnixMillis });
    } catch (error) { database.close(); throw error; }
  }

  /** Delete the current account login and fence login attempts started before deletion. */
  static async delete(options: BrowserCustodyOptions, signal?: AbortSignal): Promise<void> {
    const scope = namespace(options);
    const database = await openDatabase(options.databaseName ?? DATABASE_NAME, signal);
    try { await removeCredential(database, scope, undefined, signal); }
    finally { database.close(); }
  }

  get accountId(): string { return this.#namespace[1]; }
  get credentialId(): string { return this.#record.credentialId; }
  get publicKey(): string { return this.#record.publicKey; }
  /** Admitted public metadata for renewal scheduling; Rust rechecks the actual certificate. */
  get certificateExpiresAtUnixMillis(): bigint { return this.#record.certificateExpiresAtUnixMillis; }

  async mint(lifetimeSeconds: bigint, signal?: AbortSignal): Promise<AccountIssuedCredential> {
    const operation = this.#operation(signal);
    const record = this.#record;
    await this.#assertActive(record, operation);
    const prepared = await prepareAccountBearer(record.publicKey, record.birth,
      record.certificate, record.credentialId, lifetimeSeconds);
    try {
      if (prepared.accountId !== this.accountId) throw new BrowserCustodyError("invalid_credential", "Identity certificate names another account");
      await this.#assertActive(record, operation);
      const signature = new Uint8Array(await awaitWithCancellation(
        crypto.subtle.sign({ name: "Ed25519" }, record.signingKey, prepared.signingBytes), operation));
      await this.#assertActive(record, operation);
      const issued = await prepared.finish(signature);
      await this.#assertActive(record, operation);
      if (issued.expiresAtUnixMillis <= BigInt(Date.now())) {
        throw new BrowserCustodyError("invalid_credential", "Bearer expired before browser custody released it");
      }
      return issued;
    } finally { prepared.dispose(); }
  }

  /** Session plaintext exists only for this explicit SQL/Identity operation, never in storage. */
  async withSqlSession<T>(use: (sqlSession: string, signal: AbortSignal) => Promise<T>, signal?: AbortSignal): Promise<T> {
    const operation = this.#operation(signal);
    const record = this.#record;
    await this.#assertActive(record, operation);
    const session = await awaitWithCancellation(decryptSession(this.#namespace, record), operation);
    await this.#assertActive(record, operation);
    const result = await awaitWithCancellation(use(session, operation), operation);
    await this.#assertActive(record, operation);
    return result;
  }

  /** One in-flight renewal per handle, using the same public identity and actual SQL session. */
  async renew(issue: BrowserRenewalIssuer, signal?: AbortSignal): Promise<void> {
    const operation = this.#operation(signal);
    if (this.#renewal !== undefined) return awaitWithCancellation(this.#renewal, operation);
    const record = this.#record;
    const pending = (async () => {
      await this.#assertActive(record, operation);
      const sqlSession = await awaitWithCancellation(decryptSession(this.#namespace, record), operation);
      await this.#assertActive(record, operation);
      const issued = await awaitWithCancellation(issue({ accountId: this.accountId,
        publicKey: record.publicKey, sqlSession }, operation), operation);
      const { birth, certificate, credentialId, sqlSession: replacementSession } = issued;
      const prepared = await prepareAccountBearer(record.publicKey, birth, certificate, credentialId, 1n);
      let certificateExpiresAtUnixMillis: bigint;
      try {
        if (prepared.accountId !== this.accountId) throw new BrowserCustodyError("invalid_credential", "Identity certificate names another account");
        certificateExpiresAtUnixMillis = prepared.certificateExpiresAtUnixMillis;
      } finally { prepared.dispose(); }
      await this.#assertActive(record, operation);
      const encrypted = await awaitWithCancellation(encryptSession(this.#namespace, credentialId,
        record.wrappingKey, replacementSession), operation);
      const replacement: StoredCredential = { ...record, ...encrypted, birth, certificate, credentialId,
        revision: crypto.randomUUID(), certificateExpiresAtUnixMillis };
      await publishCredential(this.#database, this.#namespace,
        { revision: record.revision, credentialId: record.credentialId }, replacement, operation);
      this.#record = replacement;
    })();
    this.#renewal = pending;
    try { await pending; }
    finally { if (this.#renewal === pending) this.#renewal = undefined; }
  }

  /** Delete only this login; an older handle cannot delete a replacement login. */
  async delete(signal?: AbortSignal): Promise<void> {
    const operation = this.#operation(signal);
    const record = this.#record;
    await removeCredential(this.#database, this.#namespace,
      { revision: record.revision, credentialId: record.credentialId }, operation);
    this.close();
  }

  /** Release this local handle without deleting the persisted login. */
  close(): void {
    this.#closing.abort();
    this.#database.close();
  }

  #operation(signal?: AbortSignal): AbortSignal {
    if (this.#closing.signal.aborted) throw new BrowserCustodyError("closed", "Browser credential handle is closed");
    const operation = signal === undefined ? this.#closing.signal : AbortSignal.any([this.#closing.signal, signal]);
    cancelled(operation);
    return operation;
  }

  async #assertActive(record: StoredCredential, signal: AbortSignal): Promise<void> {
    cancelled(signal);
    if (this.#record.revision !== record.revision) {
      throw new BrowserCustodyError("superseded", "Browser credential was renewed during this operation");
    }
    const active = await readActive(this.#database, this.#namespace, signal);
    if (!sameRevision(active, { revision: record.revision, credentialId: record.credentialId })) {
      throw new BrowserCustodyError("superseded", "Browser credential was deleted or replaced");
    }
  }
}

async function removeCredential(database: IDBDatabase, namespace: Namespace,
  expected?: ActiveRecord, signal?: AbortSignal): Promise<void> {
  await transaction(database, "readwrite", async tx => {
    const active = tx.objectStore(ACTIVE);
    const current = await requestResult<ActiveRecord | undefined>(active.get([...namespace]));
    if (expected !== undefined && !sameRevision(current, expected)) {
      throw new BrowserCustodyError("superseded", "Browser credential was deleted or replaced");
    }
    if (current?.credentialId !== null && current?.credentialId !== undefined) {
      tx.objectStore(CREDENTIALS).delete(key(namespace, current.credentialId));
    }
    active.put({ revision: crypto.randomUUID(), credentialId: null } satisfies ActiveRecord, [...namespace]);
  }, signal);
}
