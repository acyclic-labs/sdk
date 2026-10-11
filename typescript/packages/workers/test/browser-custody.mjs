// Actual Chromium + generated Rust/WASM custody qualification. The caller supplies
// a local canonical Rust fixture issuer/verifier or genuine Identity transport;
// a fixture run is never evidence of a live Identity service request.
import { BrowserCustomerCredential } from "../dist/browser-custody.js";

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

async function rejects(operation, message, code) {
  try { await operation(); }
  catch (error) {
    if (code !== undefined) assert(error.code === code, `${message}: unexpected ${error.code ?? error.name}`);
    return;
  }
  throw new Error(message);
}

function idbResult(request) {
  const { promise, resolve, reject } = Promise.withResolvers();
  request.onsuccess = () => resolve(request.result);
  request.onerror = () => reject(request.error);
  return promise;
}

async function storedCredential(options) {
  const database = await idbResult(indexedDB.open(options.databaseName, 1));
  try {
    const tx = database.transaction(["active", "credentials"], "readonly");
    const scope = [location.origin, options.accountId];
    const active = await idbResult(tx.objectStore("active").get(scope));
    return active?.credentialId === null ? undefined : await idbResult(tx.objectStore("credentials").get([...scope, active.credentialId]));
  } finally { database.close(); }
}

/** All callbacks must use real canonical Rust-produced certificates and verification. */
export async function runBrowserCustodySmoke({ accountId, issue, issueExpiring, verifyBearer }) {
  const options = { accountId, databaseName: `acyclic-browser-custody-smoke-${crypto.randomUUID()}` };
  const handles = [];
  let initial;
  let restored;
  let expiring;
  let originalLogin;
  try {
    initial = await BrowserCustomerCredential.login(options, async (identity, signal) => {
      originalLogin = await issue(identity, signal);
      return originalLogin;
    });
    handles.push(initial);
    const originalPublicKey = initial.publicKey;
    const stored = await storedCredential(options);
    assert(stored.signingKey instanceof CryptoKey && stored.signingKey.extractable === false, "Private Ed25519 key is not an actual nonextractable CryptoKey");
    assert(stored.wrappingKey instanceof CryptoKey && stored.wrappingKey.extractable === false, "Session key is not an actual nonextractable CryptoKey");
    await rejects(() => crypto.subtle.exportKey("pkcs8", stored.signingKey), "Private signing key was exportable");
    await rejects(() => crypto.subtle.exportKey("raw", stored.wrappingKey), "Session wrapping key was exportable");
    assert(!Object.hasOwn(stored, "sqlSession") && !Object.hasOwn(stored, "session"), "SQL session plaintext was stored");
    assert(stored.encryptedSession instanceof ArrayBuffer && stored.encryptedSession.byteLength >= 16, "SQL session ciphertext was absent");
    assert(!new TextDecoder().decode(stored.encryptedSession).includes(originalLogin.sqlSession), "SQL session appeared in ciphertext as plaintext");
    assert(await initial.withSqlSession(async session => session === originalLogin.sqlSession), "Encrypted session did not decrypt for SQL use");
    const first = await initial.mint(30n);
    assert(await verifyBearer(first.bearer, originalPublicKey), "Canonical Rust verifier rejected actual browser signature");
    assert(first.expiresAtUnixMillis <= initial.certificateExpiresAtUnixMillis, "Bearer exceeded its actual certificate expiry");

    initial.close();
    await rejects(() => initial.mint(30n), "Closed handle could issue a bearer", "closed");
    restored = await BrowserCustomerCredential.open(options);
    assert(restored !== undefined && restored.publicKey === originalPublicKey, "IndexedDB reopen changed the public identity");
    handles.push(restored);
    const reopenedBearer = await restored.mint(30n);
    assert(await verifyBearer(reopenedBearer.bearer, originalPublicKey), "Restored nonextractable key could not mint a canonical bearer");
    assert(await restored.withSqlSession(async session => session === originalLogin.sqlSession), "Restored wrapping key could not decrypt the SQL session");
    assert(await BrowserCustomerCredential.open({ ...options, accountId: `${accountId}-other` }) === undefined,
      "Another account namespace reopened this login");

    await rejects(() => BrowserCustomerCredential.login(options, async () => { throw new Error("Intentional issuer rejection before publication"); }), "Failed replacement unexpectedly succeeded");
    assert((await restored.mint(30n)).bearer.length > 0, "Failed replacement destroyed the prior login");
    await rejects(() => BrowserCustomerCredential.login(options, async () => originalLogin), "Certificate for another public key was accepted");
    assert((await restored.mint(30n)).bearer.length > 0, "Invalid certificate replacement destroyed the prior login");
    const originalPut = IDBObjectStore.prototype.put;
    let abortedPublication = false;
    IDBObjectStore.prototype.put = function (value, key) {
      const request = originalPut.call(this, value, key);
      if (this.name === "credentials" && !abortedPublication) {
        abortedPublication = true;
        this.transaction.abort(); // Actual IndexedDB rollback after queuing replacement.
      }
      return request;
    };
    try {
      await rejects(() => BrowserCustomerCredential.login(options, issue), "Aborted storage replacement unexpectedly succeeded");
      assert(abortedPublication, "Replacement did not reach the actual IndexedDB publication boundary");
    } finally { IDBObjectStore.prototype.put = originalPut; }
    assert(await verifyBearer((await restored.mint(30n)).bearer, originalPublicKey),
      "Aborted IndexedDB publication removed the previous usable login");
    const cancelledLoginGate = Promise.withResolvers();
    const cancelledLoginStarted = Promise.withResolvers();
    const loginCancellation = new AbortController();
    const cancelledLogin = BrowserCustomerCredential.login(options, async (identity, signal) => {
      const result = await issue(identity, signal);
      cancelledLoginStarted.resolve();
      await cancelledLoginGate.promise;
      return result;
    }, loginCancellation.signal);
    const rejectedCancelledLogin = rejects(() => cancelledLogin, "Cancelled replacement unexpectedly succeeded", "cancelled");
    await cancelledLoginStarted.promise;
    loginCancellation.abort();
    await rejectedCancelledLogin;
    cancelledLoginGate.resolve();
    assert((await restored.mint(30n)).bearer.length > 0, "Cancelled replacement destroyed the prior login");

    const cancelled = new AbortController();
    const cancelledMint = restored.mint(30n, cancelled.signal);
    cancelled.abort();
    await rejects(() => cancelledMint, "Cancelled operation released a bearer", "cancelled");

    let renewalCalls = 0;
    const renewalGate = Promise.withResolvers();
    const renewalStarted = Promise.withResolvers();
    const renew = async (identity, signal) => {
      renewalCalls += 1;
      assert(identity.publicKey === originalPublicKey && identity.sqlSession === originalLogin.sqlSession, "Renewal lost the existing identity/session binding");
      renewalStarted.resolve();
      await renewalGate.promise;
      return issue(identity, signal);
    };
    const renewalA = restored.renew(renew);
    const renewalB = restored.renew(renew);
    await renewalStarted.promise;
    renewalGate.resolve();
    await Promise.all([renewalA, renewalB]);
    assert(renewalCalls === 1, "Concurrent renewal dispatched more than once");
    assert(restored.publicKey === originalPublicKey, "Renewal recreated the customer identity");
    assert(await verifyBearer((await restored.mint(30n)).bearer, originalPublicKey), "Renewed credential could not sign canonically");

    const lateLoginGate = Promise.withResolvers();
    const lateLoginStarted = Promise.withResolvers();
    const lateLogin = BrowserCustomerCredential.login(options, async (identity, signal) => {
      const result = await issue(identity, signal);
      lateLoginStarted.resolve();
      await lateLoginGate.promise;
      return result;
    });
    // Attach failure ownership before deleting while the issuer callback is pending.
    const rejectedLateLogin = rejects(() => lateLogin, "Pending login resurrected a deleted credential", "superseded");
    await lateLoginStarted.promise;
    const retainedBeforeDelete = await BrowserCustomerCredential.open(options);
    handles.push(retainedBeforeDelete);
    await BrowserCustomerCredential.delete(options);
    lateLoginGate.resolve();
    await rejectedLateLogin;
    await rejects(() => restored.mint(30n), "Deleted login could still issue a bearer", "superseded");
    await rejects(() => retainedBeforeDelete.withSqlSession(async () => true), "Deleted login could still release its SQL session", "superseded");
    assert(await BrowserCustomerCredential.open(options) === undefined, "Deleted login reopened");
    assert(await storedCredential(options) === undefined, "Deleted credential keys/ciphertext remained active");

    expiring = await BrowserCustomerCredential.login(options, issueExpiring);
    handles.push(expiring);
    const originalSign = SubtleCrypto.prototype.sign;
    const signed = Promise.withResolvers();
    const signatureGate = Promise.withResolvers();
    SubtleCrypto.prototype.sign = async function (...args) {
      const actualSignature = await originalSign.apply(this, args);
      signed.resolve();
      await signatureGate.promise; // Hold a real signature across actual certificate expiry.
      return actualSignature;
    };
    try {
      const pendingExpiredBearer = expiring.mint(30n);
      const rejectedExpiredBearer = rejects(() => pendingExpiredBearer,
        "Certificate that expired during actual signing released a stale bearer");
      await Promise.race([signed.promise, pendingExpiredBearer.then(
        () => { throw new Error("Expiry regression released a bearer before its signing checkpoint"); },
        error => { throw new Error("Expiry regression did not reach actual WebCrypto signing", { cause: error }); },
      )]);
      const delay = Number(expiring.certificateExpiresAtUnixMillis - BigInt(Date.now()) + 50n);
      if (delay > 0) {
        const timeout = Promise.withResolvers();
        setTimeout(timeout.resolve, delay);
        await timeout.promise;
      }
      signatureGate.resolve();
      await rejectedExpiredBearer;
    } finally {
      signatureGate.resolve();
      SubtleCrypto.prototype.sign = originalSign;
    }
    await rejects(() => expiring.mint(30n), "Expired certificate issued a stale bearer");
    const reopenedExpired = await BrowserCustomerCredential.open(options);
    handles.push(reopenedExpired);
    assert(reopenedExpired !== undefined, "Expired login could not reopen for genuine SQL renewal");
    await rejects(() => reopenedExpired.mint(30n), "Reopened expired certificate issued a bearer");
    await reopenedExpired.withSqlSession(async (sqlSession, signal) => {
      assert(sqlSession.length > 0 && !signal.aborted, "Expired login lost its encrypted SQL renewal session");
    });

    return { status: "passed", origin: location.origin, accountId,
      checks: ["real nonextractable Ed25519/AES-GCM keys", "canonical Rust/WASM signed bearer", "encrypted SQL session", "IndexedDB close/reopen", "account namespace isolation", "issuer/invalid/cancelled/transaction-aborted replacement preservation", "own-public-key rejection", "closed/cancelled mint fencing", "single-flight renewal", "deletion fences in-flight login and retained handles", "expiry during actual signing and after reopen"] };
  } finally {
    for (const handle of handles) handle?.close();
    await BrowserCustomerCredential.delete(options);
    await idbResult(indexedDB.deleteDatabase(options.databaseName));
  }
}
