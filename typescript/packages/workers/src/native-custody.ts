import type { NativePendingCustomerLeaf as RawPending, NativeCustomerCredential as RawCredential, NativeIdentityResponse, WorkersCancellation as NativeCancellation } from "../generated/native/binding.js";
import type { AccountHolderMetadata, AccountIssuedCredential } from "./account.js";
import { inspectAccountHolder } from "./account.js";
import { isNodeRuntime, loadWorkersNativeModule } from "./binding.js";
import { WorkersTransportError } from "./client.js";

/** Public signed tuple; it contains neither a private key nor a SQL session. */
export interface NativeCustomerPublicTuple {
  readonly origin: string;
  readonly publicKey: string;
  readonly birth: string;
  readonly certificate: string;
}
export interface NativeCustomerEnrollment {
  readonly origin: string;
  readonly birth: string;
  readonly certificate: string;
  /** Transient device-login session sealed by Rust, not a Workers bearer. */
  readonly sqlSession: string;
}
export interface NativeCertificateRenewal { readonly birth: string; readonly certificate: string }
export interface NativeIdentityRequest {
  readonly method: string;
  readonly path: string;
  readonly body?: Uint8Array;
  readonly signal?: AbortSignal;
}
export type NativeIdentityResult = Pick<NativeIdentityResponse, "status"> & { readonly body: Uint8Array<ArrayBuffer> };
/** An opaque OS handle. Local close/deletion is not server revocation. */
export interface NativeCustomerCredential {
  readonly origin: string;
  readonly publicKey: string;
  readonly metadata: AccountHolderMetadata;
  mint(credentialId: string, lifetimeSeconds: bigint, signal?: AbortSignal): Promise<AccountIssuedCredential>;
  requestIdentity(request: NativeIdentityRequest): Promise<NativeIdentityResult>;
  /** Persist the returned public tuple before explicitly deleting the old namespace. */
  renew(certificate: NativeCertificateRenewal): Promise<NativeCustomerCredential>;
  delete(): Promise<void>;
  close(): void;
}
export interface PendingNativeCustomerLeaf {
  readonly publicKey: string;
  commit(enrollment: NativeCustomerEnrollment): Promise<NativeCustomerCredential>;
  dispose(): void;
}
async function binding() {
  if (!isNodeRuntime()) throw new WorkersTransportError("native OS customer custody requires Node", "configuration");
  const native = await loadWorkersNativeModule();
  if (native === undefined || typeof native.generateNativeCustomerLeaf !== "function"
    || typeof native.openNativeCustomerCredential !== "function") {
    throw new WorkersTransportError("native customer custody ABI unavailable", "configuration");
  }
  return native;
}
async function cancellable<T>(signal: AbortSignal | undefined, work: (token: NativeCancellation) => Promise<T>): Promise<T> {
  if (signal?.aborted) throw new DOMException("customer operation cancelled", "AbortError");
  const native = await binding();
  const token = new native.WorkersCancellation();
  const abort = () => token.cancel();
  signal?.addEventListener("abort", abort, { once: true });
  if (signal?.aborted) abort();
  try {
    const result = await work(token);
    if (signal?.aborted) throw new DOMException("customer operation cancelled", "AbortError");
    return result;
  } catch (error) {
    if (signal?.aborted) throw new DOMException("customer operation cancelled", "AbortError");
    throw error;
  } finally { signal?.removeEventListener("abort", abort); }
}
async function restored(raw: RawCredential, tuple: NativeCustomerPublicTuple): Promise<NativeCustomerCredential> {
  const metadata = await inspectAccountHolder(raw.publicKey, tuple.birth, tuple.certificate);
  let handle: RawCredential | undefined = raw;
  const current = () => {
    if (handle === undefined) throw new WorkersTransportError("native customer credential closed", "configuration");
    return handle;
  };
  return Object.freeze({
    origin: raw.origin,
    publicKey: metadata.publicKey,
    metadata,
    async mint(credentialId: string, lifetimeSeconds: bigint, signal?: AbortSignal): Promise<AccountIssuedCredential> {
      const handle = current();
      return cancellable(signal, async token => {
        const issued = await handle.mint(tuple.birth, tuple.certificate, credentialId, lifetimeSeconds, token);
        return Object.freeze({ bearer: issued.bearer, expiresAtUnixMillis: issued.expiresAtUnixMillis,
          certificateExpiresAtUnixMillis: issued.certificateExpiresAtUnixMillis });
      });
    },
    async requestIdentity(request: NativeIdentityRequest): Promise<NativeIdentityResult> {
      const handle = current();
      const { method, path, signal } = request;
      // Snapshot caller bytes before imports or custody awaits; never redispatch a mutation.
      const body = request.body?.slice();
      // Node Buffer is a native binding value and must not enter browser/Workerd imports.
      const { Buffer } = await import("node:buffer");
      const response = await cancellable(signal, token => handle.requestIdentity(method, path,
        body === undefined ? undefined : Buffer.from(body.buffer, body.byteOffset, body.byteLength), token));
      if (!(response.body.buffer instanceof ArrayBuffer)) {
        throw new WorkersTransportError("native identity response must own an ArrayBuffer", "configuration");
      }
      // Rust transfers an owned Vec into N-API Buffer; the guard admits normal memory without copying.
      const owned = response.body as Uint8Array<ArrayBuffer>;
      return { status: response.status, body: owned };
    },
    async renew(certificate: NativeCertificateRenewal): Promise<NativeCustomerCredential> {
      const { birth, certificate: signedCertificate } = certificate;
      const renewed = await current().renew(birth, signedCertificate);
      return restored(renewed, { origin: tuple.origin, publicKey: metadata.publicKey, birth, certificate: signedCertificate });
    },
    async delete(): Promise<void> { await current().delete(); handle = undefined; },
    close(): void { handle = undefined; },
  });
}
/** Generate the original own leaf for an actual identity CSR, without exporting its seed. */
export async function generateNativeCustomerLeaf(): Promise<PendingNativeCustomerLeaf> {
  let raw: RawPending | undefined = (await binding()).generateNativeCustomerLeaf();
  const publicKey = raw.publicKey;
  return Object.freeze({ publicKey,
    async commit(enrollment: NativeCustomerEnrollment): Promise<NativeCustomerCredential> {
      if (raw === undefined) throw new WorkersTransportError("pending customer leaf consumed", "configuration");
      const pending = raw;
      raw = undefined;
      const { origin, birth, certificate, sqlSession } = enrollment;
      const handle = await pending.commit(origin, birth, certificate, sqlSession);
      return restored(handle, { origin, publicKey, birth, certificate });
    },
    dispose(): void { raw?.dispose(); raw = undefined; },
  });
}
/** Reopen the original sealed pair; expired public certificates remain useful for SQL renewal. */
export async function openNativeCustomerCredential(tuple: NativeCustomerPublicTuple): Promise<NativeCustomerCredential> {
  const snapshot = { ...tuple };
  const native = await binding();
  const handle = await native.openNativeCustomerCredential(snapshot.origin, snapshot.publicKey, snapshot.birth, snapshot.certificate);
  return restored(handle, snapshot);
}
