import type { AccountHolderMetadata as WasmHolderMetadata, AccountIssuedCredential as WasmIssuedCredential, AccountPreparedBearer as WasmPreparedBearer } from "../generated/wasm/acyclic_workers_wasm.js";
import type { AccountHolderMetadata as NativeHolderMetadata, AccountIssuedCredential as NativeIssuedCredential, AccountPreparedBearer as NativePreparedBearer } from "../generated/native/binding.js";
import { isNodeRuntime, loadWorkersNativeModule, loadWorkersWasmModule } from "./binding.js";
import { WorkersTransportError } from "./client.js";

/** Rust-owned non-authorizing own-leaf metadata, including exact certificate expiry. */
export type AccountHolderMetadata = Pick<WasmHolderMetadata, "environment" | "accountId" | "keyId" | "publicKey" | "certificateExpiresAtUnixMillis">;
/** Actual signed bearer expiry, never inferred by parsing a JWT in JavaScript. */
export type AccountIssuedCredential = Pick<WasmIssuedCredential, "bearer" | "expiresAtUnixMillis" | "certificateExpiresAtUnixMillis">;
type RawMetadata = WasmHolderMetadata | NativeHolderMetadata;
type RawCredential = WasmIssuedCredential | NativeIssuedCredential;
type RawPreparation = WasmPreparedBearer | NativePreparedBearer;
/** Rust-prepared bytes for a platform-held own leaf, with explicit ownership. */
export interface PreparedAccountBearer extends Pick<WasmPreparedBearer, "accountId" | "keyId" | "publicKey" | "expiresAtUnixMillis" | "certificateExpiresAtUnixMillis"> {
  /** Generated Rust byte exports own a normal ArrayBuffer, not shared memory. */
  readonly signingBytes: Uint8Array<ArrayBuffer>;
  finish(signature: Uint8Array): Promise<AccountIssuedCredential>;
  dispose(): void;
}
function release(value: RawMetadata | RawCredential | RawPreparation): void {
  if ("free" in value) value.free();
}
function credential(value: RawCredential): AccountIssuedCredential {
  try {
    return Object.freeze({ bearer: value.bearer, expiresAtUnixMillis: value.expiresAtUnixMillis,
      certificateExpiresAtUnixMillis: value.certificateExpiresAtUnixMillis });
  } finally { release(value); }
}
async function module() {
  const selected = isNodeRuntime() ? await loadWorkersNativeModule() : undefined;
  const binding = selected ?? await loadWorkersWasmModule();
  if (typeof binding.prepareAccountBearer !== "function" || typeof binding.inspectAccountHolder !== "function"
    || typeof binding.encodeAccountPublicKey !== "function") {
    throw new WorkersTransportError("Workers account holder ABI exports missing", "configuration");
  }
  return binding;
}
/** Canonical public-key encoding from the same Rust authority as preparation. */
export async function encodeAccountPublicKey(bytes: Uint8Array): Promise<string> {
  return (await module()).encodeAccountPublicKey(bytes);
}
/** Expired certificates may be inspected for renewal, but not used to mint. */
export async function inspectAccountHolder(publicKey: string, birth: string, certificate: string): Promise<AccountHolderMetadata> {
  const value = (await module()).inspectAccountHolder(publicKey, birth, certificate);
  try {
    return Object.freeze({ environment: value.environment, accountId: value.accountId, keyId: value.keyId,
      publicKey: value.publicKey, certificateExpiresAtUnixMillis: value.certificateExpiresAtUnixMillis });
  } finally { release(value); }
}
/** Prepare opaque canonical bytes without exporting any private key or Root authority. */
export async function prepareAccountBearer(publicKey: string, birth: string, certificate: string,
  credentialId: string, lifetimeSeconds: bigint): Promise<PreparedAccountBearer> {
  const raw = (await module()).prepareAccountBearer(publicKey, birth, certificate, credentialId, lifetimeSeconds);
  let disposed = false;
  const dispose = () => {
    if (!disposed) { disposed = true; raw.dispose(); release(raw); }
  };
  return Object.freeze({ accountId: raw.accountId, keyId: raw.keyId, publicKey: raw.publicKey,
    expiresAtUnixMillis: raw.expiresAtUnixMillis, certificateExpiresAtUnixMillis: raw.certificateExpiresAtUnixMillis,
    get signingBytes() {
      if (disposed) throw new WorkersTransportError("account preparation disposed", "configuration");
      // Both generated Rust bridges create owned ArrayBuffers for their Vec<u8> output.
      const bytes = raw.signingBytes as Uint8Array<ArrayBuffer>;
      return bytes;
    },
    async finish(signature: Uint8Array): Promise<AccountIssuedCredential> {
      if (disposed) throw new WorkersTransportError("account preparation disposed", "configuration");
      try { return credential(raw.finish(signature)); }
      finally { dispose(); }
    },
    dispose,
  });
}
