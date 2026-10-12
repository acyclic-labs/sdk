import type { WorkspaceName } from "./contracts.js";

/** Copies a binding buffer. */
export function copyBytes(value: Uint8Array): Uint8Array {
  return new Uint8Array(value);
}

/** Owns a fresh binding buffer, copying partial or shared views. */
export function ownBytes(value: Uint8Array): Uint8Array {
  const { buffer, byteOffset, byteLength } = value;
  // Browsers without cross-origin isolation have no SharedArrayBuffer global.
  const shared = typeof SharedArrayBuffer !== "undefined" && buffer instanceof SharedArrayBuffer;
  return byteOffset === 0 && byteLength === buffer.byteLength && !shared
    ? new Uint8Array(buffer) : copyBytes(value);
}

export function copyOptionalBytes(value: Uint8Array | undefined): Uint8Array | undefined {
  return value === undefined ? undefined : copyBytes(value);
}

export function copyName(value: WorkspaceName): WorkspaceName {
  return { encoding: value.encoding, bytes: copyBytes(value.bytes) };
}

export function copyFixedBytes(value: unknown, length: number, label: string): Uint8Array | undefined {
  if (value === undefined) return undefined;
  if (value instanceof Uint8Array && value.byteLength === length) return copyBytes(value);
  throw new TypeError(`${label} must be ${length} bytes`);
}

export function requireWorkspaceName(name: string): void {
  if (name.length === 0) throw new RangeError("workspace name must be non-empty");
}

export function requireIdentity(value: Uint8Array, label: string): void {
  if (value.byteLength !== 16) {
    throw new RangeError(`${label} must be exactly 16 bytes`);
  }
}

export function requireGenerationIdentity(value: Uint8Array, label: string): void {
  if (value.byteLength !== 32) {
    throw new RangeError(`${label} generation identity must be exactly 32 bytes`);
  }
}
