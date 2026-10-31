import type { WorkspaceName } from "./contracts.js";
import { isObjectReference } from "../generated/object-references.js";
export { isObjectReference } from "../generated/object-references.js";

/** Owns a fresh binding buffer, copying partial or shared views. */
export function ownBytes(value: Uint8Array): Uint8Array {
  const { buffer, byteOffset, byteLength } = value;
  // Browsers without cross-origin isolation have no SharedArrayBuffer global.
  const shared = typeof SharedArrayBuffer !== "undefined" && buffer instanceof SharedArrayBuffer;
  return byteOffset === 0 && byteLength === buffer.byteLength && !shared
    ? new Uint8Array(buffer) : new Uint8Array(value);
}

export function copyOptionalBytes(value: Uint8Array | undefined): Uint8Array | undefined {
  return value === undefined ? undefined : new Uint8Array(value);
}

export function copyName(value: WorkspaceName): WorkspaceName {
  return { encoding: value.encoding, bytes: new Uint8Array(value.bytes) };
}

export function copyFixedBytes(value: unknown, length: number, label: string): Uint8Array | undefined {
  if (value === undefined) return undefined;
  if (value instanceof Uint8Array && value.byteLength === length) return new Uint8Array(value);
  throw new TypeError(`${label} must be ${length} bytes`);
}

export function requireWorkspaceName(name: string): void {
  if (name.length === 0) throw new RangeError("workspace name must be non-empty");
}

export function requireIdentity(value: Uint8Array, length: number, label: string): void {
  if (value.byteLength !== length) {
    throw new RangeError(`${label} must be exactly ${length} bytes`);
  }
}

export function requireObjectReference(value: unknown, label: string, kind?: number): asserts value is Uint8Array {
  if (!isObjectReference(value, kind)) throw new TypeError(`${label} must be a canonical object reference`);
}

export function copyObjectReference(value: unknown, label: string, kind?: number): Uint8Array | undefined {
  if (value === undefined) return undefined;
  requireObjectReference(value, label, kind);
  return new Uint8Array(value);
}
