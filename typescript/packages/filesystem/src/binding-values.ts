// `new Uint8Array(view)` is one memcpy; Bun's `Uint8Array.from` iterates a
// native `Buffer` element by element (~100x slower for file-sized values).
export function copyBytes(value: Uint8Array): Uint8Array {
  return new Uint8Array(value);
}

/**
 * Takes ownership of a freshly allocated binding result without a second
 * copy. Bindings hand out a new buffer per call, so a view spanning its whole
 * backing store is already exclusively owned; anything else is copied.
 */
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
