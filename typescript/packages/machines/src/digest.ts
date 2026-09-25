/** Canonical immutable SHA-256 digest used by Machines images and contracts. */
export function digestHex(value: string): string {
  if (!/^[0-9a-fA-F]{64}$/.test(value) || /^0+$/.test(value)) {
    throw new TypeError("digest must be 32 non-zero SHA-256 bytes");
  }
  return value.toLowerCase();
}

export function digestBytes(value: Uint8Array): string {
  if (value.byteLength !== 32 || value.every(byte => byte === 0)) {
    throw new TypeError("digest must be 32 non-zero SHA-256 bytes");
  }
  return Array.from(value, byte => byte.toString(16).padStart(2, "0")).join("");
}
