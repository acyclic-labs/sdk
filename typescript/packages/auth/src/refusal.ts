/** Original member metadata; diagnostics do not authorize replay or ownership. */
export function refusalReason(metadata: { get(name: string): string | null }): string | undefined {
  return metadata.get("acyclic-unserved-reason") ?? undefined;
}

/** The original foreign binding's raw value, never interpreted as replay permission. */
export function refusalBytes(value: unknown): Uint8Array | undefined {
  if (typeof value !== "object" || value === null) return undefined;
  try {
    const bytes = (value as { readonly reasonBytes?: unknown }).reasonBytes;
    if (bytes instanceof Uint8Array) return bytes;
    // Native bindings serialize their original Vec<u8> as an array.
    if (Array.isArray(bytes)) {
      const retained = new Uint8Array(bytes.length);
      for (let index = 0; index < retained.length; index++) {
        const byte = bytes[index];
        if (!Number.isInteger(byte) || byte < 0 || byte > 255) return undefined;
        retained[index] = byte;
      }
      return retained;
    }
  } catch { /* An inaccessible foreign property supplies no diagnostic. */ }
  return undefined;
}
