import { REASON_HEADER } from "../generated/refusal.js";
export { REASON_HEADER, REFUSAL_REASONS } from "../generated/refusal.js";
export type { KnownRefusalReason, RefusalClass } from "../generated/refusal.js";

/** Original member metadata; diagnostics do not authorize replay or ownership. */
export function refusalReason(metadata: { get(name: string): string | null }): string | undefined {
  return metadata.get(REASON_HEADER) ?? undefined;
}

/** Read an optional foreign diagnostic once, preserving the original failure. */
export function refusalText(value: unknown): string | undefined {
  if (typeof value !== "object" || value === null) return undefined;
  try {
    const reason = (value as { readonly reason?: unknown }).reason;
    return typeof reason === "string" ? reason : undefined;
  } catch { return undefined; }
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
