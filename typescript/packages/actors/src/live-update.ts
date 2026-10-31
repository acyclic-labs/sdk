/** Public JSON contracts are emitted by the original Rust Actors producer. */
export type * from "./generated/live-update.js";

/** Encode a canonical v1 upload, volume document or host-bound call envelope.
 * All u64 declarations use decimal strings, preserving full epoch precision. */
export function encodeLiveUpdate(value: unknown): Uint8Array {
  const encoder = new TextEncoder();
  function canonical(input: unknown): string {
    if (Array.isArray(input)) return `[${input.map(canonical).join(",")}]`;
    if (input !== null && typeof input === "object") {
      const entries = Object.entries(input).map(([key, item]) => ({ key, item, bytes: encoder.encode(key) }));
      entries.sort((a, b) => {
        for (let i = 0; i < Math.min(a.bytes.length, b.bytes.length); i++) {
          const order = a.bytes[i]! - b.bytes[i]!;
          if (order !== 0) return order;
        }
        return a.bytes.length - b.bytes.length;
      });
      return `{${entries.map(({ key, item }) => `${canonical(key)}:${canonical(item)}`).join(",")}}`;
    }
    if (typeof input === "string") {
      for (const character of input) {
        const point = character.codePointAt(0)!;
        if (point >= 0xd800 && point <= 0xdfff) throw new TypeError("invalid Unicode scalar");
      }
    } else if (input !== null && typeof input !== "boolean"
      && !(typeof input === "number" && Number.isSafeInteger(input))) {
      throw new TypeError("live-update JSON requires exact integers and JSON values");
    }
    return JSON.stringify(input);
  }
  return encoder.encode(canonical({ v: 1, value }));
}
