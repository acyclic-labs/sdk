import type { ReadonlyBytes } from "./generated/readonly.js";

const maxAttachmentBytes = 2048;

/** Encodes structured connection attachment data as bounded UTF-8 JSON. */
export function serializeAttachment(value: unknown): Uint8Array {
  const json = JSON.stringify(value);
  if (json === undefined) throw new TypeError("Attachment must be JSON serializable");
  const bytes = new TextEncoder().encode(json);
  if (bytes.byteLength > maxAttachmentBytes) throw new RangeError("Attachment exceeds 2 KiB");
  return bytes;
}

/** Decodes structured attachment data; callers validate the resulting value. */
export function deserializeAttachment(bytes: ReadonlyBytes): unknown {
  if (bytes.byteLength > maxAttachmentBytes) throw new RangeError("Attachment exceeds 2 KiB");
  return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(Uint8Array.from(bytes)));
}
