import type { ConnectError } from "@connectrpc/connect";
import { findTrailerError } from "@connectrpc/connect/protocol-grpc";
import type { Refusal } from "./refusal.js";

/** Retain HTTP/2's original header pairs before Headers joins repeated values.
 * Node exposes header octets as Latin-1 strings. Details are opaque; they need
 * not encode Google's Status message. This evidence grants no retry authority. */
export function refusalFromGrpcHeaders(error: ConnectError, blocks: Iterable<readonly string[]>): Refusal {
  const statusHeaders = new Headers();
  const metadata: Array<readonly [string, Uint8Array]> = [];
  let details = "";
  for (const raw of blocks) {
    for (let i = 0; i < raw.length; i += 2) {
      const name = raw[i]!, value = raw[i + 1]!;
      if (name === "grpc-status" || name === "grpc-message") statusHeaders.set(name, value);
      else if (name === "grpc-status-details-bin") details = value;
      else if (name.startsWith("acyclic-")) metadata.push([name, Buffer.from(value, "latin1")]);
    }
  }
  const status = findTrailerError(statusHeaders) ?? error;
  return { status: status.code, message: status.rawMessage, details: Buffer.from(details, "base64"), metadata };
}
