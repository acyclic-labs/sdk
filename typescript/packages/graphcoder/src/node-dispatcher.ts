import type { Readable, Writable } from "node:stream";
import type { GraphCoderTransport } from "./api.js";
import { GraphCoderWireDispatcher } from "./dispatcher.js";

export interface NodeGraphCoderDispatcherOptions {
  readonly transport: GraphCoderTransport;
  readonly input?: Readable;
  readonly output?: Writable;
  readonly maximumLineBytes?: number;
  readonly maximumInFlight?: number;
}

export const DEFAULT_NODE_DISPATCHER_LINE_BYTES = 16 * 1024 * 1024;
export const DEFAULT_NODE_DISPATCHER_IN_FLIGHT = 64;

type NodeFrame = { readonly kind: "line"; readonly value: string } | { readonly kind: "too_long" };

/** Serves the native dispatcher over newline-delimited JSON. */
export async function runNodeGraphCoderDispatcher(options: NodeGraphCoderDispatcherOptions): Promise<void> {
  const maximumLineBytes = options.maximumLineBytes ?? DEFAULT_NODE_DISPATCHER_LINE_BYTES;
  const maximumInFlight = options.maximumInFlight ?? DEFAULT_NODE_DISPATCHER_IN_FLIGHT;
  if (!Number.isSafeInteger(maximumLineBytes) || maximumLineBytes < 1) throw new Error("maximum dispatcher line bytes must be positive");
  if (!Number.isSafeInteger(maximumInFlight) || maximumInFlight < 1) throw new Error("maximum dispatcher in-flight requests must be positive");
  const input = options.input ?? process.stdin;
  const output = options.output ?? process.stdout;
  const dispatcher = new GraphCoderWireDispatcher(options.transport);
  const pending = new Set<Promise<void>>();
  for await (const frame of boundedFrames(input, maximumLineBytes)) {
    while (pending.size >= maximumInFlight) await Promise.race(pending);
    const response = frame.kind === "too_long"
      ? JSON.stringify({ request_id: "", ok: false, error: { code: "invalid_input", message: "request line exceeds the configured size" } })
      : await dispatcher.dispatchLine(frame.value);
    const write = new Promise<void>((resolve, reject) => output.write(`${response}\n`, error => error == null ? resolve() : reject(error)));
    pending.add(write);
    void write.finally(() => pending.delete(write));
  }
  await Promise.all(pending);
}

/** Byte bounded JSON-lines framing shared by native and terminal hosts. */
async function* boundedFrames(input: Readable, maximumLineBytes: number): AsyncGenerator<NodeFrame> {
  let buffer = Buffer.alloc(0);
  let tooLong = false;
  for await (const chunk of input) {
    const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk));
    let offset = 0;
    while (offset < bytes.length) {
      const newline = bytes.indexOf(0x0a, offset);
      const end = newline < 0 ? bytes.length : newline;
      if (tooLong) {
        if (newline < 0) {
          offset = bytes.length;
          continue;
        }
        tooLong = false;
        buffer = Buffer.alloc(0);
        offset = newline + 1;
        yield { kind: "too_long" };
        continue;
      }
      const part = bytes.subarray(offset, end);
      if (buffer.length + part.length > maximumLineBytes) {
        tooLong = true;
        buffer = Buffer.alloc(0);
        if (newline >= 0) {
          tooLong = false;
          offset = newline + 1;
          yield { kind: "too_long" };
        } else {
          offset = bytes.length;
        }
        continue;
      }
      buffer = Buffer.concat([buffer, part]);
      if (newline >= 0) {
        offset = newline + 1;
        const value = buffer.toString("utf8");
        buffer = Buffer.alloc(0);
        yield { kind: "line", value: value.endsWith("\r") ? value.slice(0, -1) : value };
      } else {
        offset = bytes.length;
      }
    }
  }
  if (tooLong) yield { kind: "too_long" };
  else if (buffer.length > 0) yield { kind: "line", value: buffer.toString("utf8") };
}
