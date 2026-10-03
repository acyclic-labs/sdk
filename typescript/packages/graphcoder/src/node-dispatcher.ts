import { createInterface } from "node:readline";
import type { Readable, Writable } from "node:stream";
import type { GraphCoderTransport } from "./api.js";
import { GraphCoderWireDispatcher } from "./dispatcher.js";

export interface NodeGraphCoderDispatcherOptions { readonly transport: GraphCoderTransport; readonly input?: Readable; readonly output?: Writable; }

/** Serves the native dispatcher over newline-delimited JSON. */
export async function runNodeGraphCoderDispatcher(options: NodeGraphCoderDispatcherOptions): Promise<void> {
  const reader = createInterface({ input: options.input ?? process.stdin, crlfDelay: Infinity });
  const output = options.output ?? process.stdout;
  const dispatcher = new GraphCoderWireDispatcher(options.transport);
  const pending = new Set<Promise<void>>();
  for await (const line of reader) {
    const write = dispatcher.dispatchLine(line).then(value => new Promise<void>((resolve, reject) => output.write(`${value}\n`, error => error == null ? resolve() : reject(error))));
    pending.add(write);
    void write.finally(() => pending.delete(write));
  }
  await Promise.all(pending);
}
