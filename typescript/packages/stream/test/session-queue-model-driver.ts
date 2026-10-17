import { createInterface } from "node:readline";
import { plugin } from "bun";

plugin({ name: "queue-cancellation-model", setup(build) {
  build.onLoad({ filter: /[\\/]src[\\/]session-queue\.ts$/ }, async args => {
    let source = await Bun.file(args.path).text();
    const observe = "get available(): number {";
    if (!source.includes(observe)) throw new Error("queue occupancy probe no longer matches");
    source = source.replace(observe, "get modelWaiting(): number { return this.#writers.size; }\n  " + observe);
    if (process.env.STREAM_SESSION_FAULT === "queue") {
      const send = "async send(value: T, signal?: AbortSignal): Promise<void> {";
      if (!source.includes(send)) throw new Error("queue negative control no longer matches");
      source = source.replace(send, send + " signal = undefined;");
    }
    if (process.env.STREAM_SESSION_FAULT === "order") {
      const admission = "const value = this.#take();\n      this.#wakeWriters();\n      yield value;";
      if (!source.includes(admission)) throw new Error("queue ordering negative control no longer matches");
      source = source.replace(admission, "const value = this.#take();\n      yield value;\n      this.#wakeWriters();");
    }
    return { contents: source, loader: "ts" };
  });
} });
const { checkpoint } = await import("./session-node.js");
const { SessionQueue } = await import("../src/session-queue.js");
const queue = new SessionQueue<number>(1);
queue.push(0);
const reader = queue[Symbol.asyncIterator]();
const cancel = new AbortController();
let settled = false;
let accepted = false;
let acceptedAtAbort = false;
let healthySettled = false;
let error: unknown;
const seen: number[] = [];
void queue.send(1, cancel.signal).then(() => { settled = true; accepted = true; }, () => { settled = true; });
async function command(action: string): Promise<unknown> {
  switch (action) {
    case "ready": break;
    case "abort": acceptedAtAbort = accepted; cancel.abort(); break;
    case "drain": {
      const next = reader.next();
      // A fresh producer arrives before the cancelled writer can resume from
      // its wakeup. The real queue must preserve admission/wire order.
      void queue.send(2).then(() => { healthySettled = true; }, failure => { error = failure; });
      seen.push((await next).value!);
      break;
    }
    case "finish":
      for (let index = 0; index < 2; index += 1) {
        if (queue.available === 0) seen.push((await reader.next()).value!);
        await checkpoint();
      }
      break;
    default: throw new Error(`unknown queue action ${action}`);
  }
  await checkpoint();
  if (error !== undefined) throw error;
  return { settled, accepted, acceptedAtAbort, healthySettled, seen, available: queue.available,
    waiting: (queue as unknown as { modelWaiting: number }).modelWaiting };
}
for await (const line of createInterface({ input: process.stdin })) {
  try { process.stdout.write(`${JSON.stringify(await command(JSON.parse(line).action))}\n`); }
  catch (error) { process.stdout.write(`${JSON.stringify({ error: String(error) })}\n`); }
}
cancel.abort(); queue.end();
