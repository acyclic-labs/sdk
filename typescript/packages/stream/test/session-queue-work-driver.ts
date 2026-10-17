import { plugin } from "bun";
import { serve } from "./model-driver.js";

plugin({ name: "queue-work-model", setup(build) {
  build.onLoad({ filter: /[\\/]src[\\/]session-queue\.ts$/ }, async args => {
    let source = await Bun.file(args.path).text();
    const storage = "#values: (T | undefined)[] = [];";
    const observe = "get available(): number {";
    if (!source.includes(storage) || !source.includes(observe)) throw new Error("queue work probe no longer matches");
    source = "let writes = 0;\nfunction storage<T>(): T[] { return new Proxy<T[]>([], { set(target, key, value) { if (key !== 'length') writes += 1; return Reflect.set(target, key, value); } }); }\n" + source;
    source = source.replace(storage, "#values: (T | undefined)[] = storage<T | undefined>();")
      .replace("this.#values = [];", "this.#values = storage<T | undefined>();")
      .replace(observe, "get modelWrites(): number { return writes; }\n  get modelRetained(): number { return this.#values.filter(value => value !== undefined).length; }\n  " + observe);
    if (process.env.STREAM_SESSION_FAULT === "shift") {
      const take = "const value = this.#values[this.#head] as T;\n    this.#values[this.#head] = undefined;\n    this.#head = (this.#head + 1) % this.#capacity;";
      if (!source.includes(take)) throw new Error("shift negative control no longer matches");
      source = source.replace(take, "const value = this.#values.shift() as T;");
    }
    if (process.env.STREAM_SESSION_FAULT === "head") {
      const advance = "this.#head = (this.#head + 1) % this.#capacity;";
      if (!source.includes(advance)) throw new Error("head negative control no longer matches");
      source = source.replace(advance, "this.#head = (this.#head + 2) % this.#capacity;");
    }
    return { contents: source, loader: "ts" };
  });
} });
const { SessionQueue } = await import("../src/session-queue.js");
type Observed = { modelWrites: number; modelRetained: number };
async function command(request: { action: string; capacity: number; operations?: [number, number][] }): Promise<unknown> {
  const queue = new SessionQueue<number>(request.capacity);
  const observed = queue as unknown as Observed;
  const reader = queue[Symbol.asyncIterator]();
  if (request.action === "work") {
    for (let value = 0; value < request.capacity; value += 1) queue.push(value);
    const writes = observed.modelWrites;
    const started = performance.now();
    for (let value = 0; value < request.capacity; value += 1) {
      if ((await reader.next()).value !== value) throw new Error("FIFO work fixture differs");
    }
    queue.end();
    if (!(await reader.next()).done) throw new Error("work fixture did not end");
    return { writes: observed.modelWrites - writes, retained: observed.modelRetained, milliseconds: performance.now() - started };
  }
  let ended = false;
  const rows = [];
  for (const [action, value] of request.operations ?? []) {
    let outcome: unknown = "ok";
    try {
      switch (action % 5) {
        case 0: queue.push(value); break;
        case 1:
          if (queue.available !== request.capacity || ended) {
            const next = await reader.next();
            outcome = next.done ? "end" : next.value;
          } else outcome = "empty";
          break;
        case 2: {
          const discarded: number[] = [];
          queue.discard(value => discarded.push(value));
          outcome = discarded;
          break;
        }
        case 3: queue.end(); ended = true; break;
        case 4: queue.end(new Error("terminal")); ended = true; break;
      }
    } catch (error) { outcome = error instanceof Error && "code" in error ? error.code : "terminal"; }
    rows.push({ outcome, available: queue.available, retained: observed.modelRetained });
  }
  queue.end();
  await reader.return(undefined);
  return rows;
}
await serve(command);
