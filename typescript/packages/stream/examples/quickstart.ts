import { MemoryStreamProvider, StreamClient } from "@acyclic-labs/stream";

type RunEvent =
  | { readonly type: "run.started" }
  | { readonly type: "run.completed"; readonly output: string };

const streams = new StreamClient(new MemoryStreamProvider());
const events = streams.json("runs/run_42", (value): RunEvent => {
  if (value === null || typeof value !== "object" || Array.isArray(value) || !("type" in value)) {
    throw new TypeError("expected run event");
  }
  if (value.type === "run.started") return { type: value.type };
  if (value.type === "run.completed" && "output" in value && typeof value.output === "string") {
    return { type: value.type, output: value.output };
  }
  throw new TypeError("expected run event");
});

await events.append({ type: "run.started" });
await events.append({ type: "run.completed", output: "typed and durable at the provider boundary" });

for await (const record of events.read({ from: 0n, limit: 100 })) {
  console.log(record.sequence, record.value);
}
