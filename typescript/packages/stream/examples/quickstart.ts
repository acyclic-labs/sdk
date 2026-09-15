import { MemoryStreamProvider, StreamClient } from "@acyclic-labs/stream";

type RunEvent =
  | { readonly type: "run.started" }
  | { readonly type: "run.completed"; readonly output: string };

const streams = new StreamClient(new MemoryStreamProvider());
const events = streams.json<RunEvent>("runs/run_42");

await events.append({ type: "run.started" });
await events.append({ type: "run.completed", output: "typed and durable at the provider boundary" });

for await (const record of events.read({ from: 0, limit: 100 })) {
  console.log(record.sequence, record.value);
}
