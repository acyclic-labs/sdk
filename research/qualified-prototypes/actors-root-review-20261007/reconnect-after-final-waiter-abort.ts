import { ActorsClient } from "../../../typescript/packages/actors/src/client.ts";
let connects = 0;
const client = new ActorsClient({ endpoint: "https://fixture.invalid", token: "token", binding: {
  connect(_endpoint, _token, signal) {
    connects++;
    return new Promise((_resolve, reject) => signal?.addEventListener("abort", () => setTimeout(() => reject(new Error("delayed old connection cleanup")), 50), {once: true}));
  }
}});
const first = new AbortController();
const initial = client.inspectActor({ actorId: "first" } as never, {signal: first.signal}).catch(error => error.message);
first.abort();
await initial;
const second = new AbortController();
const next = client.inspectActor({ actorId: "second" } as never, {signal: second.signal}).catch(error => error.message);
const nextOutcome = await Promise.race([next, new Promise(resolve => setTimeout(() => resolve("still_pending"), 120))]);
const result = { connects, second: nextOutcome, secondSignalAborted: second.signal.aborted };
second.abort(); await next;
console.log(JSON.stringify(result));
if (connects !== 2 || nextOutcome !== "still_pending") process.exitCode = 1;