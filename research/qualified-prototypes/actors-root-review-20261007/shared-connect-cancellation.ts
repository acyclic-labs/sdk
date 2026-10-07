import { ActorsClient } from "../../../typescript/packages/actors/src/client.ts";
const first = new AbortController();
const second = new AbortController();
let connects = 0;
const client = new ActorsClient({ endpoint: "https://fixture.invalid", token: "token", binding: {
  connect(_endpoint, _token, signal) {
    connects++;
    return new Promise((_resolve, reject) => signal?.addEventListener("abort", () => reject(new Error("shared connect aborted")), {once: true}));
  }
}});
const a = client.inspectActor({ actorId: "actor-a" } as never, {signal: first.signal}).then(() => "resolved", error => error.message);
const b = client.inspectActor({ actorId: "actor-b" } as never, {signal: second.signal}).then(() => "resolved", error => error.message);
first.abort();
const secondOutcome = await Promise.race([b, new Promise(resolve => setTimeout(() => resolve("still_pending"), 100))]);
const result = { connects, first: await a, second: secondOutcome, secondSignalAborted: second.signal.aborted };
second.abort();
await b;
console.log(JSON.stringify(result));
if (secondOutcome !== "still_pending") process.exitCode = 1;