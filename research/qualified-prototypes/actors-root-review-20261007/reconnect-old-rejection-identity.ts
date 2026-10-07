import assert from "node:assert/strict";
import { ActorsClient } from "../../../typescript/packages/actors/src/client.ts";
let connects = 0;
let firstAbort!: () => void;
const clientShape = { transport: "fixture", inspectActor: async () => new Uint8Array() };
const client = new ActorsClient({ endpoint: "https://fixture.invalid", token: "token", binding: {
  connect(_endpoint, _token, signal) {
    connects += 1;
    if (connects === 1) {
      return new Promise((_resolve, reject) => {
        firstAbort = () => setTimeout(() => reject(new Error("delayed old connection cleanup")), 50);
        signal?.addEventListener("abort", firstAbort, { once: true });
      });
    }
    return Promise.resolve(clientShape);
  },
}});
const firstAbortController = new AbortController();
const first = client.inspectActor({ actorId: "first" } as never, { signal: firstAbortController.signal }).catch(error => error.message);
firstAbortController.abort();
assert.equal(await first, "Actors operation cancelled");
const second = await client.inspectActor({ actorId: "second" } as never);
assert.deepEqual(second, { actor: null });
await new Promise(resolve => setTimeout(resolve, 100));
const third = await client.inspectActor({ actorId: "third" } as never);
assert.deepEqual(third, { actor: null });
assert.equal(connects, 2);
console.log(JSON.stringify({ connects, second: "resolved", third: "reused-after-old-rejection" }));

