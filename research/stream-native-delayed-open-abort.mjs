import assert from "node:assert/strict";
import { NativeStreamProvider } from "@acyclic-labs/stream/native";
class FakeCancellation {
  cancelled = false;
  cancel() { this.cancelled = true; }
}
let opened = false;
let cancellation;
let enteredResolve;
const entered = new Promise(resolve => { enteredResolve = resolve; });
const fakeModule = { NativeStreamCancellation: FakeCancellation };
const fakeClient = {
  openFollowResult(_request, token) {
    opened = true;
    cancellation = token;
    enteredResolve();
    return new Promise(resolve => setTimeout(() => resolve({ error: { code: "unavailable", message: "fixture cancelled" } }), 100));
  },
};
const provider = new NativeStreamProvider(fakeClient, fakeModule);
const controller = new AbortController();
const pending = (async () => { for await (const _record of provider.follow("accounts/events", { from: 0n, signal: controller.signal })) {} })();
await Promise.race([entered, new Promise((_, reject) => setTimeout(() => reject(new Error("open was not entered")), 1000))]);
controller.abort();
await Promise.race([pending, new Promise((_, reject) => setTimeout(() => reject(new Error("delayed open did not cancel")), 1000))]);
assert.equal(opened, true);
assert.equal(cancellation.cancelled, true);
console.log(JSON.stringify({ schema: "acyclic.stream.native.delayed-open-abort.v1", opened: true, cancellationObserved: true, completed: true }));
