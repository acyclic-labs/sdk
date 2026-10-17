import assert from "node:assert/strict";
import { setImmediate } from "node:timers/promises";
import { ActorsClient } from "../dist/client.js";
import { credentialProvider } from "../dist/credential.js";

// This scheduler executes the real shared provider path. Promise jobs run only
// at explicit drain steps; all permutations of abort, settlement and two drains
// are explored, including settlement queued before abort but not yet observed.
function* permutations(events) {
  if (!events.length) { yield []; return; }
  for (let index = 0; index < events.length; index++) {
    for (const rest of permutations(events.filter((_, i) => i !== index))) {
      yield [events[index], ...rest];
    }
  }
}

function resolveProvider(callback) {
  if (process.argv[2] !== "broken") return credentialProvider({ credential: callback });
  // Former shared path: cancellation cannot settle until renewal does.
  return async signal => {
    signal.throwIfAborted();
    const token = await callback(signal);
    signal.throwIfAborted();
    return token;
  };
}

for (const reasonName of ["AbortError", "TimeoutError"]) {
  for (const fails of [false, true]) {
    for (const schedule of permutations(["abort", "settle", "drain1", "drain2"])) {
      const controller = new AbortController();
      const cancelled = new DOMException("cancelled", reasonName);
      const failure = new Error("renewal rejected");
      let yes, no;
      const deferred = new Promise((resolve, reject) => { yes = resolve; no = reject; });
      let observed = "pending";
      let expected = "pending";
      let settled = false;
      resolveProvider(() => deferred)(controller.signal).then(
        value => { assert.equal(value, "fresh"); observed = "value"; },
        error => { observed = error === cancelled ? "cancelled" : error === failure ? "failed" : "unexpected"; },
      );
      for (const event of schedule) {
        if (event === "abort") {
          controller.abort(cancelled);
          if (expected === "pending") expected = "cancelled";
        } else if (event === "settle") {
          settled = true;
          if (fails) no(failure); else yes("fresh");
        } else {
          if (expected === "pending" && settled) expected = fails ? "failed" : "value";
          await setImmediate();
          assert.equal(observed, expected, `${reasonName} ${fails} ${schedule}: pending renewal must cancel`);
        }
      }
      await setImmediate();
      assert.equal(observed, expected);
    }
  }
}

// Pre-abort never invokes a callback. Synchronous abort always wins, and every
// late throw/rejection is consumed even under Node's fatal rejection policy.
for (const mode of ["pre", "return", "throw", "reject"]) {
  const controller = new AbortController();
  const reason = new Error("cancelled");
  let calls = 0;
  if (mode === "pre") controller.abort(reason);
  const callback = () => {
    calls++;
    controller.abort(reason);
    if (mode === "throw") throw new Error("late failure");
    if (mode === "reject") return Promise.reject(new Error("late failure"));
    return "fresh";
  };
  await assert.rejects(resolveProvider(callback)(controller.signal), error => error === reason);
  assert.equal(calls, mode === "pre" ? 0 : 1);
  let connects = 0;
  const callController = new AbortController();
  const client = new ActorsClient({
    endpoint: "https://actors.example.test",
    credential: () => { callController.abort(); return Promise.reject(new Error("late failure")); },
    binding: { connect: async () => { connects++; throw new Error("must not connect"); } },
  });
  await assert.rejects(client.inspectActor({ actorId: "actor-a" }, { signal: callController.signal }), { code: "cancelled" });
  await setImmediate();
  assert.equal(connects, 0);
}

// A transport deadline is carried by AbortSignal too. An already-expired signal
// is above; here it expires while a provider that ignores cancellation is pending.
const deadline = AbortSignal.timeout(1);
const deadlineResult = credentialProvider({ credential: () => new Promise(() => {}) })(deadline);
const keepAlive = setTimeout(() => {}, 1000);
try { await assert.rejects(deadlineResult, error => error.name === "TimeoutError"); }
finally { clearTimeout(keepAlive); }
