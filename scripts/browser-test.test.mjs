import { expect, test } from "bun:test";
import fc from "fast-check";
import { EventEmitter } from "node:events";
import { childExit, rejectWaits } from "./browser-test.mjs";

async function checkWaiters(histories, broken) {
  const error = new Error("browser process failed");
  const registries = [new Map(), new Map()];
  const observed = [], expected = [];
  for (let group = 0; group < 2; group++) {
    // Keep one pending event in every history: an exit after navigate must wake it.
    const history = group === 1 ? [...histories[group], false] : histories[group];
    for (const complete of history) {
      const index = observed.length;
      observed.push(undefined); expected.push(complete ? "done" : error);
      let resolve, reject;
      new Promise((yes, no) => { resolve = yes; reject = no; })
        .then(() => { observed[index] = "done"; }, failure => { observed[index] = failure; });
      if (complete) resolve();
      else registries[group].set(index, { resolve, reject });
    }
  }
  rejectWaits(error, ...registries.slice(0, broken ? 1 : 2));
  await Promise.resolve();
  if (registries.some(map => map.size !== 0) || observed.some((value, index) => value !== expected[index])) {
    throw new Error("a source wait survived process failure");
  }
}
const histories = fc.tuple(fc.array(fc.boolean(), { maxLength: 16 }), fc.array(fc.boolean(), { maxLength: 16 }));

test("terminal process failure drains RPC and event waits without changing completed waits", async () => {
  await fc.assert(fc.asyncProperty(histories, history => checkWaiters(history, false)));
});

test("negative control omitting page-event cancellation fails and shrinks", async () => {
  const result = await fc.check(fc.asyncProperty(histories, history => checkWaiters(history, true)));
  expect(result.failed).toBe(true);
  expect(result.counterexample).toEqual([[[], []]]);
});

async function checkChild(terminal, broken) {
  const child = new EventEmitter();
  child.on("error", () => {});
  let settled = false;
  const done = broken ? new Promise(resolve => child.once("exit", resolve)) : childExit(child);
  done.then(() => { settled = true; });
  child.emit(terminal, terminal === "error" ? new Error("spawn failed") : 0);
  await Promise.resolve();
  if (!settled) throw new Error("process creation failure never settled");
}

test("exit and failed creation both settle process cleanup", async () => {
  await fc.assert(fc.asyncProperty(fc.constantFrom("error", "exit"), event => checkChild(event, false)));
});

test("negative control awaiting exit alone misses failed creation", async () => {
  const result = await fc.check(fc.asyncProperty(fc.constantFrom("error", "exit"), event => checkChild(event, true)));
  expect(result.failed).toBe(true);
  expect(result.counterexample).toEqual(["error"]);
});
