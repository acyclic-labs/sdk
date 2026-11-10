import assert from "node:assert/strict";
import test from "node:test";
import acyclic from "../../opencode/index.mjs";

test("OpenCode wait outlives the host default deadline without a polling reply", async t => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const hooks = await acyclic({ client: {}, directory: process.cwd() });
  const config = {};
  await hooks.config(config);
  const registration = config.mcp["acyclic-sessions"];
  const call = timeout => {
    const result = Promise.withResolvers();
    const timer = setTimeout(() => result.reject(new Error("MCP error -32001: Request timed out")), timeout ?? 20);
    return { promise: result.promise.finally(() => clearTimeout(timer)),
      complete: () => result.resolve({ status: { settled: true }, reply: "completed" }) };
  };
  const defaultWait = call();
  const rejected = assert.rejects(defaultWait.promise, /-32001/);
  const wait = call(registration.timeout);
  let replies = 0;
  wait.promise.then(() => replies++);
  t.mock.timers.tick(21);
  await rejected;
  assert.equal(replies, 0, "wait returned before the child's terminal event");
  wait.complete();
  assert.deepEqual(await wait.promise, { status: { settled: true }, reply: "completed" });
  assert.equal(replies, 1);
});
