import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { once } from "node:events";
import { test } from "node:test";
import { McpClient } from "../../shared/mcp-client.mjs";

test("native MCP pipe credits retain cancellation and terminals for queued callers", { timeout: 10_000 }, async () => {
  const child = fork(new URL("./mcp-pipe-child.mjs", import.meta.url), [], { silent: true });
  const closed = once(child, "close");
  child.stderr.resume();
  const initialization = once(child, "message");
  const ready = Promise.withResolvers();
  const failed = Promise.withResolvers();
  const stalled = Promise.withResolvers();
  let blocked = false;
  let writes = 0;
  const originalWrite = child.stdin.write.bind(child.stdin);
  child.stdin.on("drain", () => { blocked = false; });
  child.stdin.write = (...args) => {
    assert(!blocked, "write(false) must be followed by the original drain before another write");
    writes++;
    const credit = originalWrite(...args);
    blocked = !credit;
    if (blocked) stalled.resolve();
    return credit;
  };
  const client = new McpClient(child, { name: "transport-conformance", version: "1",
    recovery: "node --test plugin/tests/hosts/mcp-client.test.mjs",
    onInitialized: ready.resolve, onFailure: failed.resolve });
  try {
    const [{ request }] = await initialization;
    assert.equal(request.method, "initialize");
    const initialWrites = writes;
    const text = "x".repeat(4 * 1024 * 1024);
    const cancelled = new AbortController();
    const calls = Array.from({ length: 100_000 }, (_, index) =>
      client.request("wait", { text }, index === 99_999 ? cancelled.signal : undefined)
        .then(value => ({ value }), error => ({ error })));
    assert.equal(writes, initialWrites, "initialize response owns release, without a caller replay loop");
    cancelled.abort();
    assert.match((await calls.at(-1)).error.message, /cancelled/);
    assert.equal(writes, initialWrites, "queued cancellation sends no frame");
    child.send("initialize");
    await ready.promise;
    await stalled.promise;
    assert(blocked, "the real unread native pipe must exhaust its write credit");
    assert(child.stdin.writableLength <= child.stdin.writableHighWaterMark);
    const stalledWrites = writes;
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(writes, stalledWrites, "a scheduler continuation cannot replace drain readiness");
    child.send("resume");
    assert.deepEqual((await calls[0]).value, { received: text.length });
    const failure = await failed.promise;
    assert.match(failure.message, /output closed/);
    const results = await Promise.all(calls);
    assert(results.slice(1, -1).every(result => result.error === failure),
      "one original EOF must terminate every active waiter with the retained cause");
    await assert.rejects(client.request("later", {}), error => error === failure);
  } finally {
    child.send("exit");
    const [code, signal] = await closed;
    assert.equal(code, 0);
    assert.equal(signal, null);
  }
});
