import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { once } from "node:events";
import { test } from "node:test";
import { McpClient } from "../../shared/mcp-client.mjs";

test("native MCP cancellation during encoding retains the next request producer", { timeout: 10_000 }, async () => {
  const child = fork(new URL("./mcp-pipe-child.mjs", import.meta.url), [], { silent: true });
  const closed = once(child, "close");
  child.stderr.resume();
  const initialization = once(child, "message");
  const client = new McpClient(child, { name: "transport-conformance", version: "1",
    recovery: "node --test --test-name-pattern 'native MCP cancellation during encoding' plugin/tests/hosts/mcp-client.test.mjs" });
  try {
    await initialization;
    const abort = new AbortController();
    const cancelled = client.request("wait", { toJSON() {
      abort.abort();
      return { text: "withdrawn" };
    } }, abort.signal).catch(error => error);
    const next = client.request("wait", { text: "kept" });
    child.send("initialize");
    child.send("resume");
    assert.match((await cancelled).message, /cancelled/);
    assert.deepEqual(await next, { received: 4 },
      "a retired encoder must neither write its frame nor unlink the next request");
  } finally {
    child.send("exit");
    assert.deepEqual(await closed, [0, null]);
    await client.closed;
  }
});

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

test("native MCP rejected initialization and partial frames retain exact terminal ownership", { timeout: 10_000 }, async () => {
  const recovery = "node --test plugin/tests/hosts/mcp-client.test.mjs";
  for (const command of ["eof", "malformed"]) {
    const child = fork(new URL("./mcp-pipe-child.mjs", import.meta.url), [], { silent: true });
    const closed = once(child, "close");
    child.stderr.resume();
    const initialization = once(child, "message");
    const failed = Promise.withResolvers();
    const client = new McpClient(child, { name: "transport-conformance", version: "1",
      recovery, onFailure: failed.resolve });
    try {
      const request = client.request("wait", {}).catch(error => error);
      await initialization;
      child.send(command);
      const failure = await failed.promise;
      assert.equal(await request, failure, "loss before initialize must settle the original gated request");
      await assert.rejects(client.request("late", {}), error => error === failure);
      if (command === "malformed") {
        assert.doesNotThrow(() => child.stdin.emit("error", new Error("later stdin failure")));
        await assert.rejects(client.request("later", {}), error => error === failure);
      } else assert.match(failure.message, /output closed/);
    } finally {
      child.send("exit");
      assert.deepEqual(await closed, [0, null]);
      await client.closed;
    }
  }

  const child = fork(new URL("./mcp-pipe-child.mjs", import.meta.url), ["cancel"], { silent: true });
  const closed = once(child, "close");
  child.stderr.resume();
  const initialization = once(child, "message");
  const failed = Promise.withResolvers();
  const stalled = Promise.withResolvers();
  const cancelledOnWire = Promise.withResolvers();
  const observed = [];
  child.on("message", message => {
    if (message.phase === "request") observed.push(message);
    if (message.phase === "cancelled") cancelledOnWire.resolve(message);
  });
  const originalWrite = child.stdin.write.bind(child.stdin);
  child.stdin.write = (...args) => {
    const credit = originalWrite(...args);
    if (!credit) stalled.resolve();
    return credit;
  };
  const client = new McpClient(child, { name: "transport-conformance", version: "1",
    recovery, onFailure: failed.resolve });
  try {
    await initialization;
    const abort = new AbortController();
    const text = "💠".repeat(1024 * 1024);
    const partial = client.request("wait", { text }, abort.signal).catch(error => error);
    const next = client.request("wait", { text: "next" }).catch(error => error);
    child.send("initialize");
    await stalled.promise;
    abort.abort();
    assert.match((await partial).message, /cancelled/);
    child.send("resume");
    const cancellation = await cancelledOnWire.promise;
    assert.deepEqual(observed, [{ phase: "request", id: 1, received: text.length }],
      "the original complete Unicode frame must precede its cancellation");
    assert.deepEqual(cancellation, { phase: "cancelled", id: 1, requests: 1 },
      "cancellation must pass queued calls without cutting an admitted frame");
    const failure = await failed.promise;
    assert.equal(await next, failure);
    await assert.rejects(client.request("late", {}), error => error === failure);
  } finally {
    child.send("exit");
    assert.deepEqual(await closed, [0, null]);
    await client.closed;
  }
});
