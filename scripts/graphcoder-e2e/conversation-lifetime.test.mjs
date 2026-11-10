import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer, connect } from "node:net";
import test from "node:test";
import { waitFor } from "./conversation.mjs";

test("a cancelled conversation releases a real gate that never receives readiness", async () => {
  const server = createServer();
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const connected = once(server, "connection");
  const client = connect(server.address().port, "127.0.0.1");
  const [peer] = await connected;
  const controller = new AbortController();
  const failure = new Error("original child exited before readiness");
  const ready = once(peer, "data", { signal: controller.signal });
  const waiting = waitFor(ready, controller.signal);
  const rejected = assert.rejects(waiting, error => error === failure || error.name === "AbortError");
  try {
    const closed = once(peer, "close");
    client.destroy();
    await closed;
    controller.abort(failure);
    await rejected;
    assert.equal(peer.listenerCount("data"), 0, "cancelled readiness retained its socket listener");
  } finally {
    controller.abort(failure);
    client.destroy(); peer.destroy();
    await new Promise(resolve => server.close(resolve));
  }
});
