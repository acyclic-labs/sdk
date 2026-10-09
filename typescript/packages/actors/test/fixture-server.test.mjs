import assert from "node:assert/strict";
import { connect, createServer } from "node:http2";
import { once } from "node:events";
import { test } from "node:test";
import { fixtureServer } from "./fixture-server.mjs";

test("fixture shutdown closes a persistent HTTP/2 client session", { timeout: 5000 }, async () => {
  const server = createServer();
  const close = fixtureServer(server);
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const accepted = once(server, "session");
  const client = connect(`http://127.0.0.1:${server.address().port}`);
  try {
    await accepted;
    await once(client, "connect");
    const errors = [];
    client.on("error", error => errors.push(error));
    const disconnected = new Promise(resolve => client.once("close", resolve));
    await close();
    await disconnected;
    assert.equal(server.listening, false);
    assert.equal(client.destroyed, true);
    for (const error of errors) assert.equal(error.code, "ECONNRESET");
  } finally {
    client.destroy();
    if (server.listening) await close();
  }
});
