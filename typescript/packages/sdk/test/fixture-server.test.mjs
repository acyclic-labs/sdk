import assert from "node:assert/strict";
import { test } from "node:test";
import { createServer, connect } from "node:http2";
import { once } from "node:events";
import { createServer as createHttpServer, request as httpRequest } from "node:http";
import { ownFixtureServer } from "../../../../scripts/fixture-server.mjs";

async function bounded(promise) {
  let timer;
  try {
    await Promise.race([promise, new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error("fixture retained its transport")), 3000);
    })]);
  } finally { clearTimeout(timer); }
}

test("fixture shutdown drains a retained HTTP/2 stream after another RPC completes", { timeout: 10000 }, async () => {
  const server = createServer();
  const close = ownFixtureServer(server);
  server.on("stream", (stream, headers) => {
    stream.respond({ ":status": 200 });
    if (headers[":path"] === "/retained") stream.write("retained");
    else stream.end("complete");
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const peer = connect(`http://127.0.0.1:${server.address().port}`);
  peer.on("error", error => assert.equal(error.code, "ECONNRESET"));
  const closed = new Promise(resolve => peer.once("close", resolve));
  try {
    await once(peer, "connect");
    const retained = peer.request({ ":path": "/retained" });
    retained.on("error", error => assert.ok(["ECONNRESET", "ERR_HTTP2_STREAM_CANCEL"].includes(error.code)));
    retained.resume();
    await once(retained, "response");
    const request = peer.request({ ":path": "/" });
    let body = "";
    request.setEncoding("utf8");
    request.on("data", chunk => { body += chunk; });
    await once(request, "end");
    assert.equal(body, "complete");
    assert.equal(peer.destroyed, false);
    await bounded(close());
    await bounded(closed);
    assert.equal(peer.destroyed, true);
  } finally { peer.destroy(); await close(); }
});

test("fixture shutdown also handles an unstarted server", async () => {
  const server = createServer();
  await ownFixtureServer(server)();
  assert.equal(server.listening, false);
});
test("fixture failure drains an unfinished HTTP response without masking the failure", { timeout: 10000 }, async () => {
  const server = createHttpServer((_request, response) => { response.writeHead(200); response.write("partial"); });
  const close = ownFixtureServer(server);
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  let request;
  const response = await new Promise(resolve => {
    request = httpRequest({ hostname: "127.0.0.1", port: server.address().port });
    request.once("response", resolve);
    request.end();
  });
  response.on("error", error => assert.equal(error.code, "ECONNRESET"));
  const closed = new Promise(resolve => response.once("close", resolve));
  try {
    const [body] = await once(response, "data");
    assert.equal(body.toString(), "partial");
    assert.equal(response.complete, false);
    const failure = new Error("fixture assertion failed");
    await assert.rejects(async () => {
      try { throw failure; }
      finally { await bounded(close()); }
    }, error => error === failure);
    await bounded(closed);
    assert.equal(response.complete, false);
  } finally { request.destroy(); await close(); }
});