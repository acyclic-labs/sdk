import {test} from "node:test";
import assert from "node:assert/strict";
import { once } from "node:events";
import { connect, constants, createServer } from "node:http2";
import { Readable } from "node:stream";
import { openGuestResponse } from "../dist/fetch-response.js";

test("native HTTP/2 keeps upload streaming, terminal trailers and original close custody", async context => {
  const server = createServer();
  let client;
  context.signal.addEventListener("abort", () => { client?.destroy(); server.close(); }, { once: true });
  const cancelled = Promise.withResolvers();
  const completed = Promise.withResolvers();
  const failures = [];
  const closes = new Map();
  server.on("stream", (stream, request) => {
    const path = String(request[":path"]);
    stream.on("error", error => failures.push(error));
    const terminal = Promise.withResolvers();
    stream.once("end", () => terminal.resolve(new Headers({ "grpc-status": "0", "x-terminal": "original" })));
    const exchange = openGuestResponse({
      status: 200, headers: new Headers({ "content-type": "application/grpc+proto" }),
      // toWeb on a Duplex waits for its writable completion too. This is the
      // original upload's readable iterator, independent of response EOF.
      body: Readable.toWeb(Readable.from(stream, { objectMode: false })), trailers: terminal.promise,
      close() {
        closes.set(path, (closes.get(path) ?? 0) + 1);
        if (path === "/cancel") cancelled.resolve();
      },
    });
    stream.once("close", exchange.close);
    void (async () => {
      stream.respond({ ":status": exchange.status, ...Object.fromEntries(exchange.headers) }, { waitForTrailers: true });
      for await (const frame of exchange.frames) {
        if ("data" in frame) {
          if (!stream.write(frame.data)) await once(stream, "drain");
        } else {
          stream.once("wantTrailers", () => stream.sendTrailers(Object.fromEntries(frame.trailers)));
        }
      }
      stream.end();
      if (path === "/complete") completed.resolve();
    })().catch(error => {
      // Disconnect cancels the original reader and pending trailer wait.
      if (path !== "/cancel") failures.push(error);
      exchange.close();
    });
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("HTTP/2 address missing");
  client = connect(`http://127.0.0.1:${address.port}`);
  try {
    const request = client.request({ ":method": "POST", ":path": "/complete" }, { endStream: false });
    const trailers = once(request, "trailers");
    const first = once(request, "data");
    request.write(new Uint8Array([1, 2, 3]));
    assert.deepEqual(new Uint8Array((await first)[0]), new Uint8Array([1, 2, 3]));
    // No EOF or trailer existed when the original bytes were already received.
    request.end();
    const [terminal] = await trailers;
    assert.equal(terminal["grpc-status"], "0");
    assert.equal(terminal["x-terminal"], "original");
    await completed.promise;
    assert.equal(closes.get("/complete"), 1);

    const stopped = client.request({ ":method": "POST", ":path": "/cancel" }, { endStream: false });
    const received = once(stopped, "data");
    stopped.write(new Uint8Array([4]));
    await received;
    stopped.close(constants.NGHTTP2_CANCEL);
    await cancelled.promise;
    assert.equal(closes.get("/cancel"), 1);
    assert.deepEqual(failures, []);
  } finally {
    client.destroy();
    await new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
  }
});
