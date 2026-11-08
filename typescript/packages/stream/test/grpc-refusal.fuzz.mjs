import assert from "node:assert/strict";
import { createServer, connect } from "node:http2";
import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import { ConnectError, Code } from "@connectrpc/connect";
import { findTrailerError } from "@connectrpc/connect/protocol-grpc";
import { refusalFromGrpcHeaders } from "@acyclic-labs/auth/grpc-refusal";
import { ownFixtureServer } from "../../../../scripts/fixture-server.mjs";
import { producer } from "../../objects/test/refusal.fuzz.mjs";

let original;
async function capture() {
  const diagnostic = (await producer()).refusal;
  const encoded = Buffer.from(diagnostic.details).toString("base64");
  const server = createServer();
  const close = ownFixtureServer(server);
  server.on("stream", stream => stream.respond({ ":status": 200,
    "grpc-status": String(Code.Unknown), "grpc-message": encodeURIComponent(diagnostic.message),
    "grpc-status-details-bin": encoded, "acyclic-original": [encoded, encoded],
  }, { endStream: true }));
  let client;
  try {
    await new Promise((resolve, reject) => {
      server.once("error", reject);
      server.once("close", () => reject(new Error("original HTTP/2 listener closed before admission")));
      server.listen(0, "localhost", resolve);
    });
    client = connect(`http://localhost:${server.address().port}`);
    const raw = await new Promise((resolve, reject) => {
      client.once("error", reject);
      const request = client.request({ ":path": "/" });
      request.once("response", (_headers, _flags, pairs) => resolve(pairs));
      request.once("error", reject);
      request.once("close", () => reject(new Error("original HTTP/2 header producer closed before its response")));
      request.end();
    });
    return { raw, diagnostic };
  } finally { client?.destroy(); await close(); }
}

/** Seed pairs come from Node's real HTTP/2 writer and parser. Arbitrary input
 * derives adversarial diagnostic operands, not captured provider attribution. */
export async function fuzz(input) {
  original ??= capture();
  const { raw: seed, diagnostic } = await original;
  const raw = seed.slice();
  for (let i = 0; i < raw.length; i += 2) {
    if (raw[i] === "acyclic-original") raw[i + 1] = Buffer.from(input).toString("latin1");
    if (raw[i] === "grpc-status-details-bin") raw[i + 1] = Buffer.from(input).toString("base64");
  }
  const pairs = [];
  for (let i = 0; i < raw.length; i += 2) pairs.push([raw[i], raw[i + 1]]);
  let projected;
  try { projected = findTrailerError(new Headers(pairs)); }
  catch (error) { projected = ConnectError.from(error); }
  assert.ok(projected, "the original peer status must refuse");
  const at = ((input[0] ?? 0) % (raw.length / 2 + 1)) * 2;
  const actual = refusalFromGrpcHeaders(projected, [raw.slice(0, at), raw.slice(at)]);
  assert.equal(actual.status, Code.Unknown);
  assert.equal(actual.message, diagnostic.message);
  assert.deepEqual(Array.from(actual.details), Array.from(input));
  assert.deepEqual(actual.metadata.map(([name, bytes]) => [name, Array.from(bytes)]),
    Array.from({ length: 2 }, () => ["acyclic-original", Array.from(input)]));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const paths = process.argv.slice(2);
  if (paths.length === 0) throw new Error("gRPC refusal corpus missing; recovery: node typescript/packages/stream/test/grpc-refusal.fuzz.mjs rust/codec-fuzz/corpus/protobuf/acyclic.objects.v1.ErrorDetail");
  for (const path of paths) await fuzz(await readFile(path));
  console.log(`replayed ${paths.length} original HTTP/2 refusal corpus inputs`);
}
