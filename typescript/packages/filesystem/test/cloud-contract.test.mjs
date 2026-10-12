import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { createSecureServer } from "node:http2";
import { once } from "node:events";
import test from "node:test";
import { isDeepStrictEqual } from "node:util";
import { fromJson, toJson } from "@bufbuild/protobuf";
import { connectNodeAdapter } from "@connectrpc/connect-node";
import { createFilesystemGrpcClient } from "../dist/grpc.js";
import { FilesystemService } from "../generated/proto/filesystem/v2/filesystem_pb.js";
import { ownFixtureServer } from "../../../../scripts/fixture-server.mjs";

const fixtureDirectory = new URL("../../../../test-fixtures/cloud-filesystem/", import.meta.url);
const source = JSON.parse(readFileSync(new URL("source.json", fixtureDirectory), "utf8"));
const cases = JSON.parse(readFileSync(new URL("golden.json", fixtureDirectory), "utf8"));

test("Cloud filesystem fixture provenance pins its handler tests and exact descriptor", () => {
  assert.equal(source.repository, "acyclic-labs/cloud");
  assert.match(source.commit, /^[a-f0-9]{40}$/);
  assert.match(source.sdk_pin, /^[a-f0-9]{40}$/);
  assert.equal(source.test, "write_read_list_and_delete_through_reconstructed_replica");
  assert.ok(source.files.some(file => file.path === "svc/fs/tests/multi_tenant.rs"));
  for (const file of source.files) assert.match(file.blob, /^[a-f0-9]{40}$/);
  const descriptor = readFileSync(new URL("../../../../rust/crates/filesystem/src/generated/acyclic-filesystem-v2.bin", import.meta.url));
  const blob = createHash("sha1").update(`blob ${descriptor.length}\0`).update(descriptor).digest("hex");
  assert.equal(blob, source.descriptor_blob, "refresh Cloud's SDK pin and these fixtures together");
  for (const method of ["CreateWorkspace", "ApplyTransaction", "Read", "ListDirectory", "DeleteWorkspace"]) {
    assert.ok(cases.some(row => row.method === method), `missing ${method} golden`);
  }
});

test("TypeScript fs client replays Cloud create/write/read/list/delete JSON over real HTTP/2 gRPC", async t => {
  assert.equal(FilesystemService.typeName, "acyclic.filesystem.v2.FilesystemService");
  const called = [];
  const implementation = {};
  for (const method of FilesystemService.methods) {
    const rows = cases.filter(row => row.method === method.name);
    if (!rows.length) continue;
    implementation[method.localName] = (request, context) => {
      assert.equal(context.requestHeader.get("authorization"), "Bearer fixture");
      const json = toJson(method.input, request);
      const row = rows.find(candidate => isDeepStrictEqual(candidate.request, json));
      assert.ok(row, `${method.name} request differs from Cloud's golden JSON`);
      called.push(method.name);
      return fromJson(method.output, row.response);
    };
  }
  const cert = readFileSync(new URL("../../machines/test/fixtures/server.pem", import.meta.url), "utf8");
  const key = readFileSync(new URL("../../machines/test/fixtures/server-key.pem", import.meta.url), "utf8");
  const server = createSecureServer({ cert, key }, connectNodeAdapter({
    routes(router) { router.service(FilesystemService, implementation); },
  }));
  const close = ownFixtureServer(server);
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const client = createFilesystemGrpcClient({
    endpoint: `https://127.0.0.1:${server.address().port}`, token: "fixture", caCertificate: cert,
  });
  t.after(async () => { client.close(); await close(); });
  for (const row of cases) {
    const method = FilesystemService.methods.find(candidate => candidate.name === row.method);
    assert.ok(method, `missing ${row.method} in the public client`);
    assert.equal(method.input.typeName, `acyclic.filesystem.v2.${row.input}`);
    assert.equal(method.output.typeName, `acyclic.filesystem.v2.${row.output}`);
    const response = await client.filesystem[method.localName](fromJson(method.input, row.request));
    assert.deepEqual(toJson(method.output, response), row.response);
  }
  assert.deepEqual(called, cases.map(row => row.method));
});
