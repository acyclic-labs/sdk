import { readFileSync } from "node:fs";
import { test } from "node:test";
import assert from "node:assert/strict";
import { createSourceInspector } from "./public-rpc-source.mjs";
import { ActorsService } from "../typescript/packages/actors/generated/proto/actors/v1/actors_pb.js";
import { StreamService } from "../typescript/packages/stream/generated/proto/stream/v1/stream_pb.js";
import { BlobsService } from "../typescript/packages/objects/generated/proto/objects/v1/objects_pb.js";

import { MachinesService } from "../typescript/packages/machines/generated/proto/machines/v1/machines_pb.js";
const root = new URL("..", import.meta.url);
const read = path => { try { return readFileSync(new URL(path, root), "utf8"); } catch (error) { if (error.code === "ENOENT") return ""; throw error; } };
function missing(label, service, rpc, path, replace, flag) {
  test(label, () => {
    const method = service.methods.find(method => method.name === rpc);
    assert.ok(method);
    assert.equal(createSourceInspector(root)(service, method)[flag], true);
    const original = read(path);
    const changed = replace(original);
    assert.notEqual(changed, original, "negative fixture must remove actual exposure");
    const inspect = createSourceInspector(root, candidate => candidate === path ? changed : read(candidate));
    assert.equal(inspect(service, method)[flag], false);
  });
}
missing("missing Rust-owned Actors gRPC operation", ActorsService, ActorsService.methods[0].name, "rust/crates/actors/src/client.rs", s => s.replace(`("${ActorsService.methods[0].name}",`, '("Removed",').replace(`operation!(\n    ${ActorsService.methods[0].localName.replace(/[A-Z]/g, c => `_${c.toLowerCase()}`)},`, "operation!(\n    removed,"), "rustGrpc");
missing("missing Actors WASM bridge", ActorsService, "DeployNamespace", "rust/crates/actors-wasm/src/lib.rs", s => s.replace("pub struct ActorsClient", "pub struct Removed"), "typescriptGrpcWeb");
missing("server implementation cannot substitute for Rust client", StreamService, "Read", "rust/crates/stream/src/grpc/client.rs", s => s.replace("async fn read(", "async fn removed_read("), "rustGrpc");
missing("missing inherited Objects HTTP operation", BlobsService, "PutBlob", "typescript/packages/objects/src/v1.ts", s => s.replace("async put(", "async removed_put("), "typescriptHttp");

test("missing generated Actors operation removes both TypeScript capability flags", () => {
  const method = ActorsService.methods[0];
  const path = "typescript/packages/actors/src/generated/actors-service.ts";
  const original = read(path);
  const changed = original.replace(`"${method.input.typeName}":`, '"removedOperation":');
  assert.notEqual(changed, original);
  const inspect = createSourceInspector(root, candidate => candidate === path ? changed : read(candidate));
  const result = inspect(ActorsService, method);
  assert.equal(result.typescriptGrpcNodeBun, false);
  assert.equal(result.typescriptGrpcWeb, false);
});

test("missing Actors native bridge removes the Node/Bun capability", () => {
  const method = ActorsService.methods[0];
  const path = "typescript/packages/actors/src/client.ts";
  const original = read(path);
  const changed = original.replaceAll("nativeBinding()", "removedNativeBinding()");
  assert.notEqual(changed, original);
  const inspect = createSourceInspector(root, candidate => candidate === path ? changed : read(candidate));
  assert.equal(inspect(ActorsService, method).typescriptGrpcNodeBun, false);
});

test("missing Actors client export removes the package capability", () => {
  const method = ActorsService.methods[0];
  const path = "typescript/packages/actors/package.json";
  const original = read(path);
  const manifest = JSON.parse(original);
  delete manifest.exports["./client"];
  const changed = JSON.stringify(manifest);
  assert.notEqual(changed, original);
  const inspect = createSourceInspector(root, candidate => candidate === path ? changed : read(candidate));
  assert.equal(inspect(ActorsService, method).typescriptPackageExported, false);
});

for (const service of [BlobsService, StreamService]) {
  test(`${service.typeName} rejects the obsolete namespace`, () => {
    const obsolete = { ...service, typeName: service.typeName.replace(".v1.", ".v2.") };
    assert.throws(() => createSourceInspector(root)(obsolete, service.methods[0]), /unsupported current contract/);
  });
}
missing("missing Stream factory delegation removes Node/Bun exposure", StreamService, "Read", "typescript/packages/stream/src/grpc.ts", s => s.replace("const { client, sessions } = streamGrpc(options)", "const { client, sessions } = removed(options)"), "typescriptGrpcNodeBun");
missing("missing Stream generated client removes Node/Bun exposure", StreamService, "Read", "typescript/packages/stream/src/grpc.ts", s => s.replace("createClient(StreamService, home)", "removed(StreamService, home)"), "typescriptGrpcNodeBun");
missing("missing Stream transport removes Node/Bun exposure", StreamService, "Read", "typescript/packages/stream/src/grpc.ts", s => s.replace("return createGrpcTransport(", "return removedTransport("), "typescriptGrpcNodeBun");

missing("missing Machines descriptor transport removes package exposure", MachinesService, "WatchOperation", "typescript/packages/machines/src/transport.ts", s => s.replace("createClient(wire.MachinesService, transport)", "removed(wire.MachinesService, transport)"), "typescriptPackageExported");
missing("missing Machines transport export removes package exposure", MachinesService, "WatchOperation", "typescript/packages/machines/package.json", s => { const manifest = JSON.parse(s); delete manifest.exports["./transport"]; return JSON.stringify(manifest); }, "typescriptPackageExported");
