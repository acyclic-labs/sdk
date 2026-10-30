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
function rows(service, readSource = read) {
  const inspect = createSourceInspector(root, readSource);
  try { return service.methods.map(method => inspect(service, method)); }
  finally { inspect.close(); }
}
function removed(label, service, rpc, path, replace, flag) {
  test(label, () => {
    const index = service.methods.findIndex(method => method.name === rpc);
    assert.ok(index >= 0);
    const before = rows(service);
    assert.equal(before[index][flag], true);
    const original = read(path);
    const changed = replace(original);
    assert.notEqual(changed, original, "negative control must remove an actual implementation");
    const after = rows(service, candidate => candidate === path ? changed : read(candidate));
    const expected = structuredClone(before);
    expected[index][flag] = false;
    assert.deepEqual(after, expected, "only the removed operation's capability may change");
  });
}
removed("removing an inherited Objects implementation changes exactly its HTTP row", BlobsService, "PutBlob", "typescript/packages/objects/src/v1.ts",
  source => source.replace("async put(", "async removed_put("), "typescriptHttp");
removed("compiler-emitted Rust inventory changes exactly the removed provider row", StreamService, "Read", "compatibility/rpc-implementations/stream.json",
  source => { const inventory = JSON.parse(source); inventory.grpc = inventory.grpc.filter(method => method !== "read"); return JSON.stringify(inventory); }, "rustGrpc");

for (const [name, flag] of [["nativeBinding", "typescriptGrpcNodeBun"], ["wasmBinding", "typescriptGrpcWeb"]]) {
  test(`removing the typed Actors ${name} declaration removes its bridge exposure`, () => {
    const path = "typescript/packages/actors/src/client.ts";
    const original = read(path);
    const changed = original.replace(`function ${name}(`, `function removed_${name}(`);
    assert.notEqual(changed, original);
    assert.ok(rows(ActorsService).every(row => row[flag]));
    assert.ok(rows(ActorsService, candidate => candidate === path ? changed : read(candidate)).every(row => !row[flag]));
  });
}

test("removing the public Actors client export removes its package exposure", () => {
  const path = "typescript/packages/actors/package.json";
  const manifest = JSON.parse(read(path));
  delete manifest.exports["./client"];
  assert.ok(rows(ActorsService).every(row => row.typescriptPackageExported));
  assert.ok(rows(ActorsService, candidate => candidate === path ? JSON.stringify(manifest) : read(candidate)).every(row => !row.typescriptPackageExported));
});

test("an unresolved Stream factory cannot substitute for a typed generated client", () => {
  const path = "typescript/packages/stream/src/grpc.ts";
  const original = read(path);
  const changed = original.replace("const { client, sessions } = streamGrpc(options)", "const { client, sessions } = removed(options)");
  assert.notEqual(changed, original);
  assert.ok(rows(StreamService).every(row => row.typescriptGrpcNodeBun));
  const index = StreamService.methods.findIndex(method => method.name === "Read");
  assert.equal(rows(StreamService, candidate => candidate === path ? changed : read(candidate))[index].typescriptGrpcNodeBun, false);
});

for (const service of [BlobsService, StreamService]) {
  test(`${service.typeName} rejects the obsolete namespace`, () => {
    const inspect = createSourceInspector(root);
    try { assert.throws(() => inspect({ ...service, typeName: service.typeName.replace(".v1.", ".v2.") }, service.methods[0]), /unsupported current contract/); }
    finally { inspect.close(); }
  });
}

test("Machines package export remains tied to the typed generated transport", () => {
  const method = MachinesService.methods.find(method => method.name === "WatchOperation");
  const path = "typescript/packages/machines/src/transport.ts";
  const source = read(path).replace("createClient(wire.MachinesService, transport)", "removed(wire.MachinesService, transport)");
  assert.notEqual(source, read(path));
  const inspect = createSourceInspector(root);
  const missing = createSourceInspector(root, candidate => candidate === path ? source : read(candidate));
  try {
    assert.equal(inspect(MachinesService, method).typescriptPackageExported, true);
    assert.equal(missing(MachinesService, method).typescriptPackageExported, false);
  } finally { inspect.close(); missing.close(); }
});
