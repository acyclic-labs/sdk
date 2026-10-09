import { readFileSync } from "node:fs";
import { test } from "node:test";
import assert from "node:assert/strict";
import { createSourceInspector } from "./public-rpc-source.mjs";
import { ActorsService } from "../typescript/packages/actors/generated/proto/actors/v1/actors_pb.js";
import { WorkersService } from "../typescript/packages/workers/generated/proto/workers/v1/workers_pb.js";
import { StreamService } from "../typescript/packages/stream/generated/proto/stream/v1/stream_pb.js";
import { ObjectsService } from "../typescript/packages/objects/generated/proto/objects/v1/objects_pb.js";

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
missing("missing Rust HTTP operation", WorkersService, "InvokeVersion", "rust/crates/workers/src/http.rs", s => s.replace("async fn invoke_version(", "async fn removed_invoke("), "rustHttp");
missing("missing Rust-owned Actors gRPC operation", ActorsService, ActorsService.methods[0].name, "rust/crates/actors/src/client.rs", s => s.replace(`operation!(\n    ${ActorsService.methods[0].localName.replace(/[A-Z]/g, c => `_${c.toLowerCase()}`)},`, "operation!(\n    removed,"), "rustGrpc");
missing("missing TypeScript gRPC factory", WorkersService, "InvokeVersion", "typescript/packages/workers/src/client.ts", s => s.replace("export const createWorkersGrpcClient =", "export const removed ="), "typescriptGrpcNodeBun");
missing("missing Workers WASM loader", WorkersService, "InvokeVersion", "typescript/packages/workers/src/binding.ts", s => s.replace("function loadWasm()", "function removed()"), "typescriptGrpcWeb");
missing("missing Workers native bridge", WorkersService, "InvokeVersion", "typescript/packages/workers/src/binding.ts", s => s.replace("function loadNative()", "function removed()"), "typescriptGrpcNodeBun");
missing("missing Workers Rust operation", WorkersService, "InvokeVersion", "rust/crates/workers/src/client_binding.rs", s => s.replace('( "InvokeVersion",', '( "Removed",').replace('("InvokeVersion",', '("Removed",'), "rustGrpc");
missing("missing Actors WASM bridge", ActorsService, "CreateActor", "rust/crates/actors-wasm/src/lib.rs", s => s.replace("pub struct ActorsClient", "pub struct Removed"), "typescriptGrpcWeb");
missing("missing Workers browser Rust transport", WorkersService, "InvokeVersion", "rust/crates/workers/src/client_binding.rs", s => s.replace("tonic_web_wasm_client::Client::new_with_options(", "removed("), "typescriptGrpcWeb");
missing("missing package gRPC export", WorkersService, "InvokeVersion", "typescript/packages/workers/package.json", s => { const manifest = JSON.parse(s); delete manifest.exports["./grpc"]; return JSON.stringify(manifest); }, "typescriptPackageExported");
missing("missing entire HTTP transport", WorkersService, "InvokeVersion", "rust/crates/workers/src/http.rs", () => "", "rustHttp");
missing("server implementation cannot substitute for Rust client", StreamService, "Read", "rust/crates/stream/src/grpc.rs", s => s.replace("async fn read(", "async fn removed_read("), "rustGrpc");
missing("missing inherited Objects HTTP operation", ObjectsService, "PutObject", "typescript/packages/objects/src/v1.ts", s => s.replace("put(", "async removed_put("), "typescriptHttp");

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

for (const service of [ObjectsService, StreamService]) {
  test(`${service.typeName} rejects the obsolete namespace`, () => {
    const obsolete = { ...service, typeName: service.typeName.replace(".v1.", ".v2.") };
    assert.throws(() => createSourceInspector(root)(obsolete, service.methods[0]), /unsupported current contract/);
  });
}
test("additional Objects contract module fails closed", () => {
  const path = "rust/crates/objects/src/lib.rs";
  const inspect = createSourceInspector(root, candidate => candidate === path ? read(path) + "\npub mod v2;\n" : read(candidate));
  assert.throws(() => inspect(ObjectsService, ObjectsService.methods[0]), /one current contract module/);
});
