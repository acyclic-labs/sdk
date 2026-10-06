import { readFileSync } from "node:fs";
import { test } from "node:test";
import assert from "node:assert/strict";
import { createSourceInspector } from "./public-rpc-source.mjs";
import { ActorsService } from "../typescript/packages/actors/generated/proto/actors/v1/actors_pb.js";
import { WorkersService } from "../typescript/packages/workers/generated/proto/workers/v1/workers_pb.js";
import { StreamService } from "../typescript/packages/stream/generated/proto/stream/v2/stream_pb.js";
import { ObjectsService } from "../typescript/packages/objects/generated/proto/objects/v2/objects_pb.js";

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
const actorsBuildOwnedGrpc = (rpc, source = read) => {
  const contract = source("rust/crates/actors/src/contract.rs");
  const codegen = source("rust/crates/actors/src/codegen.rs");
  const build = source("rust/crates/actors/build.rs");
  const wire = source("rust/crates/actors/src/wire.rs");
  return build.includes("codegen::generate(out_dir)")
    && codegen.includes(".build_client(true)")
    && codegen.includes(".build_server(true)")
    && wire.includes('include!(concat!(env!("OUT_DIR"), "/rust/acyclic.actors.v1.rs"))')
    && new RegExp(`\\b${rpc.name}\\s*\\{`).test(contract);
};
missing("missing Rust HTTP operation", WorkersService, "InvokeVersion", "rust/crates/workers/src/http.rs", s => s.replace("async fn invoke_version(", "async fn removed_invoke("), "rustHttp");
test("Actors Rust gRPC operation is build-owned", () => {
  const rpc = ActorsService.methods[0];
  assert.equal(actorsBuildOwnedGrpc(rpc), true);
});
test("missing build-owned Actors Rust gRPC operation", () => {
  const rpc = ActorsService.methods[0];
  const path = "rust/crates/actors/src/contract.rs";
  const original = read(path);
  const changed = original.replace(new RegExp(`(\\s)${rpc.name}(\\s*\\{)`), "$1RemovedActorsOperation$2");
  assert.notEqual(changed, original, "negative fixture must remove actual contract operation");
  const inspect = candidate => actorsBuildOwnedGrpc(rpc, pathName => pathName === path ? changed : read(pathName));
  assert.equal(inspect(), false);
});
missing("missing TypeScript gRPC factory", WorkersService, "InvokeVersion", "typescript/packages/workers/src/grpc.ts", s => s.replace("function createWorkersGrpcClient(", "function removed("), "typescriptGrpcNodeBun");
missing("missing TypeScript HTTP operation", WorkersService, "InvokeVersion", "typescript/packages/workers/src/http.ts", s => s.replace("async invokeVersion(", "async removed("), "typescriptHttp");
missing("missing package gRPC export", WorkersService, "InvokeVersion", "typescript/packages/workers/package.json", s => { const manifest = JSON.parse(s); delete manifest.exports["./grpc"]; return JSON.stringify(manifest); }, "typescriptPackageExported");
missing("missing entire HTTP transport", WorkersService, "InvokeVersion", "rust/crates/workers/src/http.rs", () => "", "rustHttp");
missing("server implementation cannot substitute for Rust client", StreamService, "Read", "rust/crates/stream/src/grpc.rs", s => s.replace("async fn read(", "async fn removed_read("), "rustGrpc");
missing("missing inherited Objects HTTP operation", ObjectsService, "PutObject", "typescript/packages/objects/src/v2.ts", s => s.replace("put(", "async removed_put("), "typescriptHttp");
