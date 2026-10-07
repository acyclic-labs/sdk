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
missing("missing Rust HTTP operation", WorkersService, "InvokeVersion", "rust/crates/workers/src/http.rs", s => s.replace("async fn invoke_version(", "async fn removed_invoke("), "rustHttp");
missing("missing generated Rust gRPC operation", ActorsService, ActorsService.methods[0].name, "rust/crates/actors/src/generated/acyclic.actors.v1.tonic.rs", s => s.replace(`async fn ${ActorsService.methods[0].localName.replace(/[A-Z]/g, c => `_${c.toLowerCase()}`)}(`, "async fn removed("), "rustGrpc");
missing("missing TypeScript gRPC factory", WorkersService, "InvokeVersion", "typescript/packages/workers/src/grpc.ts", s => s.replace("function createWorkersGrpcClient(", "function removed("), "typescriptGrpcNodeBun");
missing("missing TypeScript HTTP operation", WorkersService, "InvokeVersion", "typescript/packages/workers/src/http.ts", s => s.replace("async invokeVersion(", "async removed("), "typescriptHttp");
missing("missing package gRPC export", WorkersService, "InvokeVersion", "typescript/packages/workers/package.json", s => { const manifest = JSON.parse(s); delete manifest.exports["./grpc"]; return JSON.stringify(manifest); }, "typescriptPackageExported");
missing("missing entire HTTP transport", WorkersService, "InvokeVersion", "rust/crates/workers/src/http.rs", () => "", "rustHttp");
missing("server implementation cannot substitute for Rust client", StreamService, "Read", "rust/crates/stream/src/grpc.rs", s => s.replace("async fn read(", "async fn removed_read("), "rustGrpc");
missing("missing inherited Objects HTTP operation", ObjectsService, "PutObject", "typescript/packages/objects/src/v2.ts", s => s.replace("put(", "async removed_put("), "typescriptHttp");
