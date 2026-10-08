// Enumerate canonical services; HTTP mappings describe existing provider operations.
import { readFileSync, writeFileSync } from "node:fs";
import { createSourceInspector } from "./public-rpc-source.mjs";
import { ActorsService } from "../typescript/packages/actors/generated/proto/actors/v1/actors_pb.js";
import { WorkersService } from "../typescript/packages/workers/generated/proto/workers/v1/workers_pb.js";
import { StreamService } from "../typescript/packages/stream/generated/proto/stream/v1/stream_pb.js";
import { BucketsService as BucketsV1, ObjectsService as ObjectsV1, MultipartService as MultipartV1 } from "../typescript/packages/objects/generated/proto/objects/v1/objects_pb.js";

const objects = {
  CreateBucket: "buckets/create", HeadBucket: "buckets/head", DeleteBucket: "buckets/delete",
  PutObject: "objects/put", GetObject: "objects/get", HeadObject: "objects/head",
  DeleteObject: "objects/delete", ListObjects: "objects/list",
  CreateMultipart: "multipart/create", UploadPart: "multipart/upload-part", ListParts: "multipart/list-parts",
  CompleteMultipart: "multipart/complete", AbortMultipart: "multipart/abort",
};
const stream = {
  InspectIdempotency: "idempotency/inspect", Append: "append", Tail: "tail", Fork: "fork",
  Read: "read", Follow: "tail + read (polling)", Children: "children", ChildrenPage: "children/page",
  Commit: "commit", ReadCommit: "commits/read",
};
function routes(family) {
  const source = readFileSync(new URL(`../rust/crates/${family}/src/lib.rs`, import.meta.url), "utf8");
  return Object.fromEntries([...source.matchAll(/\("([a-zA-Z]+)", "(v1\/[^"]+)"\)/g)].map(match => [match[1], match[2]]));
}
const actors = routes("actors");
const workers = routes("workers");
const inspectSource = createSourceInspector(new URL("..", import.meta.url));
// Rows describe the sole current SDK contract.
// Implementation fields describe source presence, not package or live acceptance.
const services = [ActorsService, WorkersService, StreamService, BucketsV1, ObjectsV1, MultipartV1];
const v1Routes = readFileSync(new URL("../rust/crates/objects/src/v1/mod.rs", import.meta.url), "utf8");
const objectsRoot = readFileSync(new URL("../rust/crates/objects/src/lib.rs", import.meta.url), "utf8");
if (!/pub use v1::\*/.test(objectsRoot)) throw new Error("current Objects Rust exposure is missing");
const rows = services.flatMap(service => service.methods.map(method => {
  const family = service.typeName.split(".")[1];
  const version = service.typeName.split(".")[2];
  const objectsV1 = family === "objects" && version === "v1";
  const http = family === "actors" ? actors[method.localName] : family === "workers" ? workers[method.localName] : (family === "stream" ? stream : objects)[method.name];
  if (!http) throw new Error(`missing HTTP mapping for ${service.typeName}/${method.name}`);
  if (objectsV1 && !v1Routes.includes(`"${http}"`)) throw new Error(`Objects v1 Rust route missing: ${http}`);
  return {
    rpc: `${service.typeName}/${method.name}`,
    kind: method.methodKind,
    request: method.input.typeName,
    response: method.output.typeName,
    contractStatus: "target",
    httpOperation: family === "objects" ? `${version}/objects/${http}` : family === "stream" ? `v1/stream/${http}` : http,
    ...inspectSource(service, method),
  };
}));
const output = JSON.stringify(rows, null, 2) + "\n";
const path = new URL("../compatibility/public-rpc-matrix.json", import.meta.url);
if (process.argv[2] === "check" || process.argv[2] === "complete") {
  if (readFileSync(path, "utf8") !== output) throw new Error("public RPC matrix is stale");
} else if (process.argv[2] === "write") writeFileSync(path, output);
else throw new Error("expected write, check, or complete");
if (process.argv[2] === "complete") {
  const gaps = rows.filter(row => !row.rustGrpc || !row.rustHttp || !row.typescriptGrpcNodeBun || !row.typescriptHttp || !row.typescriptPackageExported);
  if (gaps.length) throw new Error(`public SDK surface incomplete:\n${gaps.map(row => `${row.rpc}: target transport/package export missing`).join("\n")}`);
}
console.log(`Public RPC matrix: ${rows.length} current operations`);
