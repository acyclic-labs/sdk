// Enumerate canonical services; HTTP mappings describe existing provider operations.
import { readFileSync, writeFileSync } from "node:fs";
import { ActorsService } from "../typescript/packages/actors/generated/proto/actors/v1/actors_pb.js";
import { WorkersService } from "../typescript/packages/workers/generated/proto/workers/v1/workers_pb.js";
import { StreamService } from "../typescript/packages/stream/generated/proto/stream/v2/stream_pb.js";
import { BucketsService, ObjectsService, MultipartService, SnapshotsService } from "../typescript/packages/objects/generated/proto/objects/v1/objects_pb.js";

const objects = {
  CreateBucket: "buckets/create", HeadBucket: "buckets/head", DeleteBucket: "buckets/delete",
  PutObject: "objects/put", GetObject: "objects/get", HeadObject: "objects/head",
  DeleteObject: "objects/delete", ListObjects: "objects/list",
  CreateMultipart: "multipart/create", UploadPart: "multipart/upload-part", ListParts: "multipart/list-parts",
  CompleteMultipart: "multipart/complete", AbortMultipart: "multipart/abort",
  CreateSnapshot: "snapshots/create", DestroySnapshot: "snapshots/destroy", ForkSnapshot: "snapshots/fork", ForkBucket: "snapshots/fork",
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
const services = [ActorsService, WorkersService, StreamService, BucketsService, ObjectsService, MultipartService, SnapshotsService];
const rows = services.flatMap(service => service.methods.map(method => {
  const family = service.typeName.split(".")[1];
  const http = family === "actors" ? actors[method.localName] : family === "workers" ? workers[method.localName] : (family === "stream" ? stream : objects)[method.name];
  if (!http) throw new Error(`missing HTTP mapping for ${service.typeName}/${method.name}`);
  return {
    rpc: `${service.typeName}/${method.name}`,
    kind: method.methodKind,
    request: method.input.typeName,
    response: method.output.typeName,
    httpOperation: family === "objects" ? `v1/objects/${http}` : family === "stream" ? `v1/stream/${http}` : http,
    rustGrpc: true,
    rustHttp: family === "actors" || family === "workers",
    typescriptGrpcNodeBun: true,
    typescriptHttp: true,
  };
}));
const output = JSON.stringify(rows, null, 2) + "\n";
const path = new URL("../compatibility/public-rpc-matrix.json", import.meta.url);
if (process.argv[2] === "check") {
  if (readFileSync(path, "utf8") !== output) throw new Error("public RPC matrix is stale");
} else if (process.argv[2] === "write") writeFileSync(path, output);
else throw new Error("expected write or check");
console.log(`Public RPC matrix: ${rows.length} canonical operations`);
