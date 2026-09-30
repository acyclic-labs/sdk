// Enumerate canonical services; HTTP mappings describe existing provider operations.
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";
import { createSourceInspector } from "./public-rpc-source.mjs";
import { ActorsService } from "../typescript/packages/actors/generated/proto/actors/v1/actors_pb.js";
import { WorkersService } from "../typescript/packages/workers/generated/proto/workers/v1/workers_pb.js";
import { StreamService } from "../typescript/packages/stream/generated/proto/stream/v2/stream_pb.js";
import { BucketsService as BucketsV2, ObjectsService as ObjectsV2, MultipartService as MultipartV2 } from "../typescript/packages/objects/generated/proto/objects/v2/objects_pb.js";

// Retired v1 RPCs remain inventoried from the frozen published descriptor.
const requireObjects = createRequire(new URL("../typescript/packages/objects/package.json", import.meta.url));
const { fromBinary, createFileRegistry } = await import(pathToFileURL(requireObjects.resolve("@bufbuild/protobuf")));
const { FileDescriptorSetSchema } = await import(pathToFileURL(requireObjects.resolve("@bufbuild/protobuf/wkt")));
const legacyRegistry = createFileRegistry(fromBinary(FileDescriptorSetSchema,
  readFileSync(new URL("../compatibility/objects/v1/objects_descriptor.bin", import.meta.url))));
const legacyServices = ["BucketsService", "ObjectsService", "MultipartService", "SnapshotsService"].map(name => {
  const service = legacyRegistry.getService(`acyclic.objects.v1.${name}`);
  if (!service) throw new Error(`missing canonical legacy Objects service: ${name}`);
  return service;
});

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
const inspectSource = createSourceInspector(new URL("..", import.meta.url));
// Retired rows record published history; target rows describe current SDK exposure.
// Implementation fields describe source presence, not package or live acceptance.
const services = [ActorsService, WorkersService, StreamService, BucketsV2, ObjectsV2, MultipartV2, ...legacyServices];
const v2Routes = readFileSync(new URL("../rust/crates/objects/src/v2/mod.rs", import.meta.url), "utf8");
const objectsRoot = readFileSync(new URL("../rust/crates/objects/src/lib.rs", import.meta.url), "utf8");
if (!/pub use v2::\*/.test(objectsRoot) || /objects\.v1|mod provider|mod local;/.test(objectsRoot) ||
    ["provider.rs", "local.rs", "conformance.rs", "generated/acyclic.objects.v1.rs", "generated/acyclic.objects.v1.tonic.rs"].some(path => existsSync(new URL(`../rust/crates/objects/src/${path}`, import.meta.url)))) {
  throw new Error("active Objects v1 Rust exposure has not been retired");
}
const rows = services.flatMap(service => service.methods.map(method => {
  const family = service.typeName.split(".")[1];
  const version = service.typeName.split(".")[2];
  const objectsV2 = family === "objects" && version === "v2";
  const legacy = family === "objects" && version === "v1";
  const http = family === "actors" ? actors[method.localName] : family === "workers" ? workers[method.localName] : (family === "stream" ? stream : objects)[method.name];
  if (!http) throw new Error(`missing HTTP mapping for ${service.typeName}/${method.name}`);
  if (objectsV2 && !v2Routes.includes(`"${http}"`)) throw new Error(`Objects v2 Rust route missing: ${http}`);
  return {
    rpc: `${service.typeName}/${method.name}`,
    kind: method.methodKind,
    request: method.input.typeName,
    response: method.output.typeName,
    contractStatus: legacy ? "retired" : "target",
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
  const gaps = rows.filter(row => row.contractStatus !== "retired" && (!row.rustGrpc || !row.rustHttp || !row.typescriptGrpcNodeBun || !row.typescriptHttp || !row.typescriptPackageExported));
  if (gaps.length) throw new Error(`public SDK surface incomplete:\n${gaps.map(row => `${row.rpc}: ${row.contractStatus === "retired" ? "legacy source retired" : "target transport/package export missing"}`).join("\n")}`);
}
const target = rows.filter(row => row.contractStatus === "target").length;
console.log(`Public RPC matrix: ${target} target operations, ${rows.length - target} retired (${rows.length} total)`);
