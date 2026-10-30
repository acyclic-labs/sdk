// Enumerate canonical services; HTTP mappings describe existing provider operations.
import { readFileSync, writeFileSync } from "node:fs";
import { createSourceInspector } from "./public-rpc-source.mjs";
import { ActorsService } from "../typescript/packages/actors/generated/proto/actors/v1/actors_pb.js";
import { StreamService } from "../typescript/packages/stream/generated/proto/stream/v1/stream_pb.js";
import { BlobsService as BlobsV1 } from "../typescript/packages/objects/generated/proto/objects/v1/objects_pb.js";

const objects = {
  Availability: "blobs/availability", PutBlob: "blobs/put", PutBlobs: "blobs/put-batch", PutManifest: "blobs/put-manifest", GetBlob: "blobs/get", HeadBlob: "blobs/head",
  DeleteBlob: "blobs/delete", ListBlobs: "blobs/list", Retain: "blobs/retain", Release: "blobs/release",
  Collect: "blobs/collect", Usage: "blobs/usage",
};
const stream = {
  InspectIdempotency: "idempotency/inspect", Append: "append", Head: "head", Heads: "heads", Tail: "tail", Fork: "fork",
  Read: "read", Children: "children", ChildrenPage: "children/page",
  Commit: "commit", ReadCommit: "commits/read",
};
function routes(family) {
  const source = readFileSync(new URL(`../rust/crates/${family}/src/lib.rs`, import.meta.url), "utf8");
  return Object.fromEntries([...source.matchAll(/\("([a-zA-Z]+)", "(v1\/[^"]+)"\)/g)].map(match => [match[1], match[2]]));
}
const actors = routes("actors");
const inspectSource = createSourceInspector(new URL("..", import.meta.url));
// Rows describe the sole current SDK contract.
// Implementation fields describe source presence, not package or live acceptance.
const services = [ActorsService, StreamService, BlobsV1];
// Objects v1 routes are exactly the first fields of these Rust tables, however rustfmt lays them out.
const objectsTables = ["HTTP_ROUTES", "HTTP_STREAM_ROUTES"];
const objectsSource = readFileSync(new URL("../rust/crates/objects/src/lib.rs", import.meta.url), "utf8");
const objectsRoutes = new Set(objectsTables.flatMap(table => {
  const body = new RegExp(String.raw`\bpub\s+const\s+${table}\s*:[^=]*=\s*&\[([\s\S]*?)\];`).exec(objectsSource)?.[1];
  if (body === undefined) throw new Error(`Objects v1 Rust route table missing: ${table}`);
  return [...body.matchAll(/\(\s*"([^"]+)"\s*,/g)].map(match => match[1]);
}));
const rows = services.flatMap(service => service.methods.map(method => {
  const family = service.typeName.split(".")[1];
  const version = service.typeName.split(".")[2];
  const objectsV1 = family === "objects" && version === "v1";
  const http = family === "actors" ? actors[method.localName] : (family === "stream" ? stream : objects)[method.name];
  if (!http) throw new Error(`missing HTTP mapping for ${service.typeName}/${method.name}`);
  if (objectsV1 && !objectsRoutes.has(http)) throw new Error(`Objects v1 Rust route missing from ${objectsTables.join(" and ")}: ${http}`);
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
  const gaps = rows.filter(row => !row.rustGrpc || !row.rustHttp || !row.typescriptGrpcNodeBun || !(row.rpc.startsWith("acyclic.actors.") ? row.typescriptGrpcWeb : row.typescriptHttp) || !row.typescriptPackageExported);
  if (gaps.length) throw new Error(`public SDK surface incomplete:\n${gaps.map(row => `${row.rpc}: target transport/package export missing`).join("\n")}`);
}
console.log(`Public RPC matrix: ${rows.length} current operations`);
