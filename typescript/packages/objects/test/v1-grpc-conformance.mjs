import { ownFixtureServer } from "../../../../scripts/fixture-server.mjs";
import assert from "node:assert/strict";
import { createSecureServer } from "node:http2";
import { spawn, spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { existsSync } from "node:fs";
import { join } from "node:path";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { Code, ConnectError } from "@connectrpc/connect";
import { connectNodeAdapter } from "@connectrpc/connect-node";
import * as wire from "@acyclic-labs/objects/proto";
import { GrpcObjectsV1 } from "@acyclic-labs/objects/grpc";
import { MemoryObjectsV1 } from "@acyclic-labs/objects";
import { ObjectsV1Memory } from "../generated/wasm/acyclic_objects_wasm.js";
import { lifecycle } from "./v1-lifecycle.mjs";

const root = fileURLToPath(new URL("../../../../", import.meta.url));
const file = fileURLToPath(import.meta.url);
const routes = {
  PutBlob: "blobs/put", PutManifest: "blobs/put-manifest", GetBlob: "blobs/get", HeadBlob: "blobs/head", DeleteBlob: "blobs/delete",
  ListBlobs: "blobs/list", Retain: "blobs/retain", Release: "blobs/release", Collect: "blobs/collect", Usage: "blobs/usage",
};
if (process.argv.includes("--client")) {
  let config = "";
  for await (const chunk of process.stdin) config += chunk;
  const options = JSON.parse(config);
  const client = new GrpcObjectsV1(options);
  const wrong = new GrpcObjectsV1({ ...options, token: "wrong" });
  try {
    await lifecycle(client);
    await assert.rejects(wrong.usage(), error => error.code === wire.ErrorCode.ACCESS_DENIED);
  } finally { client.close(); wrong.close(); }
  console.log(`${process.versions.bun ? "Bun" : "Node"} Objects v1 TLS gRPC: blob lifecycle, typed errors and authentication pass`);
} else {
  await MemoryObjectsV1.create();
  const certificate = process.env.ACYCLIC_CONFORMANCE_CERTIFICATE_BIN
    ?? join(process.env.CARGO_TARGET_DIR ?? join(root, "target"), "debug", "examples", `conformance-certificate${process.platform === "win32" ? ".exe" : ""}`);
  const generated = existsSync(certificate)
    ? spawnSync(certificate, [], { cwd: root, encoding: "utf8" })
    : spawnSync("cargo", ["run", "--quiet", "--locked", "-p", "acyclic-actors", "--example", "conformance-certificate"], { cwd: root, encoding: "utf8" });
  if (generated.status !== 0) throw new Error(generated.stderr);
  const identity = JSON.parse(generated.stdout);
  let memory;
  const seen = new Set();
  const adapter = connectNodeAdapter({ routes(router) {
    const implementation = {};
    for (const method of wire.BlobsService.methods) {
      implementation[method.localName] = async (query, context) => {
        try {
          if (context.requestHeader.get("authorization") !== "Bearer fixture") throw new ConnectError("missing bearer", Code.Unauthenticated);
          seen.add(method.name);
          return fromBinary(method.output, await memory.invoke(routes[method.name], toBinary(method.input, query)));
        } catch (error) {
          if (error instanceof ConnectError) throw error;
          const code = typeof error.code === "number" ? error.code : wire.ErrorCode.UNAVAILABLE;
          throw new ConnectError("Objects fixture rejected request", Code.Unknown, undefined, [{ desc: wire.ErrorDetailSchema, value: create(wire.ErrorDetailSchema, { code }) }]);
        }
      };
    }
    router.service(wire.BlobsService, implementation);
  } });
  const server = createSecureServer({ key: identity.key, cert: identity.certificate }, adapter);
  const closeServer = ownFixtureServer(server);
  try {
    await new Promise(resolve => server.listen(0, "localhost", resolve));
    const options = { endpoint: `https://localhost:${server.address().port}`, token: "fixture", caCertificate: identity.certificate };
    for (const runtime of [process.execPath, "bun"]) {
      memory = new ObjectsV1Memory();
      seen.clear();
      await new Promise((resolve, reject) => {
        const child = spawn(runtime, [file, "--client"], { cwd: root, stdio: ["pipe", "inherit", "inherit"] });
        child.stdin.end(JSON.stringify(options));
        child.on("error", reject);
        child.on("exit", code => code === 0 ? resolve() : reject(new Error(`${runtime} exited ${code}`)));
      });
      assert.deepEqual([...seen].sort(), Object.keys(routes).sort());
    }
  } finally { await closeServer(); }
}
