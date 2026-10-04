#!/usr/bin/env node
// Execute the Rust-owned Filesystem/Harness lifecycle assertions against an
// installed generated TypeScript package. The package is loaded from the
// supplied consumer root so this probe cannot accidentally import checkout
// sources.

import assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import { basename, join, resolve } from "node:path";

const args = parseArgs(process.argv.slice(2));
const consumerRoot = resolve(required("consumer-root"));
const scenarioManifestPath = resolve(required("scenario-manifest"));
const outputPath = resolve(required("output"));
const endpoint = args.endpoint ?? process.env.FIXTURE_GRPC_ADDRESS ?? "http://127.0.0.1:50080";
const scenarios = JSON.parse(await readFile(scenarioManifestPath, "utf8"));
if (scenarios.schema !== "acyclic.sdk.rust-rpc-scenarios.v1" || scenarios.count !== scenarios.scenarios.length) {
  fail("Rust-owned semantic scenario manifest is invalid");
}

const moduleRoot = join(consumerRoot, "node_modules");
const protobuf = await import(pathToFileURL(join(moduleRoot, "@bufbuild/protobuf/dist/esm/index.js")));
const connect = await import(pathToFileURL(join(moduleRoot, "@connectrpc/connect/dist/esm/index.js")));
const connectNode = await import(pathToFileURL(join(moduleRoot, "@connectrpc/connect-node/dist/esm/index.js")));
const fs = await import(pathToFileURL(join(moduleRoot, "@acyclic-labs/fs/generated/proto/filesystem/v2/filesystem_pb.js")));
const harness = await import(pathToFileURL(join(moduleRoot, "@acyclic-labs/harness/generated/proto/harness/v2/harness_pb.js")));

const transport = connectNode.createGrpcTransport({
  baseUrl: endpoint,
  readMaxBytes: 2 * 1024 * 1024,
  writeMaxBytes: 2 * 1024 * 1024,
});
const fsClient = connect.createClient(fs.FilesystemService, transport);
const harnessClient = connect.createClient(harness.HarnessService, transport);
const results = [];
const operationKey = new Uint8Array(16).fill(7);

function record(family, operation, expected, actual, status = "passed") {
  results.push({ family, operation, expected, actual, status });
  assert.equal(status, "passed", `${family}/${operation}: ${expected}`);
}

const fsHandshake = await fsClient.handshake(protobuf.create(fs.HandshakeRequestSchema));
record(
  "filesystem",
  "handshake",
  "negotiated protocol and capabilities",
  fsHandshake.protocol && fsHandshake.capabilities?.profiles?.length >= 4 ? "negotiated" : "missing-capabilities",
);

const created = await fsClient.createWorkspace(protobuf.create(fs.CreateWorkspaceRequestSchema, {
  name: "semantic-fixture",
  profile: fs.FilesystemProfile.PORTABLE,
  operation: protobuf.create(fs.OperationOptionsSchema, { idempotencyKey: operationKey }),
}));
assert.ok(created.workspace?.workspace && created.workspace.head, "Rust Filesystem did not return a workspace head");
record("filesystem", "create_workspace", "workspace response", created.workspace?.workspace ? "workspace response" : "missing");

const workspace = created.workspace.workspace;
const head = created.workspace.head;
const opened = await fsClient.openWorkspace(protobuf.create(fs.OpenWorkspaceRequestSchema, {
  selector: { case: "workspace", value: workspace },
}));
record("filesystem", "open_workspace", "workspace response", opened.workspace ? "workspace response" : "missing");

const currentHead = await fsClient.getHead(protobuf.create(fs.GetHeadRequestSchema, { workspace }));
record("filesystem", "get_head", "generation response", currentHead.generation ? "generation response" : "missing");

try {
  await fsClient.read(protobuf.create(fs.ReadRequestSchema, {
    generation: head,
    path: "/missing",
    maximumBytes: 1024n,
  }));
  fail("Filesystem read of a missing path unexpectedly succeeded");
} catch (error) {
  const code = error?.code ?? "unknown";
  record("filesystem", "read", "read response", code === connect.Code.NotFound ? "not-found" : code, code === connect.Code.NotFound ? "passed" : "failed");
}

const harnessHandshake = await harnessClient.handshake(protobuf.create(harness.HandshakeRequestSchema));
const protocol = harnessHandshake.protocol;
const authority = protobuf.create(harness.AuthoritySchema, { kind: harness.AggregateKind.TASK, id: "semantic-fixture" });
const operation = protobuf.create(harness.OperationIdentitySchema, { operationId: "semantic-operation", idempotencyKey: "semantic-key" });
const submit = await harnessClient.submit(protobuf.create(harness.CommandEnvelopeSchema, {
  protocol,
  authority,
  operation,
  actionType: "fixture",
  canonicalActionJson: new Uint8Array([123, 125]),
  intentDigest: new Uint8Array(32).fill(2),
}));
record("harness", "handshake", "negotiated protocol and capabilities", protocol ? "negotiated" : "missing-protocol");
record("harness", "submit", "accepted admission and succeeded status", submit.state === harness.AdmissionState.ACCEPTED ? "accepted" : "rejected");

const control = {
  operationId: operation.operationId,
  protocol,
  owner: authority,
  scope: protobuf.create(harness.ScopeSchema, { id: "semantic-control", capabilities: ["operation:observe", "operation:cancel"], issuer: "runtime", proof: new Uint8Array(32).fill(1) }),
};
const observed = await harnessClient.observe(protobuf.create(harness.ObserveRequestSchema, control));
record("harness", "observe", "succeeded operation status", observed.state === harness.CompletionState.SUCCEEDED ? "succeeded" : "unexpected-state");

const deliveries = [];
for await (const delivery of harnessClient.replay(protobuf.create(harness.ResumeRequestSchema, { protocol, cursors: [] }))) deliveries.push(delivery);
record("harness", "replay", "live delivery at cursor revision", deliveries.length > 0 && deliveries.every((value) => value.live) ? "live-delivery" : "missing-delivery");

const cancelled = await harnessClient.cancel(protobuf.create(harness.CancelRequestSchema, {
  ...control,
  recursive: true,
  idempotencyKey: "semantic-cancel",
}));
record("harness", "cancel", "cancelled status with incremented revision", cancelled.operation?.state === harness.CompletionState.CANCELLED && cancelled.operation.revision > observed.revision ? "cancelled" : "unexpected-state");

const covered = new Set(results.map(({ family, operation }) => `${family}::${operation}`));
const expected = scenarios.scenarios.map((scenario) => `${scenario.family}::${pascal(scenario.operation)}`);
const missing = expected.filter((key) => !covered.has(key));
const receipt = {
  schema: "acyclic.sdk.rust-rpc-semantic-receipt.v1",
  status: missing.length === 0 ? "passed" : "partial",
  endpoint,
  consumer_root: consumerRoot,
  scenario_manifest: scenarioManifestPath,
  scenario_manifest_schema: scenarios.schema,
  scenario_count: scenarios.count,
  assertions: results,
  semantic_assertion_count: results.length,
  missing_semantic_assertions: missing,
  package_artifacts: ["@acyclic-labs/fs", "@acyclic-labs/harness"],
  source: "Rust FilesystemWireService and HarnessGrpcService",
};
await writeFile(outputPath, `${JSON.stringify(receipt, null, 2)}\n`);
console.log(JSON.stringify(receipt, null, 2));

function parseArgs(values) {
  const parsed = {};
  for (let index = 0; index < values.length; index += 1) {
    const value = values[index];
    if (!value.startsWith("--")) fail(`unexpected argument ${value}`);
    parsed[value.slice(2)] = values[++index];
    if (!parsed[value.slice(2)] || parsed[value.slice(2)].startsWith("--")) fail(`missing value for ${value}`);
  }
  return parsed;
}
function required(name) { return args[name] ?? fail(`--${name} is required`); }
function fail(message) { throw new Error(message); }
function pascal(value) { return value.split("_").map((part) => part[0].toUpperCase() + part.slice(1)).join(""); }
