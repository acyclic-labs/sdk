import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { createRequire } from "node:module";
import { mkdirSync, writeFileSync } from "node:fs";

const require = createRequire(import.meta.url);

const bindingPath = process.env.ACYCLIC_STREAM_NATIVE_MODULE;
const endpoint = process.env.ACYCLIC_STREAM_FIXTURE_ENDPOINT;
const ca = process.env.ACYCLIC_STREAM_FIXTURE_CA;
if (bindingPath === undefined || endpoint === undefined || ca === undefined) {
  throw new Error("native Stream runtime fixture environment is incomplete");
}

const binding = require(bindingPath);
const { NativeStreamClient, NativeStreamCancellation, nativeStreamCapabilities } = binding;

function lengthDelimited(field, value) {
  assert(value.length < 128, "runtime fixture protobuf helper only handles short fields");
  return Buffer.concat([Buffer.from([(field << 3) | 2, value.length]), value]);
}

function firstLengthDelimited(value, wantedField) {
  let offset = 0;
  while (offset < value.length) {
    const tag = value[offset++];
    const field = tag >> 3;
    assert.equal(tag & 7, 2, "runtime fixture expects length-delimited protobuf fields");
    const length = value[offset++];
    const end = offset + length;
    assert(end <= value.length, "runtime fixture protobuf field exceeds response");
    if (field === wantedField) return value.subarray(offset, end);
    offset = end;
  }
  throw new Error(`missing protobuf field ${wantedField}`);
}

const sourceRevision = process.env.ACYCLIC_STREAM_SOURCE_REVISION;
const scenarioDirectory = process.env.ACYCLIC_STREAM_SCENARIO_DIR;
const scenarioRecords = [];
function recordScenario(rpc, shape, checks) {
  if (scenarioDirectory === undefined) return;
  if (sourceRevision === undefined || !/^[0-9a-f]{40}$/i.test(sourceRevision)) {
    throw new Error("scenario output requires ACYCLIC_STREAM_SOURCE_REVISION");
  }
  const result = {
    schema: "acyclic.sdk.rpc-scenario-result.v1",
    source_revision: sourceRevision,
    family: "stream",
    rpc,
    shape,
    transport: "grpc",
    status: "passed",
    invoked: true,
    exit_code: 0,
    checks,
  };
  const file = `${rpc.slice(rpc.lastIndexOf("/") + 1).toLowerCase()}.json`;
  mkdirSync(scenarioDirectory, { recursive: true });
  const path = `${scenarioDirectory}/${file}`;
  const bytes = Buffer.from(`${JSON.stringify(result, null, 2)}\n`);
  writeFileSync(path, bytes);
  scenarioRecords.push({
    family: "stream",
    rpc,
    shape,
    status: "passed",
    output_path: process.env.ACYCLIC_STREAM_SCENARIO_PREFIX === undefined
      ? file
      : `${process.env.ACYCLIC_STREAM_SCENARIO_PREFIX}/${file}`,
    output_sha256: `sha256:${createHash("sha256").update(bytes).digest("hex")}`,
  });
}

const capabilities = nativeStreamCapabilities();
assert.equal(capabilities.maxEndpoints, 16);
assert.equal(capabilities.operationDeadlineMs, 10_000);
assert.equal(capabilities.endpointAttemptTimeoutMs, 1_000);
assert.equal(capabilities.followAttemptTimeoutMs, 500);
assert.equal(capabilities.retryDelayMs, 10);

const options = {
  endpoints: [endpoint],
  bearerToken: "fixture-token",
  caCertificatePem: Buffer.from(ca, "base64"),
};
const client = await NativeStreamClient.connect(options);
const appended = await client.append(
  "native/runtime",
  [Buffer.from("one")],
  null,
  Buffer.from("native-runtime-append"),
);
assert.equal(appended.committed, true);
assert.equal(appended.start, "0");
assert.equal(appended.end, "1");
assert.equal(appended.tail, "1");
recordScenario("acyclic.stream.v2.StreamService/Append", "unary", ["invocation", "transport", "serialization"]);

assert.equal(await client.tail("native/runtime"), "1");
const page = await client.read("native/runtime", "0", 8);
assert.equal(page.cancelled, false);
assert.equal(page.records.length, 1);
assert.equal(page.records[0].sequence, "0");
assert.equal(Buffer.from(page.records[0].value).toString(), "one");
recordScenario("acyclic.stream.v2.StreamService/Read", "server", ["invocation", "transport", "serialization"]);

const idempotencyKey = Buffer.from("native-runtime-append");
const idempotency = await client.inspectIdempotency(lengthDelimited(1, idempotencyKey));
assert(idempotency.length > 0);
recordScenario("acyclic.stream.v2.StreamService/InspectIdempotency", "unary", ["invocation", "transport", "serialization"]);

const commitRequest = Buffer.from(process.env.ACYCLIC_STREAM_FIXTURE_COMMIT_REQUEST, "base64");
const commitResponse = await client.commit(
  commitRequest,
);
const committedEnvelope = firstLengthDelimited(commitResponse, 1);
const commitId = firstLengthDelimited(committedEnvelope, 1);
assert.equal(commitId.length, 32);
const readCommit = await client.readCommit(lengthDelimited(1, commitId));
assert.equal(firstLengthDelimited(readCommit, 1).length, 32);
recordScenario("acyclic.stream.v2.StreamService/Commit", "unary", ["invocation", "transport", "serialization"]);
recordScenario("acyclic.stream.v2.StreamService/ReadCommit", "unary", ["invocation", "transport", "serialization"]);

const childrenResponse = await client.childrenPage(Buffer.from([0x0a, 0x06, 0x6e, 0x61, 0x74, 0x69, 0x76, 0x65, 0x20, 0x08]));
assert(childrenResponse.length > 0);
recordScenario("acyclic.stream.v2.StreamService/ChildrenPage", "unary", ["invocation", "transport", "serialization"]);

const forkAt = await client.tail("native/runtime");
const forked = await client.fork(
  "native/runtime",
  "native/fork",
  forkAt,
  Buffer.from("native-runtime-fork"),
);
assert.equal(forked.destination, "native/fork");
assert.equal(forked.forkedAt, forkAt);
recordScenario("acyclic.stream.v2.StreamService/Fork", "unary", ["invocation", "transport", "serialization"]);

const children = await client.children("native", 16);
assert(children.some((child) => child.path === "native/fork"));
recordScenario("acyclic.stream.v2.StreamService/Children", "server", ["invocation", "transport", "serialization"]);

const cancellation = new NativeStreamCancellation();
const follow = client.follow("native/runtime", "1", cancellation);
const appendFollowed = client.append(
  "native/runtime",
  [Buffer.from("two")],
  null,
  Buffer.from("native-runtime-follow"),
);
const followedAppend = await appendFollowed;
assert.equal(followedAppend.committed, true);
setTimeout(() => cancellation.cancel(), 100);
const followed = await follow;
assert.equal(followed.cancelled, true);
assert.equal(followed.records.length, 1);
assert.equal(Buffer.from(followed.records[0].value).toString(), "two");
recordScenario("acyclic.stream.v2.StreamService/Follow", "server", ["invocation", "transport", "serialization", "cancellation"]);

const recovering = await NativeStreamClient.connect({
  endpoints: ["https://localhost:9", endpoint],
  bearerToken: "fixture-token",
  caCertificatePem: Buffer.from(ca, "base64"),
});
assert.equal(await recovering.tail("native/runtime"), "2");
recordScenario("acyclic.stream.v2.StreamService/Tail", "unary", ["invocation", "transport", "serialization", "recovery"]);

if (process.env.ACYCLIC_STREAM_CONSUMER_OUTPUT !== undefined) {
  if (sourceRevision === undefined || !/^[0-9a-f]{40}$/i.test(sourceRevision)) {
    throw new Error("consumer output requires ACYCLIC_STREAM_SOURCE_REVISION");
  }
  const consumer = {
    schema: "acyclic.sdk.stream.native-installed-consumer.v1",
    source_revision: sourceRevision,
    status: "passed",
    invoked: true,
    exit_code: 0,
    package: process.env.ACYCLIC_STREAM_NATIVE_MODULE,
    resolution: "node_modules package name",
    scenarios: scenarioRecords,
    checks: ["package-install", "napi-loader", "tls", "serialization", "cancellation", "recovery"],
  };
  const bytes = `${JSON.stringify(consumer, null, 2)}\n`;
  writeFileSync(process.env.ACYCLIC_STREAM_CONSUMER_OUTPUT, bytes);
}

console.log(JSON.stringify({
  append: appended,
  commitId: commitId.toString("hex"),
  idempotencyBytes: idempotency.length,
  childrenBytes: childrenResponse.length,
  read: page.records.length,
  follow: followed.records.length,
  recoveredTail: await recovering.tail("native/runtime"),
  capabilities,
}));
