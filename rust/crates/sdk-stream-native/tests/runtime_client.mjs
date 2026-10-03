import assert from "node:assert/strict";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

const bindingPath = process.env.ACYCLIC_STREAM_NATIVE_MODULE;
const endpoint = process.env.ACYCLIC_STREAM_FIXTURE_ENDPOINT;
const ca = process.env.ACYCLIC_STREAM_FIXTURE_CA;
if (bindingPath === undefined || endpoint === undefined || ca === undefined) {
  throw new Error("native Stream runtime fixture environment is incomplete");
}

const binding = require(bindingPath);
const { NativeStreamClient, NativeStreamCancellation, nativeStreamCapabilities } = binding;
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

assert.equal(await client.tail("native/runtime"), "1");
const page = await client.read("native/runtime", "0", 8);
assert.equal(page.cancelled, false);
assert.equal(page.records.length, 1);
assert.equal(page.records[0].sequence, "0");
assert.equal(Buffer.from(page.records[0].value).toString(), "one");

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

const recovering = await NativeStreamClient.connect({
  endpoints: ["https://localhost:9", endpoint],
  bearerToken: "fixture-token",
  caCertificatePem: Buffer.from(ca, "base64"),
});
assert.equal(await recovering.tail("native/runtime"), "2");

console.log(JSON.stringify({
  append: appended,
  read: page.records.length,
  follow: followed.records.length,
  recoveredTail: await recovering.tail("native/runtime"),
  capabilities,
}));
