import assert from "./browser-assert.mjs";
import { create } from "@bufbuild/protobuf";
import { lifecycle } from "../../objects/test/v2-lifecycle.mjs";
import { HttpObjectsV2 } from "@acyclic-labs/objects/v2/http";
import * as objectsWire from "@acyclic-labs/objects/v2";
import { ActorsService, ActorsTransportError, HttpActorsClient } from "@acyclic-labs/actors";
import { WorkersService, WorkersTransportError, HttpWorkersClient } from "@acyclic-labs/workers";
import { HttpStreamProvider, idempotencyKey } from "@acyclic-labs/stream";

try {
  const options = { endpoint: location.origin, token: "conformance" };
  const objects = new HttpObjectsV2(options);
  await lifecycle(objects);
  await assert.rejects(new HttpObjectsV2({ ...options, token: "wrong" }).headBucket(create(objectsWire.HeadBucketRequestSchema, { bucket: { name: "customer.inputs" } })), error => error.code === objectsWire.ErrorCode.ACCESS_DENIED);
  await assert.rejects(new HttpObjectsV2({ ...options, maximumResponseBytes: 8 }).createBucket(create(objectsWire.CreateBucketRequestSchema, { name: "bounded-response" })), error => error.code === objectsWire.ErrorCode.QUOTA_EXCEEDED);
  let methods = 0;
  for (const [service, client] of [[ActorsService, new HttpActorsClient(options)], [WorkersService, new HttpWorkersClient(options)]]) {
    for (const method of service.methods) {
      const init = {};
      if (method.name === "AddSubscription") Object.assign(init, { actorId: "browser-actor", subscription: { subscriptionId: "input", streamPath: "events/input", start: { start: { case: "cursor", value: 9007199254740993n } } }, idempotencyKey: "subscribe-browser" });
      if (method.name === "CheckpointActor") Object.assign(init, { actorId: "browser-actor", idempotencyKey: "checkpoint-browser" });
      if (method.name === "SelectDeployment") init.expectedRevision = 7n;
      if (method.name === "InvokeVersion") init.versionSha256 = new Uint8Array(32).fill(1);
      if (method.name === "InvokeDeployment") init.alias = "current";
      const result = await client[method.localName](create(method.input, init));
      assert.equal(result.$typeName, method.output.typeName);
      if (method.name === "CheckpointActor") assert.equal(result.actor.checkpointEpoch, 9n);
      if (method.name === "InvokeActor") assert.equal(result.status, 201);
      if (method.name === "InvokeVersion") { assert.deepEqual(result.resolvedSha256, new Uint8Array(32).fill(1)); assert.equal(result.resolvedRevision, undefined); }
      if (method.name === "InvokeDeployment") { assert.deepEqual(result.resolvedSha256, new Uint8Array(32).fill(2)); assert.equal(result.resolvedRevision, 8n); }
      methods++;
    }
  }
  assert.equal(methods, 15);
  const inspectActor = ActorsService.methods.find(method => method.name === "InspectActor");
  await assert.rejects(new HttpActorsClient({ ...options, token: "wrong" }).inspectActor(create(inspectActor.input, { actorId: "browser-actor" })), error => error.status === 403 && typeof error.code === "number");
  await assert.rejects(new HttpActorsClient({ ...options, maximumResponseBytes: 8 }).inspectActor(create(inspectActor.input, { actorId: "oversize" })), error => error instanceof ActorsTransportError && error.message === "response exceeds configured bound");
  const inspectJob = WorkersService.methods.find(method => method.name === "InspectJob");
  await assert.rejects(new HttpWorkersClient({ ...options, token: "wrong" }).inspectJob(create(inspectJob.input, { jobId: "job" })), error => error.status === 403 && typeof error.code === "number");
  await assert.rejects(new HttpWorkersClient({ ...options, maximumResponseBytes: 8 }).inspectJob(create(inspectJob.input, { jobId: "oversize" })), error => error instanceof WorkersTransportError && error.message === "response exceeds configured bound");

  const stream = new HttpStreamProvider(options);
  const key = text => idempotencyKey(new TextEncoder().encode(text));
  const body = text => new TextEncoder().encode(text);
  const path = "browser/events";
  await assert.rejects(stream.tail(path), error => error.code === "stream_not_found");
  assert.equal(await stream.inspectIdempotency(key("browser-missing")), undefined);
  const appended = await stream.append(path, [body("one"), body("two")], { ifTail: 0n, idempotencyKey: key("browser-append") });
  assert.equal(appended.ok, true); assert.equal(appended.end, 2n);
  assert.deepEqual(await stream.append(path, [body("one"), body("two")], { ifTail: 0n, idempotencyKey: key("browser-append") }), appended);
  await assert.rejects(stream.append(path, [body("changed")], { ifTail: 0n, idempotencyKey: key("browser-append") }), error => error.code === "idempotency_mismatch");
  const records = []; for await (const item of stream.read(path, { from: 0n, limit: 2 })) records.push(item);
  assert.deepEqual(records.map(item => new TextDecoder().decode(item.value)), ["one", "two"]);
  const controller = new AbortController(); let followed = 0;
  for await (const item of stream.follow(path, { from: 0n, signal: controller.signal })) { assert.equal(item.sequence, BigInt(followed++)); if (followed === 2) controller.abort(); }
  assert.equal(followed, 2);
  assert.equal((await stream.fork(path, "browser/fork", { atTail: 1n, idempotencyKey: key("browser-fork") })).tail, 1n);
  assert.deepEqual((await stream.childrenPage({ parent: "browser", limit: 8 })).children.map(item => item.path), ["browser/events", "browser/fork"]);
  const children = []; for await (const item of stream.children("browser", 8)) children.push(item.path);
  assert.deepEqual(children, ["browser/events", "browser/fork"]);
  const commitRequest = { conditions: [{ path, ifTail: 2n }, { path: "browser/other", ifAbsent: true }], mutations: [{ append: { path, values: [body("three")] } }, { append: { path: "browser/other", values: [body("other")] } }] };
  const committed = await stream.commit(commitRequest, { idempotencyKey: key("browser-commit") });
  assert.equal(committed.ok, true); assert.equal(committed.tails[path], 3n);
  assert.deepEqual(await stream.commit(commitRequest, { idempotencyKey: key("browser-commit") }), committed);
  assert.equal((await stream.readCommit(committed.commitId)).mutations.length, 2);
  assert.equal((await stream.inspectIdempotency(key("browser-commit"))).outcome.type, "commit");
  const conflict = await stream.commit({ conditions: [{ path, ifTail: 0n }], mutations: [{ append: { path, values: [body("rejected")] } }] }, { idempotencyKey: key("browser-conflict") });
  assert.equal(conflict.ok, false); assert.equal(await stream.tail(path), 3n);
  await assert.rejects(new HttpStreamProvider({ ...options, token: "wrong" }).tail(path), error => error.code === "access_denied");
  await assert.rejects(new HttpStreamProvider({ ...options, maximumResponseBytes: 8 }).readCommit(committed.commitId), error => error.code === "response_too_large");
  assert.equal((await stream.createToken({ expiresIn: "1m", allow: [{ path: "browser", subtree: true, operations: ["read"] }] })).token, "fixture-issued-token");
  globalThis.sdkHttpResult = { status: "passed", detail: "Objects lifecycle/framing, 15 Actor/Worker operations, Stream follow/atomic Commit/replay, auth/errors/bounds" };
} catch (error) {
  globalThis.sdkHttpResult = { status: "failed", detail: error.stack ?? String(error) };
}
