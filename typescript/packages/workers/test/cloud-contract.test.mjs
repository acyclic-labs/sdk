import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { test } from "node:test";
import { fromBinary, fromJson, toBinary, toJson } from "@bufbuild/protobuf";
import * as actors from "../../actors/generated/proto/actors/v1/actors_pb.js";
import { PutObjectHeaderSchema } from "../../objects/generated/proto/objects/v1/objects_pb.js";
import { WorkersDeploymentClient } from "../dist/deployment.js";

const golden = JSON.parse(await readFile(new URL("../../../../rust/crates/workers/tests/fixtures/cloud-workers/golden.json", import.meta.url)));

test("Named Worker deployment retains canonical request/response JSON", async () => {
  const seen = [];
  const rpc = (name, input, output) => async bytes => {
    seen.push(name);
    assert.deepEqual(toJson(input, fromBinary(input, bytes)), golden[name].request);
    return toBinary(output, fromJson(output, golden[name].response));
  };
  const client = new WorkersDeploymentClient({
    endpoint: "http://127.0.0.1:1234", token: "fixture-token",
    binding: { async connect(endpoint, token) {
      assert.equal(endpoint, "http://127.0.0.1:1234");
      assert.equal(token, "fixture-token");
      return {
        deployWorker: rpc("deploy", actors.DeploymentRequestSchema, actors.DeploymentResponseSchema),
        invokeWorker: rpc("invoke", actors.InvokeWorkerRequestSchema, actors.InvokeActorResponseSchema),
        deleteWorker: rpc("delete", actors.DeleteWorkerRequestSchema, actors.DeleteWorkerResponseSchema),
      };
    } },
  }, {
    async put(header, bytes) {
      seen.push("publish");
      assert.deepEqual(toJson(PutObjectHeaderSchema, header), golden.publish.request);
      assert.equal(new TextDecoder().decode(bytes), golden.code);
    },
  });
  const digest = await client.publish(new TextEncoder().encode(golden.code), "publish-code");
  assert.equal(Buffer.from(digest).toString("hex"), golden.publish.sha256);
  const created = await client.deploy(fromJson(actors.DeploymentRequestSchema, golden.deploy.request));
  assert.equal(created.deployment.name, golden.deploy.response.deployment.name);
  assert.equal(created.deployment.revision, 1n);
  const invoked = await client.invoke(fromJson(actors.InvokeWorkerRequestSchema, golden.invoke.request));
  assert.equal(invoked.status, 201);
  assert.equal(new TextDecoder().decode(Uint8Array.from(invoked.body)), "output");
  assert.deepEqual(invoked.headers, golden.invoke.response.headers);
  await client.delete(fromJson(actors.DeleteWorkerRequestSchema, golden.delete.request));
  assert.deepEqual(seen, ["publish", "deploy", "invoke", "delete"]);
});

test("failed publication does not return a digest or enter the lifecycle", async () => {
  const failure = new Error("publication rejected");
  const client = new WorkersDeploymentClient({
    endpoint: "http://127.0.0.1:1234", token: "fixture-token",
    binding: { connect: () => assert.fail("publication connected to Actors") },
  }, { put: async () => { throw failure; } });
  await assert.rejects(client.publish(new Uint8Array([1]), "publish"), error => error === failure);
});

test("fixtures distinguish proposed SDK deployment from historical Cloud code", () => {
  assert.match(golden.source.normalization, /not the live Cloud API/);
  assert.match(golden.source.commit, /^[a-f0-9]{40}$/);
  assert.ok(golden.source.files.some(file => file.includes("deleted_worker_exits")));
  assert.ok(golden.source.files.some(file => file.includes("edge-workers")));
  assert.ok(!golden.code.includes("export const actor"));
});
