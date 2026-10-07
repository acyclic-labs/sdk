import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createClient, Code } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { ActorsService } from "../typescript/packages/actors/generated/proto/actors/v1/actors_pb.js";

const [optionsPath] = process.argv.slice(2);
if (!optionsPath || process.argv.length !== 3) throw new Error("usage: node scripts/check-pending-actors-fixture.mjs OPTIONS_JSON");
const options = JSON.parse(await readFile(optionsPath, "utf8"));
const state = async () => {
  const response = await fetch(`${options.controlEndpoint}/state`);
  assert.equal(response.status, 200);
  return response.json();
};
const waitFor = async predicate => {
  for (let attempt = 0; attempt < 100; attempt++) {
    const current = await state();
    if (predicate(current)) return current;
    await new Promise(resolve => setTimeout(resolve, 20));
  }
  throw new Error("fixture state did not reach the expected transition");
};
const before = await state();
assert.equal(before.active, 0, "qualification requires exclusive use of this fixture");
const client = createClient(ActorsService, createGrpcTransport({
  baseUrl: options.endpoint,
  nodeOptions: { ca: options.caCertificate },
  idleConnectionTimeoutMs: 100,
}));
const controller = new AbortController();
const result = client.inspectActor({ actorId: "pending-fixture-control" }, {
  headers: { authorization: `Bearer ${options.token}` },
  signal: controller.signal,
}).then(() => { throw new Error("pending call completed before cancellation"); }, error => {
  assert.equal(error.code, Code.Canceled);
});
try {
  await waitFor(current => current.started === before.started + 1 && current.active === 1);
} finally {
  controller.abort();
}
await result;
await waitFor(current => current.aborted === before.aborted + 1 && current.active === 0);
console.log("PORTABLE_PENDING_FIXTURE_CANCELLATION_PASS");
