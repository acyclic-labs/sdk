import assert from "node:assert/strict";
import { ActorsClient } from "@acyclic-labs/actors";

const digest = new Uint8Array(32).fill(1);
const limits = { handlerTimeoutMillis: 1n, memoryBytes: 2n, checkpointBytes: 3n };
const client = new ActorsClient({ endpoint: "https://actors.example.test", token: "fixture" });
assert.equal(await client.transport, "fixture-native");

await client.createActor({ codeSha256: digest, homeRegion: "eu", bindings: [], limits, subscriptions: [], idempotencyKey: "create" });
await client.updateActor({ actorId: "actor-a", codeSha256: digest, bindings: [], limits, expectedConfigurationRevision: 0n, idempotencyKey: "update" });
await client.inspectActor({ actorId: "actor-a" });
await client.addSubscription({ actorId: "actor-a", subscription: { subscriptionId: "sub", streamPath: "events", start: { start: { case: "currentHead", value: true } }, placementAnchor: false }, idempotencyKey: "add" });
await client.removeSubscription({ actorId: "actor-a", subscriptionId: "sub", idempotencyKey: "remove" });
await client.resumeSubscription({ actorId: "actor-a", subscriptionId: "sub", idempotencyKey: "resume" });
await client.checkpointActor({ actorId: "actor-a", idempotencyKey: "checkpoint" });
await client.invokeActor({ actorId: "actor-a", method: "POST", url: "/invoke", body: new Uint8Array([4, 5]), headers: [{ name: "content-type", value: "application/json" }] });
console.log("installed native companion/default loader passed for 8 operations");
