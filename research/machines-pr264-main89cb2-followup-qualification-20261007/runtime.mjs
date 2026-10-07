import assert from "node:assert/strict";
import { Machines, HttpMachinesProvider, machineId, idempotencyKey, checkpointId, operationId, managedOci } from "@acyclic-labs/machines";
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
for (const [fn, value] of [[machineId, "machine-1"], [idempotencyKey, "request-1"], [checkpointId, "checkpoint-1"], [operationId, "operation-1"]]) { const first = fn(value); assert.match(first, uuid); assert.equal(fn(value), first); }
assert.throws(() => machineId(""));
const image = managedOci("registry.example/image@sha256:" + "0a".repeat(32)); assert.equal(image.kind, "managed-oci"); assert.equal(image.digestHex.length, 64); assert.throws(() => managedOci("registry.example/image:latest"));
const facade = Machines.fromEnv({ endpoint: "https://example.test", token: "token" }); assert.equal(facade.provider.assurance, "managed-service");
const provider = new HttpMachinesProvider({ endpoint: "https://example.test/api", token: "token" }); assert.equal(provider.assurance, "managed-service");
assert.throws(() => new HttpMachinesProvider({ endpoint: "http://example.test", token: "token" }), TypeError); assert.throws(() => new HttpMachinesProvider({ endpoint: "https://example.test", token: "" }), TypeError); assert.throws(() => new HttpMachinesProvider({ endpoint: "https://example.test", token: "token", maximumResponseBytes: 0 }), RangeError);
console.log("runtime-smoke-ok");
