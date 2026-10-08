import assert from "node:assert/strict";
import { create } from "@bufbuild/protobuf";
import { MemoryObjectsV1, CreateBucketRequestSchema, PutObjectHeaderSchema, GetObjectRequestSchema } from "@acyclic-labs/objects/v1";
import { HttpObjectsV1 } from "@acyclic-labs/objects/v1/http";
import { createObjectsV1GrpcClients } from "@acyclic-labs/objects/v1/grpc";
import { ObjectContentStore } from "@acyclic-labs/harness/objects";

assert.equal(typeof HttpObjectsV1, "function");
assert.equal(typeof createObjectsV1GrpcClients, "function");
assert.equal(typeof ObjectContentStore.create, "function");
const objects = await MemoryObjectsV1.create();
const bucket = await objects.createBucket(create(CreateBucketRequestSchema, { name: "package-smoke" }));
const body = new TextEncoder().encode("logical-content");
await objects.put(create(PutObjectHeaderSchema, { bucket: bucket.bucket, objectKey: "one", mutation: { idempotencyKey: "package-smoke-put" } }), body);
const selected = await objects.get(create(GetObjectRequestSchema, { bucket: bucket.bucket, objectKey: "one" }), BigInt(body.byteLength));
assert.deepEqual(selected.body, body);
console.log("Objects v1 and Harness package entry points: PASS");
