import assert from "node:assert/strict";
import { create } from "@bufbuild/protobuf";
import { MemoryObjectsV2, CreateBucketRequestSchema, PutObjectHeaderSchema, GetObjectRequestSchema } from "@acyclic-labs/objects/v2";
import { HttpObjectsV2 } from "@acyclic-labs/objects/v2/http";
import { createObjectsV2GrpcClients } from "@acyclic-labs/objects/v2/grpc";
import { ObjectContentStore } from "@acyclic-labs/harness/objects";

assert.equal(typeof HttpObjectsV2, "function");
assert.equal(typeof createObjectsV2GrpcClients, "function");
assert.equal(typeof ObjectContentStore.create, "function");
const objects = await MemoryObjectsV2.create();
const bucket = await objects.createBucket(create(CreateBucketRequestSchema, { name: "package-smoke" }));
const body = new TextEncoder().encode("logical-content");
await objects.put(create(PutObjectHeaderSchema, { bucket: bucket.bucket, objectKey: "one", mutation: { idempotencyKey: "package-smoke-put" } }), body);
const selected = await objects.get(create(GetObjectRequestSchema, { bucket: bucket.bucket, objectKey: "one" }), BigInt(body.byteLength));
assert.deepEqual(selected.body, body);
console.log("Objects v2 and Harness package entry points: PASS");
