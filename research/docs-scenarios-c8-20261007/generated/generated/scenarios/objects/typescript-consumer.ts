// Generated from Rust scenario objects/typescript-consumer.
// Rust output SHA256: sha256:93ef337032290ebb852ebfb6085297dfe90a90d5d9f77046d535a4e7b18ce111
import { create } from "@bufbuild/protobuf";
import { BucketRefSchema, CreateBucketRequestSchema, GetObjectRequestSchema, MemoryObjectsV2, PutObjectHeaderSchema } from "@acyclic-labs/objects/v2";

const bucket = "customer.example";
const key = "welcome.txt";
const body = new TextEncoder().encode("hello from Rust");
const provider = await MemoryObjectsV2.create();
await provider.createBucket(create(CreateBucketRequestSchema, { name: bucket }));
const written = await provider.put(create(PutObjectHeaderSchema, { bucket: create(BucketRefSchema, { name: bucket }), objectKey: key }), body);
if (written.size !== BigInt("15")) throw new Error("Rust Objects size parity failed");
const selected = await provider.get(create(GetObjectRequestSchema, { bucket: create(BucketRefSchema, { name: bucket }), objectKey: key }), 1024n);
if (new TextDecoder().decode(selected.body) !== "hello from Rust") throw new Error("Rust Objects body parity failed");
console.log(JSON.stringify({ bucket, key, size: Number(written.size) }));
