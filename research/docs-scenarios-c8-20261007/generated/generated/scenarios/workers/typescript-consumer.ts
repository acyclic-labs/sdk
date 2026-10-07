// Generated from Rust scenario workers/typescript-consumer.
// Rust output SHA256: sha256:dea41e6c4e3ddc5148fb209ab834d1e3fbc9dfb3a14a22bec62f1a8f36675c48
import { create, toBinary } from "@bufbuild/protobuf";
import { JobLimitsSchema, JobTargetSchema, PayloadSchema, PublishVersionRequestSchema, RetryPolicySchema, SubmitJobRequestSchema } from "@acyclic-labs/workers/proto";

const module = Uint8Array.from([101, 120, 112, 111, 114, 116, 32, 100, 101, 102, 97, 117, 108, 116, 32, 97, 115, 121, 110, 99, 32, 102, 117, 110, 99, 116, 105, 111, 110, 32, 114, 117, 110, 40, 41, 32, 123, 32, 114, 101, 116, 117, 114, 110, 32, 110, 101, 119, 32, 85, 105, 110, 116, 56, 65, 114, 114, 97, 121, 40, 91, 55, 93, 41, 59, 32, 125]);
const digest = Uint8Array.from([12, 215, 81, 21, 151, 119, 208, 118, 79, 203, 86, 155, 143, 76, 37, 220, 186, 155, 30, 20, 225, 117, 115, 29, 19, 0, 19, 121, 210, 177, 122, 216]);
const publication = create(PublishVersionRequestSchema, { javascriptModule: module, expectedSha256: digest, idempotencyKey: "publish-example" });
const submission = create(SubmitJobRequestSchema, {
  target: create(JobTargetSchema, { target: { case: "deploymentAlias", value: "current" } }),
  input: create(PayloadSchema, { source: { case: "inlineBytes", value: Uint8Array.from([1, 2, 3]) } }),
  limits: create(JobLimitsSchema, { timeoutMillis: BigInt("1000"), memoryBytes: BigInt("4194304"), outputBytes: BigInt("4096") }),
  retry: create(RetryPolicySchema, { maxAttempts: 2, backoffMillis: BigInt("25") }),
  idempotencyKey: "job-example",
});
if (toBinary(PublishVersionRequestSchema, publication).length === 0 || toBinary(SubmitJobRequestSchema, submission).length === 0) throw new Error("Workers request encoded to an empty payload");
console.log(JSON.stringify({ validated: true, moduleBytes: module.length, attempts: submission.retry?.maxAttempts }));
