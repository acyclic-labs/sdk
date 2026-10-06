import { expect, test } from "bun:test";
import { create } from "@bufbuild/protobuf";
import * as wire from "../generated/proto/objects/v2/objects_pb.js";
import { GrpcObjectsV2 } from "../src/v2-grpc.js";
import { HttpObjectsV2 } from "../src/v2-http.js";

test("Objects transports apply the Rust-owned bearer policy", async () => {
  for (const token of ["", "   ", "valid\r\nInjected: yes"]) {
    const http = new HttpObjectsV2({ endpoint: "https://objects.example", token, fetch: async () => { throw new Error("unexpected fetch"); } });
    await expect(http.createBucket(create(wire.CreateBucketRequestSchema, { name: "customer.inputs" }))).rejects.toThrow();
    expect(() => new GrpcObjectsV2({ endpoint: "https://objects.example/", token })).toThrow();
  }
});
