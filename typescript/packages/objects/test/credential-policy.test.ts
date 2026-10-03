import { expect, test } from "bun:test";
import { GrpcObjectsV2 } from "../src/v2-grpc.js";
import { HttpObjectsV2 } from "../src/v2-http.js";

test("Objects transports apply the Rust-owned bearer policy", () => {
  for (const token of ["", "   ", "valid\r\nInjected: yes"]) {
    expect(() => new HttpObjectsV2({ endpoint: "https://objects.example", token })).toThrow();
    expect(() => new GrpcObjectsV2({ endpoint: "https://objects.example/", token })).toThrow();
  }
});
