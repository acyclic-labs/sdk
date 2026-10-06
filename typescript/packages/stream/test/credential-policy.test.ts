import { expect, test } from "bun:test";
import { GrpcStreamProvider } from "../src/grpc.js";
import { HttpStreamProvider } from "../src/http.js";

test("Stream transports apply the Rust-owned bearer policy", () => {
  for (const token of ["", "   ", "valid\r\nInjected: yes"]) {
    expect(() => new HttpStreamProvider({ endpoint: "https://stream.example", token })).toThrow();
    expect(() => new GrpcStreamProvider({ endpoint: "https://stream.example/", token })).toThrow();
  }
});
