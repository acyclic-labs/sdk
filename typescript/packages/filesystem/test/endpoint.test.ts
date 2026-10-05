import { describe, expect, test } from "bun:test";
import { rustOwnedServiceEndpoint } from "../src/endpoint.js";
import { openHostedFs } from "../src/hosted.js";

const parse = (value: string) => rustOwnedServiceEndpoint(value, message => new RangeError(message));

describe("hosted filesystem endpoint policy", () => {
  test("accepts HTTPS and explicit loopback development endpoints", async () => {
    expect((await parse("https://filesystem.example/v2")).protocol).toBe("https:");
    for (const endpoint of ["http://localhost:8080", "http://127.0.0.1:8080", "http://[::1]:8080"]) {
      expect((await parse(endpoint)).protocol).toBe("http:");
    }
  });

  test("rejects plaintext remote, credential-bearing, ambiguous, and non-HTTP endpoints", async () => {
    for (const endpoint of [
      "http://filesystem.example",
      "ftp://filesystem.example",
      "https://user:secret@filesystem.example",
      "https://filesystem.example?token=secret",
      "https://filesystem.example#fragment",
      "/relative",
    ]) await expect(parse(endpoint)).rejects.toBeInstanceOf(RangeError);
    await expect(openHostedFs({ endpoint: "http://filesystem.example", bearerToken: "token" })).rejects.toBeInstanceOf(RangeError);
  });

  test("applies the Rust-owned bearer policy before opening a hosted client", async () => {
    for (const bearerToken of ["", "   ", "valid\r\nInjected: yes"]) {
      await expect(openHostedFs({ endpoint: "https://filesystem.example", bearerToken }))
        .rejects.toThrow("invalid bearer token");
    }
  });
});
