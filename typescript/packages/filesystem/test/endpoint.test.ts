import { describe, expect, test } from "bun:test";
import { secureServiceEndpoint } from "../src/endpoint.js";
import { openHostedFs } from "../src/hosted.js";

const parse = (value: string) => secureServiceEndpoint(value, message => new RangeError(message));

describe("hosted filesystem endpoint policy", () => {
  test("accepts HTTPS and explicit loopback development endpoints", () => {
    expect(parse("https://filesystem.example/v2").protocol).toBe("https:");
    for (const endpoint of ["http://localhost:8080", "http://127.0.0.1:8080", "http://[::1]:8080"]) {
      expect(parse(endpoint).protocol).toBe("http:");
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
    ]) expect(() => parse(endpoint)).toThrow(RangeError);
    await expect(openHostedFs({ endpoint: "http://filesystem.example", bearerToken: "token" })).rejects.toBeInstanceOf(RangeError);
  });
});
