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

  test("rejects header-unsafe or oversized bearer tokens and never follows redirects", async () => {
    for (const bearerToken of ["", " ", "a\nb", "a\rb", "a\0b", "x".repeat(12 * 1024 + 1)]) {
      await expect(openHostedFs({ endpoint: "https://filesystem.example", bearerToken })).rejects.toBeInstanceOf(RangeError);
    }
    let redirect: RequestRedirect | undefined;
    const fetch = (async (_input: RequestInfo | URL, init?: RequestInit) => {
      redirect = init?.redirect;
      throw new TypeError("offline");
    }) as typeof globalThis.fetch;
    await expect(openHostedFs({ endpoint: "https://filesystem.example", bearerToken: "token", fetch })).rejects.toThrow();
    expect(redirect).toBe("error");
  });
});
