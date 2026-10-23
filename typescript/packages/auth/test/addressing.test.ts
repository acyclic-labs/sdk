import { expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import { blake3 } from "@noble/hashes/blake3.js";
import { resourceAddress, resourceAddressPreimage, canonicalResourceHost, type ResourceKind } from "../src/addressing.js";

const vectors = JSON.parse(await readFile(new URL("../../../../rust/crates/auth/tests/fixtures/resource-address-v1.json", import.meta.url), "utf8"));
const hex = (value: Uint8Array) => Buffer.from(value).toString("hex");
const unhex = (value: string) => Uint8Array.from(Buffer.from(value, "hex"));

test("shared golden address vectors execute through the Rust WASM codec", () => {
  expect(vectors.hash).toBe("blake3-256");
  for (const vector of vectors.addresses) {
    const account = unhex(vector.account_hex), name = unhex(vector.name_hex);
    expect(hex(resourceAddressPreimage(account, vector.kind, name))).toBe(vector.preimage_hex);
    expect(hex(resourceAddress(account, vector.kind, name))).toBe(vector.address_hex);
    expect(hex(blake3(unhex(vector.preimage_hex)))).toBe(vector.address_hex);
  }
});

test("JavaScript numbers cannot wrap into another kind", () => {
  for (const kind of [0, 7, -1, 1.5, NaN, Infinity, 2 ** 32 + 1]) {
    expect(() => resourceAddress(new Uint8Array(), kind as ResourceKind, new Uint8Array())).toThrow();
  }
});

test("host has a single TLS/SNI and signing canonical form", () => {
  const host = "worker.account.example.com";
  expect(canonicalResourceHost(host)).toBe(host);
  expect(new URL(`https://${canonicalResourceHost(host)}/`).host).toBe(host);
  for (const alias of [host.toUpperCase(), `${host}.`, `${host}:443`, `x@${host}`, `*.${host}`, `${host}/`, "127.0.0.1", "0x7f000001"]) {
    expect(() => canonicalResourceHost(alias)).toThrow();
  }
});
