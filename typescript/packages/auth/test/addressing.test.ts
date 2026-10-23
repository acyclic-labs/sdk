import { expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import { ResourceIdentity, canonicalResourceHost } from "../src/addressing.js";

const vectors = JSON.parse(await readFile(new URL("../../../../rust/crates/auth/tests/fixtures/resource-identity-v1.json", import.meta.url), "utf8"));
const hex = (bytes: Uint8Array) => Buffer.from(bytes).toString("hex");
const unhex = (text: string) => Uint8Array.from(Buffer.from(text, "hex"));

test("shared golden pairs use the literal Rust WASM encoding", () => {
  for (const row of vectors.pairs) {
    const identity = new ResourceIdentity(unhex(row.account_hex), unhex(row.id_hex));
    expect(hex(identity.encode())).toBe(row.encoded_hex);
    const decoded = ResourceIdentity.decode(unhex(row.encoded_hex));
    expect(hex(decoded.account)).toBe(row.account_hex);
    expect(hex(decoded.id)).toBe(row.id_hex);
  }
  for (const row of vectors.invalid) {
    try {
      new ResourceIdentity(unhex(row.account_hex), unhex(row.id_hex));
      throw new Error("invalid vector was accepted");
    } catch (error) {
      expect((error as { name: string }).name).toBe("IdentityError");
      expect((error as { code: string }).code).toBe(row.error);
    }
  }
});

test("shared host vectors use account id directly for URL, SNI and signed Host", () => {
  for (const row of vectors.hosts) {
    const identity = new ResourceIdentity(unhex(row.account_hex), unhex(row.id_hex));
    if (row.hostname !== undefined) {
      const host = identity.hostname(row.domain);
      expect(host).toBe(row.hostname);
      expect(canonicalResourceHost(host)).toBe(host);
      expect(new URL(`https://${host}/`).host).toBe(host);
    } else {
      try {
        identity.hostname(row.domain);
        throw new Error("invalid DNS vector was accepted");
      } catch (error) {
        expect((error as { name: string }).name).toBe("DnsError");
        expect((error as { code: string }).code).toBe(row.error);
      }
    }
  }
});

test("pair values retain bytes and reject alternate encodings", () => {
  const account = unhex("ff00"), id = unhex("80fe");
  const identity = new ResourceIdentity(account, id);
  account.fill(0); id.fill(0);
  identity.account.fill(0); identity.id.fill(0); identity.encode().fill(0);
  expect(hex(identity.account)).toBe("ff00");
  expect(hex(identity.id)).toBe("80fe");
  const encoded = identity.encode();
  expect(() => ResourceIdentity.decode(encoded.subarray(1))).toThrow();
  expect(() => ResourceIdentity.decode(new Uint8Array([...encoded, 0]))).toThrow();
});

test("host aliases cannot change signing or TLS spelling", () => {
  for (const host of ["Worker.account.example.com", "worker.account.example.com.",
    "worker.account.example.com:443", "x@worker.account.example.com", "127.1", "0x7f000001"]) {
    expect(() => canonicalResourceHost(host)).toThrow();
  }
});
