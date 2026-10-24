import { describe, expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import type * as Binding from "@acyclic-labs/auth/addressing";

export async function resourceIdentityConformance(binding: typeof Binding, name: string): Promise<void> {
  const { ActorReference, ResourceIdentity, canonicalResourceHost, WorkerRoutePattern, selectWorkerRoute } = binding;
  const vectors = JSON.parse(await readFile(new URL("../../rust/crates/auth/tests/fixtures/resource-identity-v1.json", import.meta.url), "utf8"));
  describe(name, () => {
    const hex = (bytes: Uint8Array) => Buffer.from(bytes).toString("hex");
    const unhex = (text: string) => Uint8Array.from(Buffer.from(text, "hex"));

    test("Worker route grammar, query matching, specificity and exclusions share vectors", () => {
      for (const row of vectors.routes) {
        const route = new WorkerRoutePattern(row.pattern);
        expect(route.toString()).toBe(row.canonical);
        expect(route.matches(row.url)).toBe(row.matches);
      }
      for (const row of vectors.invalid_routes) expect(() => new WorkerRoutePattern(row)).toThrow();
      for (const row of vectors.route_selection) {
        const routes = row.patterns.map((pattern: string, index: number) => ({ pattern: new WorkerRoutePattern(pattern), target: row.targets[index] ?? undefined }));
        expect(selectWorkerRoute(routes, row.url)).toBe(row.target);
        expect(selectWorkerRoute(routes.reverse(), row.url)).toBe(row.target);
      }
      for (const target of ["", "Upper", "bad_name", "-leading"]) {
        expect(() => selectWorkerRoute([{pattern: new WorkerRoutePattern("example.com/*"), target}], "https://example.com/")).toThrow();
      }
    });

    test("shared actor triples are disjoint and preserve the legacy default", () => {
      for (const row of vectors.actors) {
        const value = new ActorReference(unhex(row.account_hex), unhex(row.namespace_hex), unhex(row.id_hex));
        expect(hex(value.encode())).toBe(row.encoded_hex);
        expect(ActorReference.decode(unhex(row.encoded_hex)).equals(value)).toBe(true);
        expect(hex(value.toFields().namespace)).toBe(row.namespace_hex);
        expect(() => ResourceIdentity.decode(value.encode())).toThrow();
        if (row.legacy) {
          expect(ActorReference.fromLegacyFields(unhex(row.account_hex), undefined, new TextDecoder().decode(unhex(row.id_hex))).equals(value)).toBe(true);
        }
        value.account.fill(0); value.namespace.fill(0); value.id.fill(0); value.encode().fill(0);
        expect(hex(value.encode())).toBe(row.encoded_hex);
        expect(() => ActorReference.decode(new Uint8Array([...value.encode(), 0]))).toThrow();
      }
      for (const row of vectors.invalid_actors) {
        try {
          new ActorReference(unhex(row.account_hex), unhex(row.namespace_hex), unhex(row.id_hex));
          throw new Error("invalid actor accepted");
        } catch (error) { expect((error as { code: string }).code).toBe(row.error); }
      }
      for (const row of vectors.pairs) expect(() => ActorReference.decode(unhex(row.encoded_hex))).toThrow();
    });

    test("Host parsing uses an exact multi-label suffix and exactly two labels", () => {
      for (const row of vectors.host_parsing) {
        if (row.valid) {
          const value = ResourceIdentity.fromHostname(row.host, row.domain);
          expect(value.hostname(row.domain)).toBe(row.host);
        } else expect(() => ResourceIdentity.fromHostname(row.host, row.domain)).toThrow();
      }
    });

    test("shared golden pairs use the literal Rust WASM encoding", () => {
      for (const row of vectors.pairs) {
        const identity = new ResourceIdentity(unhex(row.account_hex), unhex(row.id_hex));
        expect(hex(identity.encode())).toBe(row.encoded_hex);
        const decoded = ResourceIdentity.decode(unhex(row.encoded_hex));
        expect(hex(decoded.account)).toBe(row.account_hex);
        expect(hex(decoded.id)).toBe(row.id_hex);
        const fields = decoded.toFields();
        expect(hex(fields.account)).toBe(row.account_hex);
        expect(hex(fields.id)).toBe(row.id_hex);
        expect(ResourceIdentity.fromFields(fields).equals(identity)).toBe(true);
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
      const fields = identity.toFields();
      fields.account.fill(0); fields.id.fill(0);
      expect(hex(identity.account)).toBe("ff00");
      expect(hex(identity.id)).toBe("80fe");
      expect(identity.equals(ResourceIdentity.decode(encoded))).toBe(true);
      expect(identity.equals(new ResourceIdentity(unhex("ff00"), unhex("80ff")))).toBe(false);
      expect(identity.equals(new ResourceIdentity(unhex("ff01"), unhex("80fe")))).toBe(false);
      expect(identity.equals(new ResourceIdentity(unhex("ff0080"), unhex("fe")))).toBe(false);
      expect(() => ResourceIdentity.decode(encoded.subarray(1))).toThrow();
      expect(() => ResourceIdentity.decode(new Uint8Array([...encoded, 0]))).toThrow();
    });

    test("wrapping existing RPC fields leaves their representation unchanged", () => {
      const encoder = new TextEncoder();
      const decoder = new TextDecoder("utf-8", { fatal: true });
      for (const id of ["bucket", "parent/stream", "actor:1", "worker", "cafe\u0301", "caf\u00e9"]) {
        const fields = ResourceIdentity.fromFields({ account: encoder.encode("account"), id: encoder.encode(id) }).toFields();
        expect(decoder.decode(fields.account)).toBe("account");
        expect(decoder.decode(fields.id)).toBe(id);
      }
      const persisted = unhex("12345678123442349234123456789abc");
      const view = ResourceIdentity.fromFields({ account: encoder.encode("account"), id: persisted });
      expect(hex(view.toFields().id)).toBe(hex(persisted));
      expect(view.equals(new ResourceIdentity(encoder.encode("account"), encoder.encode("12345678-1234-4234-9234-123456789abc")))).toBe(false);
      expect(new ResourceIdentity(encoder.encode("account"), encoder.encode("cafe\u0301")).equals(
        new ResourceIdentity(encoder.encode("account"), encoder.encode("caf\u00e9")))).toBe(false);
    });

    test("host aliases cannot change signing or TLS spelling", () => {
      for (const host of ["Worker.account.example.com", "worker.account.example.com.",
        "worker.account.example.com:443", "x@worker.account.example.com", "127.1", "0x7f000001"]) {
        expect(() => canonicalResourceHost(host)).toThrow();
      }
    });

    test("plain JS callers cannot coerce text or numeric arrays into identity bytes", () => {
      const bytes = unhex("6162");
      for (const wrong of ["ab", [97, 98], new Uint16Array([97, 98]), null, {}]) {
        const unchecked = wrong as unknown as Uint8Array;
        expect(() => new ResourceIdentity(unchecked, bytes)).toThrow(TypeError);
        expect(() => new ResourceIdentity(bytes, unchecked)).toThrow(TypeError);
        expect(() => ResourceIdentity.decode(unchecked)).toThrow(TypeError);
      }
      // Node Buffer is a Uint8Array and retains the same raw byte semantics.
      expect(new ResourceIdentity(Buffer.from(bytes), Buffer.from(bytes)).equals(
        new ResourceIdentity(bytes, bytes))).toBe(true);
      const view = new ResourceIdentity(bytes, bytes);
      for (const wrong of [["example.com"], { toString: () => "example.com" }, 123, null]) {
        const unchecked = wrong as unknown as string;
        expect(() => view.hostname(unchecked)).toThrow(TypeError);
        expect(() => canonicalResourceHost(unchecked)).toThrow(TypeError);
      }
    });
  });
}
