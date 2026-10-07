import { describe, expect, test } from "bun:test";
import fc from "fast-check";
import {
  adaptCompatibilityWire,
  decodeFixedBytes,
  type CompatibilityWireKind,
  type RawCompatibilityWire,
} from "../src/compat.js";

function rawWire(): RawCompatibilityWire {
  const encode = (kind: CompatibilityWireKind) => (valueJson: string): string =>
    JSON.stringify({ version: 1, kind, payload: JSON.parse(valueJson) });
  const decode = (kind: CompatibilityWireKind) => (valueJson: string): string => {
    const envelope = JSON.parse(valueJson) as { version: number; kind: string; payload: unknown };
    if (envelope.version !== 1 || envelope.kind !== kind) {
      throw new TypeError("invalid envelope");
    }
    return JSON.stringify(envelope.payload);
  };
  return {
    encodeMergePlanJson: encode("merge-plan"),
    decodeMergePlanJson: decode("merge-plan"),
    encodeMergeCandidateJson: encode("merge-candidate"),
    decodeMergeCandidateJson: decode("merge-candidate"),
    encodeMultiRootPlanJson: encode("multi-root-plan"),
    decodeMultiRootPlanJson: decode("multi-root-plan"),
    encodeMultiRootCandidateJson: encode("multi-root-candidate"),
    decodeMultiRootCandidateJson: decode("multi-root-candidate"),
    encodePublicationJson: encode("publication"),
    decodePublicationJson: decode("publication"),
  };
}

describe("compatibility wire adapter", () => {
  for (const kind of [
    "merge-plan",
    "merge-candidate",
    "multi-root-plan",
    "multi-root-candidate",
    "publication",
  ] as const) {
    test(`${kind} dispatches through the canonical codec`, () => {
      const wire = adaptCompatibilityWire(rawWire());
      const bytes = [1, 2, 3];
      const envelope = wire.encode(kind, { bytes });
      bytes[0] = 9;
      expect(envelope).toEqual({ version: 1, kind, payload: { bytes: [1, 2, 3] } });
      expect(wire.decode(kind, envelope)).toEqual({ bytes: [1, 2, 3] });
    });
  }

  test("a mismatched envelope fails closed", () => {
    const wire = adaptCompatibilityWire(rawWire());
    const envelope = wire.encode("merge-plan", { conflicts: [] });
    expect(() => wire.decode("publication", envelope)).toThrow("invalid envelope");
  });
});

describe("compatibility wire properties", () => {
  test("finite JSON payloads round-trip through every envelope kind", () => {
    const wire = adaptCompatibilityWire(rawWire());
    const kinds = fc.constantFrom("merge-plan", "merge-candidate", "multi-root-plan", "multi-root-candidate", "publication" as const);
    fc.assert(fc.property(kinds, fc.jsonValue(), (kind, value) => {
      const expected: unknown = JSON.parse(JSON.stringify(value));
      expect(wire.decode(kind, wire.encode(kind, value))).toEqual(expected);
    }), { numRuns: 100 });
  });

  test("fixed byte identities accept exactly length-matched octet arrays", () => {
    fc.assert(fc.property(fc.array(fc.integer({ min: 0, max: 255 }), { maxLength: 40 }), fc.nat(40), (bytes, length) => {
      if (bytes.length === length) expect(Array.from(decodeFixedBytes(bytes, length, "id"))).toEqual(bytes);
      else expect(() => decodeFixedBytes(bytes, length, "id")).toThrow(TypeError);
    }), { numRuns: 100 });
    const invalid = fc.oneof(fc.integer({ max: -1 }), fc.integer({ min: 256 }), fc.double({ noInteger: true }), fc.string());
    fc.assert(fc.property(fc.array(fc.integer({ min: 0, max: 255 }), { maxLength: 31 }), invalid, (prefix, byte) => {
      expect(() => decodeFixedBytes([...prefix, byte], prefix.length + 1, "id")).toThrow(TypeError);
    }), { numRuns: 100 });
  });
});
