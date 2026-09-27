import { describe, expect, test } from "bun:test";
import {
  adaptCompatibilityWire,
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
