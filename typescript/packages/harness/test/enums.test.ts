import { describe, expect, test } from "bun:test";
import {
  aggregateKindToWire,
  decodeAggregateKind,
  groupPolicies,
} from "../src/enums.js";
import {
  AggregateKind as WireAggregateKind,
} from "../generated/proto/harness/v2/harness_pb.js";

describe("derived Harness enum mappings", () => {
  test("round trips explicit public spellings through generated protobuf values", () => {
    for (const publicKind of ["agent", "conversation", "session", "turn", "task"] as const) {
      const wireKind = aggregateKindToWire[publicKind];
      expect(decodeAggregateKind(wireKind)).toBe(publicKind);
    }
  });

  test("rejects unspecified and unknown wire values", () => {
    expect(() => decodeAggregateKind(WireAggregateKind.UNSPECIFIED)).toThrow();
    expect(() => decodeAggregateKind(99)).toThrow();
  });

  test("retains the GroupPolicies object shape", () => {
    expect(groupPolicies.collectAll).toEqual({ kind: "collect-all" });
    expect(groupPolicies.cancelOnFailure).toEqual({ kind: "cancel-on-failure" });
  });
});
