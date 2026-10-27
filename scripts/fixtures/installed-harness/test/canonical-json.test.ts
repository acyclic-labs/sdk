import { expect, test } from "bun:test";
import { initializeClientQueries, decodeClientJson, encodeCanonicalJson } from "@acyclic-labs/harness";

test("installed canonical JSON preserves valid Unicode and matches native surrogate rejection", async () => {
  await initializeClientQueries();
  for (const value of ["replacement \ufffd", "paired \ud83d\ude00", "\u0000"]) {
    expect(decodeClientJson(new TextEncoder().encode(JSON.stringify({ value })))).toEqual({ value });
    const roundTrip = JSON.parse(new TextDecoder().decode(encodeCanonicalJson({ [value]: value })));
    expect(roundTrip).toEqual({ [value]: value });
  }
  for (const invalid of ["\ud800", "\udc00", "\ud800x", "x\udc00"]) {
    expect(() => decodeClientJson(new TextEncoder().encode(JSON.stringify({ value: invalid })))).toThrow();
    for (const value of [{ value: invalid }, { [invalid]: 1 }, new Map([[invalid, 1]])]) {
      expect(() => encodeCanonicalJson(value)).toThrow();
    }
  }
});

// This is the client integer contract; it does not claim the deleted model
// facade's Number-normalization policy.
test("installed client JSON retains full-width counters and refuses rounded inputs", async () => {
  await initializeClientQueries();
  const cursor = 9_007_199_254_740_993n;
  expect(decodeClientJson(encodeCanonicalJson({ cursor }))).toEqual({ cursor });
  expect(() => encodeCanonicalJson({ cursor: Number(cursor) })).toThrow();
  // JSON.parse rounds the wire integer before encoding admission.
  const rounded = JSON.parse('{"cursor":9007199254740993}');
  expect(() => encodeCanonicalJson(rounded)).toThrow();
});
