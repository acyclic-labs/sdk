import { expect, test } from "bun:test";
import { NativeContracts } from "@acyclic-labs/harness";

test("installed canonical JSON preserves valid Unicode and matches native surrogate rejection", async () => {
  const contracts = await NativeContracts.create();
  for (const value of ["replacement \ufffd", "paired \ud83d\ude00", "\u0000"]) {
    expect(contracts.decodeModelJson(new TextEncoder().encode(JSON.stringify({ value })))).toEqual({ value });
    const roundTrip = JSON.parse(new TextDecoder().decode(contracts.encodeCanonicalJson({ [value]: value })));
    expect(roundTrip).toEqual({ [value]: value });
  }
  for (const invalid of ["\ud800", "\udc00", "\ud800x", "x\udc00"]) {
    expect(() => contracts.decodeModelJson(new TextEncoder().encode(JSON.stringify({ value: invalid })))).toThrow();
    for (const value of [{ value: invalid }, { [invalid]: 1 }, new Map([[invalid, 1]])]) {
      expect(() => contracts.encodeCanonicalJson(value)).toThrow();
    }
  }
});
