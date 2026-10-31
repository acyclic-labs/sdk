import { expect, test } from "bun:test";
import { copyObjectReference, isObjectReference, requireObjectReference } from "../src/binding-values.js";
import objectReferences from "./generated/object-references.json";

test("opaque references follow the owner codec for every kind and custody", () => {
  const tags = new Set(objectReferences.layouts.map(layout => layout.tag));
  for (const fixture of objectReferences.fixtures) {
    const bytes = Uint8Array.from(fixture.bytes);
    expect(isObjectReference(bytes)).toBe(true);
    expect(isObjectReference(bytes, fixture.kind)).toBe(true);
    expect(isObjectReference(bytes, fixture.kind === 1 ? 2 : 1)).toBe(false);
    const copy = copyObjectReference(bytes, "object", fixture.kind)!;
    expect(copy).toEqual(bytes);
    expect(copy.buffer).not.toBe(bytes.buffer);
    for (let length = 0; length < bytes.length; length++) {
      expect(isObjectReference(bytes.subarray(0, length))).toBe(false);
    }
    const trailing = new Uint8Array(bytes.length + 1);
    trailing.set(bytes);
    expect(isObjectReference(trailing)).toBe(false);
    expect(() => requireObjectReference(trailing, "object")).toThrow(TypeError);
    for (let tag = 0; tag <= 255; tag++) {
      if (tags.has(tag)) continue;
      copy[0] = tag;
      expect(isObjectReference(copy)).toBe(false);
    }
  }
  for (const value of [undefined, null, [], new ArrayBuffer(0)]) {
    expect(isObjectReference(value)).toBe(false);
  }
});
