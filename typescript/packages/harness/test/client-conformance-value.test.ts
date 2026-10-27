import { expect, test } from "bun:test";
import { clientValueKey } from "../src/client-conformance-value.js";

test("client conformance integer tags cannot alias literal model or approval data", () => {
  for (const value of [1n, -1n, 9_007_199_254_740_993n]) {
    const literal = {$integer: value.toString()};
    expect(clientValueKey(value)).not.toBe(clientValueKey(literal));
    expect(clientValueKey({arguments: [value]})).not.toBe(clientValueKey({arguments: [literal]}));
  }
  expect(clientValueKey(1)).toBe(clientValueKey(1n));
  expect(clientValueKey({cursor: 1, arguments: {a: "b", c: "d"}}))
    .toBe(clientValueKey({arguments: {c: "d", a: "b"}, cursor: 1n}));
  expect(clientValueKey({$integer: "1"})).not.toBe(clientValueKey({$$integer: "1"}));
  expect(clientValueKey(Object.fromEntries([["__proto__", {safe: true}]]))).not.toBe(clientValueKey({}));
});

test("client conformance rejects non-JSON or lossy projections instead of comparing their coerced JSON", () => {
  for (const value of [undefined, Number.NaN, Number.POSITIVE_INFINITY, -0, Number.MAX_SAFE_INTEGER + 1,
    new Map([["cursor", 1n]]), {value: undefined}, [undefined], {value: () => 1}, {value: Symbol("x")}]) {
    expect(() => clientValueKey(value)).toThrow();
  }
});
