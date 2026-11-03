import { expect, test } from "bun:test";
import assert from "node:assert/strict";
import * as contracts from "../generated/wasm/acyclic_harness_wasm.js";
import { ensureModelWasm } from "../src/wasm-runtime.js";

await ensureModelWasm();
const exports = await contracts.default();

test("canonical admission releases JavaScript references after intrinsic failures", () => {
  const table = exports.__wbindgen_externrefs;
  const reason = new Error("original JavaScript intrinsic refusal");
  const refuse = () => { throw reason; };
  const array = Object.defineProperty([], Symbol.iterator, { get: refuse });
  const map = Object.defineProperty(new Map(), "forEach", { value: refuse });
  const sized = Object.defineProperty(new Map(), "size", { get: refuse });
  const inputs = [
    ["prototype", new Proxy({}, { getPrototypeOf: refuse })],
    ["keys", new Proxy({}, { ownKeys: refuse })],
    ["array iterator", array],
    ["Map callback", map],
    ["Map size", sized],
  ] as const;
  const outcomes = inputs.map(([step, input]) => {
    let error: unknown;
    try { contracts.encodeCanonicalJson(input); }
    catch (caught) { error = caught; }
    // Inspect the actual generated instance's reference table. Capacity is
    // not liveness, and unrelated exports may legitimately retain references.
    let retained = 0;
    for (let index = 0; index < table.length; ++index) {
      if (Object.is(table.get(index), input)) ++retained;
    }
    return { step, originalReason: error === reason, retained };
  });
  expect(outcomes.map(({ originalReason }) => originalReason)).toEqual(inputs.map(() => true));
  assert.equal(outcomes.reduce((total, { retained }) => total + retained, 0), 0,
    "JavaScript refusal retained original input");
  expect(contracts.decodeClientJson(contracts.encodeCanonicalJson({ valid: 7n })))
    .toEqual({ valid: 7n });
});
