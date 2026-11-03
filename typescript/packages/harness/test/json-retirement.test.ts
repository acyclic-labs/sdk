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
  const revoked = Proxy.revocable([], {});
  revoked.revoke();
  let revokedReason: unknown;
  try { Array.isArray(revoked.proxy); }
  catch (caught) { revokedReason = caught; }
  const child = Object.defineProperty({}, "value", { enumerable: true, get: refuse });
  const sources = [
    ["prototype", new Proxy({}, { getPrototypeOf: refuse }), reason],
    ["keys", new Proxy({}, { ownKeys: refuse }), reason],
    ["array iterator", array, reason],
    ["Map callback", map, reason],
    ["revoked array", revoked.proxy, revokedReason],
    ["child getter", child, reason],
  ] as const;
  const inputs = sources.flatMap(([step, child, expected]) => [
    [step, [child], expected],
    [`object ${step}`, [{ child }, child], expected],
    [`array ${step}`, [[child], child], expected],
    [`Map ${step}`, [new Map([["child", child]]), child], expected],
  ] as const);
  const outcomes = inputs.map(([step, owned, expected]) => {
    let error: unknown;
    try { contracts.encodeCanonicalJson(owned[0]); }
    catch (caught) { error = caught; }
    // Inspect the actual generated instance's reference table. Capacity is
    // not liveness, and unrelated exports may legitimately retain references.
    let retained = 0;
    for (let index = 0; index < table.length; ++index) {
      const reference = table.get(index);
      if (owned.some(value => Object.is(reference, value))) ++retained;
    }
    const originalReason = expected === reason ? error === reason
      : error instanceof TypeError && expected instanceof TypeError && error.message === expected.message;
    return { step, originalReason, retained };
  });
  expect(outcomes.map(({ originalReason }) => originalReason)).toEqual(inputs.map(() => true));
  assert.equal(outcomes.reduce((total, { retained }) => total + retained, 0), 0,
    "JavaScript refusal retained original input or selected child");
  expect(contracts.decodeClientJson(contracts.encodeCanonicalJson({ valid: 7n })))
    .toEqual({ valid: 7n });
});
