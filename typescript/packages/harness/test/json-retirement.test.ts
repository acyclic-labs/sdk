import { expect, test } from "bun:test";
import assert from "node:assert/strict";
import * as contracts from "../generated/wasm/acyclic_harness_wasm.js";
import { ensureModelWasm } from "../src/wasm-runtime.js";

await ensureModelWasm();
const exports = await contracts.default();

test("canonical admission releases JavaScript references after admission failures", () => {
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
  const objectCycle: { child?: unknown } = {};
  objectCycle.child = objectCycle;
  const arrayCycle: unknown[] = [];
  arrayCycle.push(arrayCycle);
  const mapCycle = new Map<string, unknown>();
  mapCycle.set("child", mapCycle);
  const sources = [
    ["prototype", new Proxy({}, { getPrototypeOf: refuse }), reason],
    ["keys", new Proxy({}, { ownKeys: refuse }), reason],
    ["array iterator", array, reason],
    ["Map callback", map, reason],
    ["revoked array", revoked.proxy, revokedReason],
    ["child getter", child, reason],
    // These return through Rust's own refusal exits, rather than a caught
    // JavaScript intrinsic. Their errors are strings; wording is not pinned.
    ["undefined", undefined, null],
    ["symbol", Symbol("unsupported JSON value"), null],
    ["function", () => {}, null],
    ["negative zero", -0, null],
    ["nonfinite Number", Number.NaN, null],
    ["unsafe Number", 2 ** 53, null],
    ["out-of-range BigInt", 2n ** 64n, null],
    ["invalid UTF-16", "\ud800", null],
    ["nonplain object", new Date(0), null],
    ["nonstring Map key", new Map([[7, "value"]]), null],
    ["object cycle", objectCycle, null],
    ["array cycle", arrayCycle, null],
    ["Map cycle", mapCycle, null],
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
      // Undefined and null have permanent reserved entries in this table.
      if (owned.some(value => value != null && Object.is(reference, value))) ++retained;
    }
    const originalReason = expected === null ? typeof error === "string"
      : expected === reason ? error === reason
      : error instanceof TypeError && expected instanceof TypeError && error.message === expected.message;
    return { step, originalReason, retained };
  });
  expect(outcomes.map(({ originalReason }) => originalReason)).toEqual(inputs.map(() => true));
  assert.equal(outcomes.reduce((total, { retained }) => total + retained, 0), 0,
    "JavaScript refusal retained original input or selected child");
  expect(contracts.decodeClientJson(contracts.encodeCanonicalJson({ valid: 7n })))
    .toEqual({ valid: 7n });
});
