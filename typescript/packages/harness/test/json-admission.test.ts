import { expect, test } from "bun:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import fc from "fast-check";
import * as contracts from "../generated/wasm/acyclic_harness_wasm.js";
import { ensureModelWasm } from "../src/wasm-runtime.js";

await ensureModelWasm();

type Snapshot = { input: unknown; expected: unknown };
const snapshots = fc.memo<Snapshot>(depth => {
  const numbers = fc.oneof(fc.integer().map(value => value / 2),
    fc.constantFrom(-Number.MAX_SAFE_INTEGER, Number.MAX_SAFE_INTEGER))
    .map(input => ({ input, expected: Number.isInteger(input) ? BigInt(input) : input }));
  const scalar = fc.oneof(fc.constant(null), fc.boolean(), fc.string(),
    fc.bigInt({ min: -(2n ** 63n), max: 2n ** 64n - 1n }))
    .map(value => ({ input: value, expected: value }));
  const leaf = fc.oneof(scalar, numbers, fc.uint8Array({ maxLength: 32 }).map(input => ({
    input, expected: Array.from(input, byte => BigInt(byte)),
  })));
  if (depth <= 1) return leaf;
  const items = fc.array(snapshots(depth - 1), { maxLength: 4 }).map(values => ({
    input: values.map(value => value.input), expected: values.map(value => value.expected),
  }));
  const fields = fc.dictionary(fc.oneof(fc.string(), fc.constantFrom("__proto__", "constructor", "\0")),
    snapshots(depth - 1), { maxKeys: 4 }).chain(values => {
      const entries = Object.entries(values);
      const input = entries.map(([key, value]) => [key, value.input] as const);
      const expected = Object.fromEntries(entries.map(([key, value]) => [key, value.expected]));
      return fc.constantFrom<Snapshot>(
        { input: Object.fromEntries(input), expected },
        { input: Object.assign(Object.create(null), Object.fromEntries(input)), expected },
        { input: new Map(input), expected },
      );
    });
  return fc.oneof(leaf, items, fields);
});

test("canonical admission selects entries before evaluating their values", () => {
  // Count the shipping identity binding, without a timing or scale loop.
  // A shared child is valid on separate paths; only an active ancestor cycles.
  for (const depth of [4, 16]) {
    let input: unknown = 7n;
    for (let index = 0; index < depth; ++index) input = { child: input };
    const same = Object.is;
    let comparisons = 0;
    let canonical: Uint8Array;
    Object.is = (left, right) => { ++comparisons; return same(left, right); };
    try { canonical = contracts.encodeCanonicalJson(input); }
    finally { Object.is = same; }
    assert.deepEqual(contracts.decodeClientJson(canonical), input);
    assert.ok(comparisons <= 2 * depth, "canonical admission rescanned its ancestors");
  }
  const shared = { value: 7n };
  assert.deepEqual(contracts.decodeClientJson(contracts.encodeCanonicalJson([shared, shared])),
    [shared, shared], "canonical admission confused shared children with cycles");
  const constructor = Object.getOwnPropertyDescriptor(globalThis, "WeakSet")!;
  const methods = Object.getOwnPropertyDescriptors(WeakSet.prototype);
  const prototype = WeakSet.prototype;
  const input = Object.defineProperty({}, "child", { enumerable: true, get() {
    const poison = () => { throw new Error("JSON called a replaced identity primitive"); };
    for (const name of ["has", "add", "delete"]) Object.defineProperty(prototype, name, { value: poison });
    Object.defineProperty(globalThis, "WeakSet", { value: poison });
    // Reentry must also use the captured constructor, with independent custody.
    contracts.encodeCanonicalJson(shared);
    return shared;
  } });
  try {
    assert.deepEqual(contracts.decodeClientJson(contracts.encodeCanonicalJson(input)), { child: shared });
  } finally {
    Object.defineProperties(prototype, methods);
    Object.defineProperty(globalThis, "WeakSet", constructor);
  }

  for (const selected of [new Map<string, unknown>(), [] as unknown[]]) {
    const kind = Array.isArray(selected) ? "Array" : "Map";
    let reads = 0;
    const child = Object.defineProperty({}, "value", {
      enumerable: true,
      get() {
        // Fail a live Map revisit without a timeout. Array iteration fixes its
        // length, so also replace a later element to detect live value reads.
        if (++reads > 1) throw new Error(`canonical admission revisited an appended ${kind} entry`);
        if (Array.isArray(selected)) {
          selected[1] = "changed";
          selected.push(child);
        } else selected.set("late", child);
        return "selected";
      },
    });
    if (Array.isArray(selected)) selected.push(child, "retained");
    else selected.set("first", child);
    assert.deepEqual(contracts.decodeClientJson(contracts.encodeCanonicalJson(selected)),
      Array.isArray(selected) ? [{ value: "selected" }, "retained"] : { first: { value: "selected" } },
      `canonical admission did not retain selected ${kind} entries`);
    expect(reads).toBe(1);
  }

  // Map entries define the admitted input; unrelated metadata must neither
  // invoke caller code nor determine the allocation used to select entries.
  for (const size of [() => { throw new Error("unused Map size getter"); },
    () => 2 ** 32 - 1, () => -1, () => Number.NaN, () => Infinity]) {
    let sizeReads = 0;
    const input = Object.defineProperty(new Map([["selected", 7n]]), "size", {
      get() { ++sizeReads; return size(); },
    });
    let canonical: Uint8Array;
    try { canonical = contracts.encodeCanonicalJson(input); }
    catch { assert.fail("canonical Map admission evaluated unrelated size metadata"); }
    assert.equal(sizeReads, 0, "canonical Map admission evaluated unrelated size metadata");
    expect(contracts.decodeClientJson(canonical)).toEqual({ selected: 7n });
  }

  // Reuse the original native writer capture; this corpus is not a fabricated
  // external response. Its only numeric fields are the receipt's key bytes.
  const receipt = readFileSync(new URL(
    "../../../../rust/crates/harness-fuzz/corpus/retained_skill_records/materialized.json", import.meta.url));
  const expected = JSON.parse(receipt.toString(), (_key, value) => typeof value === "number" ? BigInt(value) : value);
  expect(contracts.decodeClientJson(receipt)).toEqual(expected);
  for (const number of [-0, Number.NaN, Infinity, -Infinity, 2 ** 53, -(2 ** 53)]) {
    assert.throws(() => contracts.encodeCanonicalJson({ nested: [number] }),
      "canonical admission accepted an unsafe JavaScript Number");
  }
  fc.assert(fc.property(snapshots(4), ({ input, expected }) => {
    const canonical = contracts.encodeCanonicalJson(input);
    expect(contracts.decodeClientJson(canonical)).toEqual(expected);
    expect(contracts.encodeCanonicalJson(contracts.decodeCanonicalJson(canonical))).toEqual(canonical);
  }), { seed: 1, numRuns: 1000, examples: [[{ input: 0.5, expected: 0.5 }],
    [{ input: Number.MAX_SAFE_INTEGER, expected: BigInt(Number.MAX_SAFE_INTEGER) }],
    [{ input: -Number.MAX_SAFE_INTEGER, expected: BigInt(-Number.MAX_SAFE_INTEGER) }]] });
});
