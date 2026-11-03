import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import fc from "fast-check";
import * as contracts from "../generated/wasm/acyclic_harness_wasm.js";
import { ensureModelWasm } from "../src/wasm-runtime.js";

await ensureModelWasm();

type Snapshot = { input: unknown; expected: unknown };
const snapshots = fc.memo<Snapshot>(depth => {
  const scalar = fc.oneof(fc.constant(null), fc.boolean(), fc.string(),
    fc.bigInt({ min: -(2n ** 63n), max: 2n ** 64n - 1n }))
    .map(value => ({ input: value, expected: value }));
  const leaf = fc.oneof(scalar, fc.uint8Array({ maxLength: 32 }).map(input => ({
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

test("canonical admission selects Map entries before evaluating their values", () => {
  const selected = new Map<string, unknown>();
  let reads = 0;
  const child = Object.defineProperty({}, "value", {
    enumerable: true,
    get() {
      // A live traversal revisits this child through the appended entry. Fail
      // on that second visit so the progress regression never needs a timeout.
      if (++reads > 1) throw new Error("canonical admission revisited an appended Map entry");
      selected.set("late", child);
      return "selected";
    },
  });
  selected.set("first", child);
  expect(contracts.decodeClientJson(contracts.encodeCanonicalJson(selected)))
    .toEqual({ first: { value: "selected" } });
  expect(reads).toBe(1);

  // Reuse the original native writer capture; this corpus is not a fabricated
  // external response. Its only numeric fields are the receipt's key bytes.
  const receipt = readFileSync(new URL(
    "../../../../rust/crates/harness-fuzz/corpus/retained_skill_records/materialized.json", import.meta.url));
  const expected = JSON.parse(receipt.toString(), (_key, value) => typeof value === "number" ? BigInt(value) : value);
  expect(contracts.decodeClientJson(receipt)).toEqual(expected);
  fc.assert(fc.property(snapshots(4), ({ input, expected }) => {
    const canonical = contracts.encodeCanonicalJson(input);
    expect(contracts.decodeClientJson(canonical)).toEqual(expected);
    expect(contracts.encodeCanonicalJson(contracts.decodeCanonicalJson(canonical))).toEqual(canonical);
  }), { seed: 1, numRuns: 1000 });
});
