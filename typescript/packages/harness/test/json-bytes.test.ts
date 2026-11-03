import { test } from "bun:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import fc from "fast-check";
import * as contracts from "../generated/wasm/acyclic_harness_wasm.js";
import { ensureModelWasm } from "../src/wasm-runtime.js";

await ensureModelWasm();

// This is the existing physical native writer capture. Mutations change its
// bytes or one original numeric key element, without another corpus producer.
const captured = readFileSync(new URL(
  "../../../../rust/crates/harness-fuzz/corpus/retained_skill_records/materialized.json", import.meta.url));
const text = captured.toString();
const bytes = new TextEncoder();
const tokenInput = (token: string) => bytes.encode(text.replace(/("key":\[)\d+/u, (_match, prefix) => prefix + token));
const token = fc.oneof(
  fc.bigInt({ min: -(2n ** 80n), max: 2n ** 80n }).map(String),
  fc.integer({ min: -400, max: 400 }).map(exponent => `1e${exponent}`),
  fc.string().map(value => JSON.stringify(value)),
);
const inputs = fc.oneof(
  fc.constant(Uint8Array.from(captured)),
  token.map(tokenInput),
  fc.tuple(fc.integer({ min: 0, max: captured.length }),
    fc.integer({ min: 0, max: 8 }), fc.uint8Array({ maxLength: 16 }))
    .map(([offset, removed, inserted]) => Uint8Array.from([
      ...captured.subarray(0, offset), ...inserted, ...captured.subarray(offset + removed),
    ])),
);

function expectedValue(input: Uint8Array): unknown {
  const source = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(input);
  // The engine owns JSON syntax and exposes each original number token. This
  // oracle never tries to recover exact integers from a rounded Number.
  return JSON.parse(source, (key, value, context: { source?: string }) => {
    if (!key.isWellFormed() || typeof value === "string" && !value.isWellFormed()) {
      throw new Error("JSON strings must be Unicode scalar values");
    }
    if (typeof value !== "number") return value;
    if (!Number.isFinite(value)) throw new Error("JSON numbers must be finite");
    assert.equal(typeof context.source, "string", "the engine must retain original JSON tokens");
    if (!/^-?\d+$/u.test(context.source!) || context.source === "-0") return value;
    const exact = BigInt(context.source!);
    if (exact < -(2n ** 63n) || exact > 2n ** 64n - 1n) throw new Error("JSON integer is outside i64/u64");
    return exact;
  });
}

test("external JSON bytes preserve engine tokens or refuse before rounding", () => {
  fc.assert(fc.property(inputs, input => {
    let expected: unknown;
    let refused = false;
    try { expected = expectedValue(input); }
    catch (error) {
      if (error instanceof assert.AssertionError) throw error;
      refused = true;
    }
    if (refused) {
      assert.throws(() => contracts.decodeClientJson(input),
        "external JSON input admission disagrees with engine token oracle");
    } else {
      assert.deepEqual(contracts.decodeClientJson(input), expected,
        "external JSON input lost original token precision or value");
    }
  }), {
    seed: 1, numRuns: 1000,
    examples: [[tokenInput(String(2n ** 64n))], [tokenInput(String(-(2n ** 63n) - 1n))]],
  });
});
