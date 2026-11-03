import { expect, test } from "bun:test";
import * as contracts from "../generated/wasm/acyclic_harness_wasm.js";
import { ensureModelWasm } from "../src/wasm-runtime.js";

await ensureModelWasm();

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
});
