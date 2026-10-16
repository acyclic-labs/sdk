import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { render, rust } from "./generate-machines-limits.mjs";

const limits = { maxPageSize: 256, maxEventPageSize: 1024, maxOutputReadBytes: 65536 };
const output = "typescript/packages/machines/src/limits.ts";

test("the Machines generator exports only the live Rust-owned limits", () => {
  assert.deepEqual(rust, [["acyclic-machines", "bin", "machines-limits"]]);
  assert.deepEqual(render([JSON.stringify(limits)]), {
    [output]: readFileSync(new URL(`../${output}`, import.meta.url), "utf8"),
  });
  assert.doesNotMatch(render([JSON.stringify({ ...limits, separator: "@sha256:", hexDigits: "unused" })])[output], /separator|digestHexLength|hexDigits|zeroDigit/);
});

test("each exported limit must be a positive safe integer", () => {
  for (const field of Object.keys(limits)) {
    for (const invalid of [undefined, null, 0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
      assert.throws(() => render([JSON.stringify({ ...limits, [field]: invalid })]), /positive safe integers/);
    }
  }
});
