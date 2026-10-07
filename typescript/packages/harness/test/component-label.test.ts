import { expect, test } from "bun:test";
import { validateComponentLabel } from "../src/model.js";
import {
  validateAuthorityPathSegment as validateAuthorityPathSegmentWasm,
  validateComponentLabel as validateComponentLabelWasm,
} from "../generated/wasm/acyclic_harness_wasm.js";

test("component labels preserve the Rust UTF-8 byte ceiling", async () => {
  await expect(validateComponentLabel("a".repeat(255), "label")).resolves.toBeUndefined();
  await expect(validateComponentLabel("a".repeat(256), "label")).rejects.toThrow("label is invalid");
  await expect(validateComponentLabel("é".repeat(127), "label")).resolves.toBeUndefined();
  await expect(validateComponentLabel("é".repeat(128), "label")).rejects.toThrow("label is invalid");
  await expect(validateComponentLabel("😀".repeat(63), "label")).resolves.toBeUndefined();
  await expect(validateComponentLabel("😀".repeat(64), "label")).rejects.toThrow("label is invalid");
});

test("component labels reject Rust whitespace, controls, separators, and dot segments", async () => {
  for (const value of ["", ".", "..", "/", "\\", "a\n", "a\u{00a0}", "a\u{2003}", "a\u{007f}"]) {
    await expect(validateComponentLabel(value, "component")).rejects.toThrow("component is invalid");
  }
  await expect(validateComponentLabel("a\u{1f600}", "component")).resolves.toBeUndefined();
});

test("component labels reject unpaired UTF-16 surrogates before UTF-8 encoding", async () => {
  await expect(validateComponentLabel("a\ud800", "component")).rejects.toThrow("component is invalid");
});

test("Rust WASM owns the same label and authority policy after initialization", () => {
  for (const value of ["tool", "é".repeat(127), "😀".repeat(63)]) {
    expect(() => validateComponentLabelWasm(value)).not.toThrow();
  }
  for (const value of ["", ".", "..", "a/b", "a\\b", "a\n", "é".repeat(128), "😀".repeat(64), "a\ud800"]) {
    expect(() => validateComponentLabelWasm(value)).toThrow();
  }
  for (const value of ["agent-1", "é", "😀"]) {
    expect(validateAuthorityPathSegmentWasm(value)).toBe(value);
  }
  for (const value of ["", ".", "..", "a/b", "a\\b", "a\n"]) {
    expect(() => validateAuthorityPathSegmentWasm(value)).toThrow();
  }
});
