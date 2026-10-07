import { expect, test } from "bun:test";
import { validateComponentLabel } from "../src/model.js";
import {
  validateAuthorityPathSegment as validateAuthorityPathSegmentWasm,
  validateComponentLabel as validateComponentLabelWasm,
} from "../generated/wasm/acyclic_harness_wasm.js";

test("component labels preserve the Rust UTF-8 byte ceiling", () => {
  expect(() => validateComponentLabel("a".repeat(255), "label")).not.toThrow();
  expect(() => validateComponentLabel("a".repeat(256), "label")).toThrow("label is invalid");
  expect(() => validateComponentLabel("é".repeat(127), "label")).not.toThrow();
  expect(() => validateComponentLabel("é".repeat(128), "label")).toThrow("label is invalid");
  expect(() => validateComponentLabel("😀".repeat(63), "label")).not.toThrow();
  expect(() => validateComponentLabel("😀".repeat(64), "label")).toThrow("label is invalid");
});

test("component labels reject Rust whitespace, controls, separators, and dot segments", () => {
  for (const value of ["", ".", "..", "/", "\\", "a\n", "a\u{00a0}", "a\u{2003}", "a\u{007f}"]) {
    expect(() => validateComponentLabel(value, "component")).toThrow("component is invalid");
  }
  expect(() => validateComponentLabel("a\u{1f600}", "component")).not.toThrow();
});

test("component labels reject unpaired UTF-16 surrogates before UTF-8 encoding", () => {
  expect(() => validateComponentLabel("a\ud800", "component")).toThrow("component is invalid");
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
