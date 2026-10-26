import { beforeAll, expect, test } from "bun:test";
import { initializeClientQueries } from "../src/index.js";
import {
  validateAuthorityPathSegment,
  validateComponentLabel,
} from "../generated/wasm/acyclic_harness_wasm.js";

beforeAll(initializeClientQueries);

test("component labels preserve the Rust portable-count contract", () => {
  for (const value of ["a".repeat(255), "a".repeat(256), "é".repeat(127),
    "é".repeat(128), "😀".repeat(63), "😀".repeat(64)]) {
    expect(() => validateComponentLabel(value)).not.toThrow();
  }
});

test("component labels reject Rust whitespace, controls, separators, and dot segments", () => {
  for (const value of ["", ".", "..", "/", "\\", "a\n", "a\u{00a0}", "a\u{2003}", "a\u{007f}"]) {
    expect(() => validateComponentLabel(value)).toThrow();
  }
  expect(() => validateComponentLabel("a\u{1f600}")).not.toThrow();
});

test("component labels reject unpaired UTF-16 surrogates before UTF-8 encoding", () => {
  expect(() => validateComponentLabel("a\ud800")).toThrow();
});

test("Rust WASM owns the same label and authority policy after initialization", () => {
  for (const value of ["tool", "é".repeat(127), "😀".repeat(63)]) {
    expect(() => validateComponentLabel(value)).not.toThrow();
  }
  for (const value of ["", ".", "..", "a/b", "a\\b", "a\n", "a\ud800"]) {
    expect(() => validateComponentLabel(value)).toThrow();
  }
  for (const value of ["agent-1", "é", "😀"]) {
    expect(validateAuthorityPathSegment(value)).toBe(value);
  }
  for (const value of ["", ".", "..", "a/b", "a\\b", "a\n"]) {
    expect(() => validateAuthorityPathSegment(value)).toThrow();
  }
});
