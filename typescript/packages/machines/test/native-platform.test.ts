import assert from "node:assert/strict";
import test from "node:test";
import { nativeCompanionTarget } from "../src/native.js";

const glibcReport = { getReport: () => ({ header: { glibcVersionRuntime: "2.35" } }) };
const muslReport = { getReport: () => ({ header: {} }) };

test("native Machines resolves Linux companions to the published libc-qualified package", () => {
  assert.equal(nativeCompanionTarget("linux", "x64", glibcReport), "linux-x64-gnu");
  assert.equal(nativeCompanionTarget("linux", "arm64", glibcReport), "linux-arm64-gnu");
  assert.equal(nativeCompanionTarget("win32", "x64", glibcReport), "win32-x64");
});

test("native Machines preserves a detected musl target for package resolution", () => {
  assert.equal(nativeCompanionTarget("linux", "x64", muslReport), "linux-x64-musl");
});

