import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { encodeLiveUpdate } from "../src/live-update.js";

test("v1 bytes agree with Rust for full counters and Unicode/numeric keys", () => {
  const fixture = readFileSync(new URL("../../../../rust/crates/actors/tests/fixtures/live-update-v1.json", import.meta.url), "utf8").trimEnd();
  expect(new TextDecoder().decode(encodeLiveUpdate(JSON.parse(fixture).value))).toBe(fixture);
  for (const value of [Number.MAX_SAFE_INTEGER + 1, NaN, Infinity, undefined, "\ud800"]) {
    expect(() => encodeLiveUpdate(value)).toThrow(TypeError);
  }
});
