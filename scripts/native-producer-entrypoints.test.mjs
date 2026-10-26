import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import test from "node:test";

// Override only with captured, unchanged historical entrypoint source. Real
// Node/generator failures are retained by the caller as the negative control.
const historical = JSON.parse(process.env.NATIVE_ENTRYPOINTS_UNDER_TEST ?? "{}");
for (const family of ["stream", "actors"]) {
  const entry = historical[family] ?? fileURLToPath(new URL(`./build-${family}-native.mjs`, import.meta.url));
  for (const [name, args, extra, expected] of [
    ["target", ["build"], {}, /build requires --target/],
    ["output", ["build", "--target", "x86_64-pc-windows-msvc"], {}, /build requires --output/],
    ["delegate", ["build", "--target", "x86_64-pc-windows-msvc", "--output", "unused"],
      { RUSTC_WORKSPACE_WRAPPER: "unadmitted-delegate" }, /does not support RUSTC_WORKSPACE_WRAPPER/],
  ]) {
    test(`${family} admits ${name} before launching generators or Cargo`, () => {
      const result = spawnSync(process.execPath, [entry, ...args], {
        env: { ...process.env, PATH: "", Path: "", ...extra }, encoding: "utf8",
      });
      assert.notEqual(result.status, 0);
      assert.match(result.stderr, expected);
      assert.doesNotMatch(result.stderr, /ENOENT|spawn.*cargo|Generation failed/);
    });
  }
}
