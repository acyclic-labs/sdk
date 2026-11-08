import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { createNativeProducer } from "./build-native-family.mjs";
import { nativeFamily } from "./native-family.mjs";

test("both native admission baselines equal the actual Rust generator and refuse a missing producer", () => {
  const binary = process.env.SDK_PROTO_CODEGEN;
  const empty = mkdtempSync(join(tmpdir(), "sdk-protocol-producer-"));
  try {
    for (const family of ["actors", "stream"]) {
      const producer = createNativeProducer(nativeFamily(family));
      producer.assertCurrentNativeFamily();
      process.env.SDK_PROTO_CODEGEN = join(empty, "missing");
      try {
        assert.throws(() => producer.assertCurrentNativeFamily(), error =>
          error.code === "PROTO_CODEGEN_FAILED" && error.cause?.code === "ENOENT"
          && error.message.includes("step native-family failed; recovery:"));
      } finally {
        if (binary === undefined) delete process.env.SDK_PROTO_CODEGEN;
        else process.env.SDK_PROTO_CODEGEN = binary;
      }
    }
  } finally {
    rmSync(empty, { recursive: true, force: true });
  }
});

// Override only with captured, unchanged historical entrypoint source. Real
// Node/generator failures are retained by the caller as the negative control.
const historical = JSON.parse(process.env.NATIVE_ENTRYPOINTS_UNDER_TEST ?? "{}");
for (const family of ["stream", "actors"]) {
  const entry = historical[family] ?? fileURLToPath(new URL(`./build-${family}-native.mjs`, import.meta.url));
  for (const [name, args, extra, expected] of [
    ["target", ["build"], {}, /build requires --target/],
    ["output", ["build", "--target", "x86_64-pc-windows-msvc"], {}, /build requires --output <native-bundle>/],
    ["delegate", ["build", "--target", "x86_64-pc-windows-msvc", "--output", "unused"],
      { RUSTC_WORKSPACE_WRAPPER: "unadmitted-delegate" }, /does not support RUSTC_WORKSPACE_WRAPPER/],
  ]) {
    test(`${family} admits ${name} before launching generators or Cargo`, () => {
      const result = spawnSync(process.execPath, [entry, ...args], {
        env: { ...process.env, PATH: "", Path: "", ...extra }, encoding: "utf8",
      });
      assert.notEqual(result.status, 0);
      assert.match(result.stderr, expected);
      assert.doesNotMatch(result.stderr, /ENOENT|requires a clean source closure|spawn.*cargo|Generation failed/);
    });
  }
}
