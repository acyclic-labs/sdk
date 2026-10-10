import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { wasmSurfaceComparison } from "./tracked-wasm-surfaces.mjs";

test("actual drift guard rejects rebuilt stale HEAD declarations despite index/HEAD changes", () => {
  const root = mkdtempSync(join(tmpdir(), "wasm-head-comparison-"));
  const git = args => execFileSync("git", args, { cwd: root, stdio: "pipe" });
  const relative = "typescript/packages/filesystem/generated/wasm/fixture.d.ts";
  const path = join(root, relative);
  const fresh = join(root, "fresh");
  const commit = message => git(["-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "-c", "commit.gpgsign=false", "commit", "--quiet", "-m", message]);
  try {
    git(["init", "--quiet"]);
    mkdirSync(join(root, "typescript/packages/filesystem/generated/wasm"), { recursive: true });
    mkdirSync(fresh);
    writeFileSync(path, "export const committed: number;\n");
    git(["add", "."]);
    commit("committed declaration");
    for (const directory of [join(root, "typescript/packages/filesystem/generated/wasm"), fresh]) {
      writeFileSync(join(directory, "fixture.js"), "export const value = 1;\n");
      writeFileSync(join(directory, "fixture_bg.wasm.d.ts"), "export const memory: WebAssembly.Memory;\n");
      writeFileSync(join(directory, "fixture.d.ts"), "export const rebuilt: string;\n");
    }
    git(["add", relative]);
    const compare = wasmSurfaceComparison(root);
    assert.throws(() => compare("filesystem", "fixture", fresh), /generated WASM \.d\.ts drift: filesystem/);
    commit("new declaration");
    rmSync(path);
    assert.throws(() => compare("filesystem", "fixture", fresh), /generated WASM \.d\.ts drift: filesystem/);
    assert.doesNotThrow(() => wasmSurfaceComparison(root)("filesystem", "fixture", fresh));
    writeFileSync(join(fresh, "fixture.d.ts"), "export const committed: number;\n");
    assert.doesNotThrow(() => compare("filesystem", "fixture", fresh));
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("missing source authority fails closed", () => {
  const root = mkdtempSync(join(tmpdir(), "wasm-no-head-"));
  try {
    execFileSync("git", ["init", "--quiet"], { cwd: root });
    assert.throws(() => wasmSurfaceComparison(root));
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
