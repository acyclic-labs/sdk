import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { join, resolve, sep } from "node:path";
import test from "node:test";

test("Linux joins Harness packaging and generated checks before conformance, including failures", () => {
  const source = readFileSync("scripts/qualify-ci.sh", "utf8").replaceAll("\r\n", "\n");
  const start = source.indexOf("    background harness-package bash");
  const end = source.indexOf("    background harness-conformance bun", start);
  assert.ok(start > source.indexOf("    bun run test", source.indexOf("  linux)")));
  assert.doesNotMatch(source, /git restore --worktree --[\s\S]*?generated\/wasm/,
    "packaging must retain the bindings built from the qualified source");
  assert.ok(end > start);
  const commands = source.slice(start, end);
  const helpers = source.slice(source.indexOf("background() {"), source.indexOf("native_binding() {"))
    .replace("finish() {", 'finish() {\n  touch "$SDK_TEMP_DIR/finish-entered"');
  const bash = process.platform === "win32"
    ? join(process.env.ProgramFiles ?? "C:/Program Files", "Git", "bin", "bash.exe") : "bash";
  mkdirSync(".tools", { recursive: true });
  for (const harnessStatus of [0, 13]) for (const generatedStatus of [0, 21]) {
    const directory = mkdtempSync(".tools/linux-overlap-");
    try {
      const result = spawnSync(bash, ["-c", `set -euo pipefail
SDK_TEMP_DIR="$SDK_OVERLAP_FIXTURE"
SDK_ARTIFACT_DIR="$SDK_TEMP_DIR/artifacts"
bash() {
  [[ "$#" == 2 && "$1" == scripts/check-harness-package.sh ]]
  [[ "$2" == "$SDK_ARTIFACT_DIR/packages/harness" ]]
  touch "$SDK_TEMP_DIR/harness-started"
  while [[ ! -f "$SDK_TEMP_DIR/generated-done" || ! -f "$SDK_TEMP_DIR/finish-entered" ]]; do sleep 0.01; done
  echo harness-complete
  touch "$SDK_TEMP_DIR/harness-done"
  return ${harnessStatus}
}
bun() {
  [[ "$#" == 2 && "$1" == run && "$2" == check:baselines ]]
  while [[ ! -f "$SDK_TEMP_DIR/harness-started" ]]; do sleep 0.01; done
  [[ ! -f "$SDK_TEMP_DIR/harness-done" ]]
  echo generated-complete
  touch "$SDK_TEMP_DIR/generated-done"
  return ${generatedStatus}
}
${helpers}
${commands}
[[ -f "$SDK_TEMP_DIR/harness-done" && -f "$SDK_TEMP_DIR/generated-done" ]]
echo conformance-ready
`], {
        encoding: "utf8", timeout: 60000,
        env: { ...process.env, SDK_OVERLAP_FIXTURE: directory.replaceAll("\\", "/") },
      });
      assert.equal(result.error, undefined);
      assert.equal(result.status, harnessStatus || generatedStatus, result.stderr);
      assert.match(result.stdout, /generated-complete/);
      assert.match(result.stdout, /harness-complete/);
      assert.equal(result.stdout.includes("conformance-ready"), harnessStatus === 0 && generatedStatus === 0);
      assert.equal(result.stdout.includes("harness-package failed"), harnessStatus !== 0);
    } finally {
      assert.ok(resolve(directory).startsWith(resolve(".tools") + sep));
      rmSync(directory, { recursive: true, force: true });
    }
  }
});
