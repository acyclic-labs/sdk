import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { join, resolve, sep } from "node:path";
import test from "node:test";

test("Linux starts filesystem packaging while Harness is pending and joins preparation failures", () => {
  const source = readFileSync("scripts/qualify-ci.sh", "utf8").replaceAll("\r\n", "\n");
  const start = source.indexOf("    background harness-package bash");
  const end = source.indexOf("    background harness-conformance bun", start);
  assert.ok(start > source.indexOf("    bun run test", source.indexOf("  linux)")));
  const restore = source.match(/^    git restore --worktree -- \\\n      typescript\/packages\/filesystem\/generated\/wasm \\\n      typescript\/packages\/stream\/generated\/wasm\n/m) ?? assert.fail("missing tracked WASM restore");
  assert.ok(start >= (restore.index ?? assert.fail("missing restore position")) + restore[0].length,
    "Harness packaging must start after the complete tracked WASM restore");
  assert.ok(end > start);
  const commands = source.slice(start, end);
  const helpers = source.slice(source.indexOf("background() {"), source.indexOf("native_binding() {"))
    .replace("finish() {", 'finish() {\n  touch "$SDK_TEMP_DIR/finish-entered"');
  const bash = process.platform === "win32"
    ? join(process.env.ProgramFiles ?? "C:/Program Files", "Git", "bin", "bash.exe") : "bash";
  mkdirSync(".tools", { recursive: true });
  for (const harnessStatus of [0, 13]) for (const generatedStatus of [0, 21]) for (const filesystemStatus of [0, 34]) {
    const directory = mkdtempSync(".tools/linux-overlap-");
    try {
      const result = spawnSync(bash, ["-c", `set -euo pipefail
SDK_TEMP_DIR="$SDK_OVERLAP_FIXTURE"
SDK_ARTIFACT_DIR="$SDK_TEMP_DIR/artifacts"
bash() {
  [[ "$#" == 2 ]]
  case "$1" in
    scripts/check-harness-package.sh)
      [[ "$2" == "$SDK_ARTIFACT_DIR/packages/harness" ]]
      touch "$SDK_TEMP_DIR/harness-started"
      while [[ ! -f "$SDK_TEMP_DIR/generated-done" || ! -f "$SDK_TEMP_DIR/finish-entered" ]]; do sleep 0.01; done
      if (( ${generatedStatus} == 0 )); then
        while [[ ! -f "$SDK_TEMP_DIR/filesystem-started" ]]; do sleep 0.01; done
      fi
      echo harness-complete
      touch "$SDK_TEMP_DIR/harness-done"
      return ${harnessStatus} ;;
    scripts/check-filesystem-package.sh)
      [[ "$2" == "$SDK_ARTIFACT_DIR/packages/filesystem" ]]
      [[ -f "$SDK_TEMP_DIR/generated-done" && ! -f "$SDK_TEMP_DIR/harness-done" ]]
      touch "$SDK_TEMP_DIR/filesystem-started"
      while [[ ! -f "$SDK_TEMP_DIR/harness-done" ]]; do sleep 0.01; done
      echo filesystem-complete
      touch "$SDK_TEMP_DIR/filesystem-done"
      return ${filesystemStatus} ;;
    *) return 2 ;;
  esac
}
bun() {
  [[ "$#" == 2 && "$1" == run && "$2" == check:generated ]]
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
filesystem_status=0
finish filesystem-package || filesystem_status=$?
exit "$filesystem_status"
`], {
        encoding: "utf8", timeout: 60000,
        env: { ...process.env, SDK_OVERLAP_FIXTURE: directory.replaceAll("\\", "/") },
      });
      assert.equal(result.error, undefined);
      assert.equal(result.status, harnessStatus || generatedStatus || filesystemStatus, result.stderr);
      assert.match(result.stdout, /generated-complete/);
      assert.match(result.stdout, /harness-complete/);
      assert.equal(result.stdout.includes("conformance-ready"), harnessStatus === 0 && generatedStatus === 0);
      assert.equal(result.stdout.includes("harness-package failed"), harnessStatus !== 0);
      assert.equal(result.stdout.includes("filesystem-complete"), generatedStatus === 0);
      assert.equal(result.stdout.includes("filesystem-package failed"), generatedStatus === 0 && filesystemStatus !== 0);
    } finally {
      assert.ok(resolve(directory).startsWith(resolve(".tools") + sep));
      rmSync(directory, { recursive: true, force: true });
    }
  }
});
