import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { join, resolve, sep } from "node:path";
import test from "node:test";

test("Linux overlaps filesystem packaging but joins Harness before its dependent consumer", () => {
  const source = readFileSync("scripts/qualify-ci.sh", "utf8").replaceAll("\r\n", "\n");
  const start = source.indexOf("    background harness-package bash");
  const end = source.indexOf("    bun run licenses", start);
  assert.doesNotMatch(readFileSync("scripts/check-filesystem-package.sh", "utf8"), /check-typescript-packages\.sh|typescript-qualification\.mjs/);
  assert.ok(start > source.indexOf("    bun run test", source.indexOf("  linux)")));
  const restore = source.match(/^    git restore --worktree -- \\\n      typescript\/packages\/filesystem\/generated\/wasm \\\n      typescript\/packages\/stream\/generated\/wasm\n/m) ?? assert.fail("missing tracked WASM restore");
  assert.ok(start >= (restore.index ?? assert.fail("missing restore position")) + restore[0].length,
    "Harness packaging must start after the complete tracked WASM restore");
  assert.ok(end > start);
  const commands = source.slice(start, end);
  const helpers = source.slice(source.indexOf("background() {"), source.indexOf("native_binding() {"))
    .replace("finish() {", "fixture_finish() {");
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
  case "$1" in
    scripts/check-harness-package.sh)
      [[ "$#" == 2 && "$2" == "$SDK_ARTIFACT_DIR/packages/harness" ]]
      touch "$SDK_TEMP_DIR/harness-started"
      while [[ ! -f "$SDK_TEMP_DIR/generated-done" || ! -f "$SDK_TEMP_DIR/finish-entered" ]]; do sleep 0.01; done
      if (( ${generatedStatus} == 0 )); then
        if (( ${harnessStatus} != 0 )); then
          while [[ ! -f "$SDK_TEMP_DIR/filesystem-started" ]]; do sleep 0.01; done
          [[ ! -f "$SDK_TEMP_DIR/filesystem-done" ]]
        else
          while [[ ! -f "$SDK_TEMP_DIR/filesystem-done" ]]; do sleep 0.01; done
        fi
      fi
      echo harness-complete
      touch "$SDK_TEMP_DIR/harness-done"
      return ${harnessStatus} ;;
    scripts/check-filesystem-package.sh)
      [[ "$#" == 2 && "$2" == "$SDK_ARTIFACT_DIR/packages/filesystem" ]]
      [[ -f "$SDK_TEMP_DIR/generated-done" && ! -f "$SDK_TEMP_DIR/harness-done" ]]
      touch "$SDK_TEMP_DIR/filesystem-started"
      if (( ${harnessStatus} != 0 )); then
        while [[ ! -f "$SDK_TEMP_DIR/filesystem-join-entered" ]]; do sleep 0.01; done
        [[ -f "$SDK_TEMP_DIR/harness-done" ]]
      fi
      echo filesystem-complete
      touch "$SDK_TEMP_DIR/filesystem-done"
      return ${filesystemStatus} ;;
    scripts/check-typescript-packages.sh)
      [[ "$#" == 6 && "$2" == "$SDK_ARTIFACT_DIR/packages/typescript" ]]
      [[ "$3" == "$SDK_ARTIFACT_DIR/packages/inference/acyclic-inference.tgz" ]]
      [[ "$4" == "$SDK_ARTIFACT_DIR/packages/filesystem/acyclic-fs.tgz" ]]
      [[ "$5" == "$SDK_ARTIFACT_DIR/packages/harness/acyclic-harness.tgz" ]]
      [[ "$6" == "$SDK_ARTIFACT_DIR/packages/BUILD.json" ]]
      [[ -f "$SDK_TEMP_DIR/harness-joined" && -f "$SDK_TEMP_DIR/filesystem-done" && -f "$SDK_TEMP_DIR/consumer-receipt" ]]
      echo consumer-complete ;;
    *) return 2 ;;
  esac
}
bun() {
  if [[ "$1" == scripts/run-harness-conformance.mjs ]]; then
    [[ "$#" == 4 && -f "$SDK_TEMP_DIR/harness-joined" ]]
    return 0
  fi
  [[ "$#" == 2 && "$1" == run && "$2" == check:generated ]]
  while [[ ! -f "$SDK_TEMP_DIR/harness-started" ]]; do sleep 0.01; done
  [[ ! -f "$SDK_TEMP_DIR/harness-done" ]]
  echo generated-complete
  touch "$SDK_TEMP_DIR/generated-done"
  return ${generatedStatus}
}
node() {
  [[ "$#" == 4 && "$1" == scripts/typescript-qualification.mjs && "$2" == consumer ]]
  [[ "$4" == "$SDK_ARTIFACT_DIR/packages/BUILD.json" ]]
  [[ -f "$SDK_TEMP_DIR/harness-joined" && -f "$SDK_TEMP_DIR/filesystem-done" ]]
  touch "$SDK_TEMP_DIR/consumer-receipt"
}
${helpers}
finish() {
  touch "$SDK_TEMP_DIR/finish-entered"
  if [[ "$1" == filesystem-package && ${harnessStatus} != 0 ]]; then
    [[ -f "$SDK_TEMP_DIR/filesystem-started" && ! -f "$SDK_TEMP_DIR/filesystem-done" ]]
    touch "$SDK_TEMP_DIR/filesystem-join-entered"
  fi
  local status=0
  fixture_finish "$@" || status=$?
  if [[ "$1" == harness-package && "$status" == 0 ]]; then touch "$SDK_TEMP_DIR/harness-joined"; fi
  return "$status"
}
${commands}
[[ -f "$SDK_TEMP_DIR/harness-done" && -f "$SDK_TEMP_DIR/generated-done" ]]
echo conformance-ready
`], {
        encoding: "utf8", timeout: 60000,
        env: { ...process.env, SDK_OVERLAP_FIXTURE: directory.replaceAll("\\", "/") },
      });
      assert.equal(result.error, undefined);
      assert.equal(result.status, harnessStatus || generatedStatus || filesystemStatus, result.stderr);
      assert.match(result.stdout, /generated-complete/);
      assert.match(result.stdout, /harness-complete/);
      assert.equal(result.stdout.includes("conformance-ready"), harnessStatus === 0 && generatedStatus === 0 && filesystemStatus === 0);
      assert.equal(result.stdout.includes("harness-package failed"), harnessStatus !== 0);
      assert.equal(result.stdout.includes("filesystem-complete"), generatedStatus === 0);
      assert.equal(result.stdout.includes("filesystem-package failed"), generatedStatus === 0 && filesystemStatus !== 0);
      assert.equal(result.stdout.includes("consumer-complete"), harnessStatus === 0 && generatedStatus === 0 && filesystemStatus === 0);
    } finally {
      assert.ok(resolve(directory).startsWith(resolve(".tools") + sep));
      rmSync(directory, { recursive: true, force: true });
    }
  }
});
