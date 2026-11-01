import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { join, resolve, sep } from "node:path";
import test from "node:test";
import { runShellFixture } from "./shell-fixture.mjs";

test("Linux joins Harness packaging and generated checks before conformance, including failures", async () => {
  const source = readFileSync("scripts/qualify-ci.sh", "utf8").replaceAll("\r\n", "\n");
  const start = source.indexOf("    background harness-package bash");
  const end = source.indexOf("    background harness-conformance bun", start);
  assert.ok(start > source.indexOf("    bun run test", source.indexOf("  linux)")));
  assert.doesNotMatch(source, /git restore --worktree --[\s\S]*?generated\/wasm/,
    "packaging must retain the bindings built from the qualified source");
  assert.ok(end > start);
  const commands = source.slice(start, end);
  const helpers = source.slice(source.indexOf("background() {"), source.indexOf("native_binding() {"))
    .replace("finish() {", 'finish() {\n  printf "fixture:finish-entered\\n" >&5');
  const bash = process.platform === "win32"
    ? join(process.env.ProgramFiles ?? "C:/Program Files", "Git", "bin", "bash.exe") : "bash";
  mkdirSync(".tools", { recursive: true });
  for (const harnessStatus of [0, 13]) for (const generatedStatus of [0, 21]) {
    const directory = mkdtempSync(".tools/linux-overlap-");
    try {
      let generated = false, finishing = false, released = false;
      const result = await runShellFixture(bash, `set -euo pipefail
exec 5>&1
SDK_TEMP_DIR="$SDK_OVERLAP_FIXTURE"
SDK_ARTIFACT_DIR="$SDK_TEMP_DIR/artifacts"
bash() {
  [[ "$#" == 2 && "$1" == scripts/check-harness-package.sh ]]
  [[ "$2" == "$SDK_ARTIFACT_DIR/packages/harness" ]]
  printf 'fixture:harness-started\\n' >&5
  IFS= read -r event <&3 && [[ "$event" == generated-and-finish ]] || { echo 'step=await original generated/finish event: producer closed or invalid record; recovery command: node --test scripts/test-qualify-linux-package-overlap.mjs' >&2; return 98; }
  echo harness-complete
  touch "$SDK_TEMP_DIR/harness-done"
  return ${harnessStatus}
}
bun() {
  [[ "$#" == 2 && "$1" == run && "$2" == check:baselines ]]
  IFS= read -r event <&4 && [[ "$event" == harness-started ]] || { echo 'step=await original harness start: producer closed or invalid record; recovery command: node --test scripts/test-qualify-linux-package-overlap.mjs' >&2; return 98; }
  [[ ! -f "$SDK_TEMP_DIR/harness-done" ]]
  echo generated-complete
  touch "$SDK_TEMP_DIR/generated-done"
  printf 'fixture:generated-done\\n' >&5
  return ${generatedStatus}
}
${helpers}
${commands}
[[ -f "$SDK_TEMP_DIR/harness-done" && -f "$SDK_TEMP_DIR/generated-done" ]]
echo conformance-ready
`, {
        gates: 2, recovery: "node --test scripts/test-qualify-linux-package-overlap.mjs",
        env: { ...process.env, SDK_OVERLAP_FIXTURE: directory.replaceAll("\\", "/") },
        event(line, release) {
          if (line === "fixture:harness-started") release(1, "harness-started");
          if (line === "fixture:generated-done") generated = true;
          if (line === "fixture:finish-entered") finishing = true;
          if (generated && finishing && !released) {
            released = true;
            release(0, "generated-and-finish");
          }
        },
      });
      assert.equal(released, true, result.stderr);
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
