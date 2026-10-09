import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import test from "node:test";

test("the isolated npm job can import its staged publisher without a checkout", async () => {
  const workflow = await readFile(new URL("../.github/workflows/publish-npm.yml", import.meta.url), "utf8");
  const start = workflow.indexOf("          cp scripts/publish-npm-packages.mjs");
  const end = workflow.indexOf("          node scripts/assemble-stream-native-package.mjs inventory", start);
  assert.ok(start >= 0 && end > start, "publication helper staging block is required");
  const stage = await mkdtemp(join(tmpdir(), "sdk-npm-staging-"));
  try {
    const bash = process.platform === "win32" ? "C:/Program Files/Git/bin/bash.exe" : "bash";
    execFileSync(bash, ["-c", `set -euo pipefail\nstage="$SDK_PUBLICATION_STAGE"\nmkdir -p "$stage/scripts" "$stage/release"\n${workflow.slice(start, end).replaceAll("\r\n", "\n")}`], {
      cwd: fileURLToPath(new URL("..", import.meta.url)),
      env: { ...process.env, SDK_PUBLICATION_STAGE: stage.replaceAll("\\", "/") },
    });
    execFileSync(process.execPath, ["--input-type=module", "-e", `await import(${JSON.stringify(pathToFileURL(join(stage, "scripts/publish-npm-packages.mjs")).href)});`], { cwd: stage });
    assert.match(workflow, /assemble-actors-native-package\.mjs inventory "\$stage\/release\/actors-native-packages\.json"/u);
    assert.match(workflow, /assemble-actors-native-package\.mjs release/u);
    assert.match(workflow, /assemble-workers-native-package\.mjs inventory "\$stage\/release\/workers-native-packages\.json"/u);
    assert.match(workflow, /assemble-workers-native-package\.mjs release/u);
    for (const family of ["actors", "workers", "stream"]) {
      const invocation = workflow.slice(workflow.indexOf(`node scripts/assemble-${family}-native-package.mjs release`)).split("\n")[1];
      assert.ok(invocation.includes('"$RUNNER_TEMP/qualified/typescript"'), `${family} must retain original qualification inputs`);
    }
  } finally { await rm(stage, { recursive: true, force: true }); }
});
