import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join, resolve, sep } from "node:path";
import { Writable } from "node:stream";
import test from "node:test";
import { replayLog } from "./ci-background-log.mjs";

test("background log replay preserves all bytes and annotations under stdout and stderr backpressure", async () => {
  mkdirSync(".tools", { recursive: true });
  const directory = mkdtempSync(".tools/ci-log-");
  const path = join(directory, "output.log");
  const lines = Array.from({ length: 2000 }, (_, index) => `${index}: passed ${"λ".repeat(40)}`);
  const text = lines.join("\n") + "\n";
  writeFileSync(path, text);
  try {
    for (const status of [0, 13]) {
      const output = [], errors = [];
      const slow = chunks => new Writable({ highWaterMark: 1, write(chunk, encoding, callback) {
        chunks.push(Buffer.from(chunk)); setImmediate(callback);
      } });
      const stdout = slow(output), stderr = slow(errors);
      await replayLog(path, "fixture", status, stdout, stderr);
      assert.equal(Buffer.concat(output).toString(), `::group::fixture\n${text}::endgroup::\n` +
        (status ? "::error::fixture failed with exit status 13\n" : ""));
      assert.equal(Buffer.concat(errors).toString(), status ? lines.slice(-80).join("\n") + "\n" : "");
      assert.equal(stdout.writableEnded, false);
      assert.equal(stderr.writableEnded, false);
    }
  } finally {
    assert.ok(resolve(directory).startsWith(resolve(".tools") + sep));
    rmSync(directory, { recursive: true, force: true });
  }
});

test("finish joins and reports every child even when cat cannot replay a log", () => {
  const source = readFileSync("scripts/qualify-ci.sh", "utf8").replaceAll("\r\n", "\n");
  const helpers = source.slice(source.indexOf("background() {"), source.indexOf("native_binding() {"));
  const bash = process.platform === "win32" ? join(process.env.ProgramFiles ?? "C:/Program Files", "Git", "bin", "bash.exe") : "bash";
  mkdirSync(".tools", { recursive: true });
  for (const { status, missingLog, expected } of [
    { status: 0, missingLog: false, expected: 0 }, { status: 13, missingLog: false, expected: 13 },
    { status: 0, missingLog: true, expected: 1 }, { status: 13, missingLog: true, expected: 13 },
  ]) {
    const directory = mkdtempSync(".tools/ci-join-");
    try {
      const result = spawnSync(bash, ["-c", `set -euo pipefail
SDK_TEMP_DIR="$SDK_LOG_FIXTURE"
cat() {
  if [[ "$1" == *.log ]]; then echo 'cat: write error: Resource temporarily unavailable' >&2; return 1; fi
  command cat "$@"
}
fixture_child() {
  echo "child-$1-complete"
  if [[ "$1" == first && ${String(missingLog)} == true ]]; then rm "$SDK_TEMP_DIR/background-first.log"; fi
  return "$2"
}
${helpers}
background first fixture_child first ${status}
background second fixture_child second 0
result=0
finish first second || result=$?
printf 'result=%s\\n' "$result"
`], { encoding: "utf8", timeout: 60000, env: { ...process.env, SDK_LOG_FIXTURE: directory.replaceAll("\\", "/") } });
      assert.equal(result.error, undefined);
      assert.equal(result.status, 0, result.stderr);
      assert.equal(result.stdout.includes("child-first-complete"), !missingLog);
      assert.match(result.stdout, /child-second-complete/);
      assert.ok(result.stdout.includes(`result=${expected}`));
      if (missingLog) assert.match(result.stderr, /ENOENT/);
      assert.doesNotMatch(result.stderr, /Resource temporarily unavailable/);
      assert.equal(result.stdout.includes("::error::first failed with exit status 13"), status === 13 && !missingLog);
    } finally {
      assert.ok(resolve(directory).startsWith(resolve(".tools") + sep));
      rmSync(directory, { recursive: true, force: true });
    }
  }
});
