import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import test from "node:test";

const root = fileURLToPath(new URL("../../../", import.meta.url));
test("composed EOF qualification executes every original property", () => {
  const result = spawnSync(process.execPath, ["rust/proofs/qualify.mjs", "native-line-eof"], {
    cwd: root, encoding: "utf8",
  });
  assert.equal(result.status, 0, result.stdout + result.stderr);
  for (const phase of ["mutated", "restored"]) {
    const log = readFileSync(new URL(`../../../target/native-line-eof/${phase}.log`, import.meta.url), "utf8");
    for (const property of [
      "incremental_eof_matches_async_lines_at_every_split",
      "oversize_and_invalid_frames_cannot_hide_the_next_complete_line",
    ]) {
      assert.ok(log.includes(`test lines::tests::${property} ...`),
        `${phase} omitted composed property ${property}`);
    }
  }
});
