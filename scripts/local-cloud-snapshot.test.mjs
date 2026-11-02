import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { extractSnapshot } from "./local-cloud.mjs";

test("snapshot streams real committed bytes and joins failed archive and extraction children", { skip: process.platform !== "linux" }, async () => {
  const root = mkdtempSync(join(tmpdir(), "sdk-snapshot-")), source = join(root, "source");
  const git = (...args) => {
    const result = spawnSync("git", ["-C", source, ...args], { encoding: "utf8" });
    assert.equal(result.status, 0, result.stderr);
  };
  try {
    mkdirSync(source);
    git("init", "--quiet");
    const empty = join(root, "empty");
    mkdirSync(empty);
    await assert.rejects(extractSnapshot(source, empty), /step=snapshot-archive;.*recovery=git/);

    // The archive is produced by real Git, and exceeds pipe capacity. Nothing
    // reads the producer's stdout in JavaScript or retains an archive buffer.
    const bytes = Buffer.alloc(4 * 1024 * 1024, 0xa7);
    writeFileSync(join(source, "payload"), bytes);
    git("add", "payload");
    git("-c", "user.name=Snapshot test", "-c", "user.email=snapshot@example.invalid",
      "-c", "commit.gpgsign=false", "commit", "--quiet", "-m", "Capture snapshot input");
    writeFileSync(join(source, "untracked"), "not committed");
    const output = join(root, "output");
    mkdirSync(output);
    await extractSnapshot(source, output);
    assert.deepEqual(readFileSync(join(output, "payload")), bytes);
    assert.throws(() => readFileSync(join(output, "untracked")), { code: "ENOENT" });

    // Real tar exits before consuming the archive when its destination is
    // absent. The same join must retire the producer rather than hang on it.
    await assert.rejects(extractSnapshot(source, join(root, "absent")), /step=snapshot-extract;.*recovery=git/);

    const interrupted = join(root, "interrupted");
    mkdirSync(interrupted);
    const listeners = process.listenerCount("SIGTERM"), cancelled = extractSnapshot(source, interrupted);
    process.kill(process.pid, "SIGTERM");
    await assert.rejects(cancelled, /step=snapshot-cancel; termination requested;.*recovery=git/);
    assert.equal(process.listenerCount("SIGTERM"), listeners);
  } finally { rmSync(root, { recursive: true, force: true }); }
});
