import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { withMutationCopy } from "./qualify-objects-batch-mutants.mjs";

test("mutations use a disposable copy and remove it after an oracle failure", async () => {
  const root = mkdtempSync(join(tmpdir(), "sdk-mutant-fixture-"));
  let copy;
  try {
    writeFileSync(join(root, "source.rs"), "production");
    await assert.rejects(withMutationCopy(root, async directory => {
      copy = directory;
      writeFileSync(join(directory, "source.rs"), "mutant");
      assert.equal(readFileSync(join(root, "source.rs"), "utf8"), "production");
      throw new Error("oracle failed");
    }), /oracle failed/);
    assert.equal(existsSync(copy), false);
    assert.equal(readFileSync(join(root, "source.rs"), "utf8"), "production");
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test("termination removes the mutation copy and leaves production untouched", { timeout: 5000 }, async () => {
  const root = mkdtempSync(join(tmpdir(), "sdk-mutant-fixture-"));
  const module = new URL("./qualify-objects-batch-mutants.mjs", import.meta.url).href;
  writeFileSync(join(root, "source.rs"), "production");
  const child = spawn(process.execPath, ["--input-type=module", "-e", `
    import { writeFileSync } from 'node:fs';
    import { join } from 'node:path';
    import { withMutationCopy } from ${JSON.stringify(module)};
    process.on('message', () => {});
    await withMutationCopy(process.argv[1], async (directory, signal) => {
      writeFileSync(join(directory, 'source.rs'), 'mutant');
      process.send({directory});
      await new Promise((_, reject) => signal.addEventListener('abort', () => reject(signal.reason), {once:true}));
    });
    process.disconnect();
  `, root], { stdio: ["ignore", "ignore", "pipe", "ipc"], windowsHide: true });
  let copy;
  try {
    const [{ directory }] = await once(child, "message");
    copy = directory;
    assert.equal(readFileSync(join(copy, "source.rs"), "utf8"), "mutant");
    const exited = once(child, "exit");
    child.kill("SIGTERM");
    const [code, signal] = await exited;
    assert.equal(signal, null);
    assert.equal(code, 143);
    assert.equal(existsSync(copy), false);
    assert.equal(readFileSync(join(root, "source.rs"), "utf8"), "production");
  } finally {
    child.kill("SIGKILL");
    if (copy) rmSync(copy, { recursive: true, force: true });
    rmSync(root, { recursive: true, force: true });
  }
});
