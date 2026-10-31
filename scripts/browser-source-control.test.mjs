import assert from "node:assert/strict";
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, relative, resolve, sep } from "node:path";
import test from "node:test";
import { browserTest, BrowserSourceEnded } from "./browser-test.mjs";

test("actual Chromium preserves argument delivery and JavaScript failure", async () => {
  const root = resolve(import.meta.dirname, "..");
  const target = join(root, "target");
  await mkdir(target, { recursive: true });
  const directory = await mkdtemp(join(target, "browser-source-"));
  const entry = join(directory, "entry.mjs");
  // Browser workload, not a captured or fabricated CDP response.
  await writeFile(entry, 'export async function test(value) { if (value.fail) throw new Error("original browser workload failure"); return value; }\n');
  const module = relative(root, entry).split(sep).join("/");
  try {
    const value = { source: "original page λ", values: [1, null, "quoted ' argument"] };
    assert.deepEqual(await browserTest(module, value), value);
    await assert.rejects(browserTest(module, { fail: true }), /original browser workload failure/);
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test("native CDP pipe loss ends a wait while its actual producer stays alive", { skip: process.platform === "win32" }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "browser-pipe-loss-"));
  const child = join(directory, "producer.mjs");
  const retired = join(directory, "retired");
  // A one-shot physical fault: close the original output pipe and retain the
  // original input read. It emits no CDP reply and has no timer or status probe.
  await writeFile(child, `#!/usr/bin/env node
import { closeSync, writeFileSync } from "node:fs";
import { Socket } from "node:net";
process.on("SIGTERM", () => { writeFileSync(${JSON.stringify(retired)}, "retired after pipe loss"); process.exit(0); });
closeSync(4);
new Socket({ fd: 3, readable: true, writable: false }).resume();
`);
  await chmod(child, 0o700);
  const original = process.env.CHROME;
  process.env.CHROME = child;
  try {
    await assert.rejects(browserTest("unused-after-source-loss.mjs", {}), error => {
      assert.ok(error instanceof BrowserSourceEnded);
      assert.equal(error.code, "BROWSER_SOURCE_ENDED");
      assert.equal(error.source, "pipe");
      assert.match(error.message, /step=browser-pipe.*recovery command:/);
      return true;
    });
    assert.equal(await readFile(retired, "utf8"), "retired after pipe loss");
  } finally {
    if (original === undefined) delete process.env.CHROME;
    else process.env.CHROME = original;
    await rm(directory, { recursive: true, force: true });
  }
});
