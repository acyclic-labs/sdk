import assert from "node:assert/strict";
import { copyFile, mkdir, mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import test from "node:test";
import { Processes } from "./process.mjs";

if (process.platform === "win32") test("external command transport preserves the real Windows shell argv", async () => {
  const processes = new Processes();
  try {
    // Captured real failure: the former C# process host applied CRT quoting
    // again and cmd.exe looked for a command literally named '\"ver /?'.
    // Provenance: windows.plugins.md, 2026-10-25 17:34Z; raw diagnostic
    // 819b977aeea64706b8c98a59b794d70e.log. Capture the current OS reference
    // instead of inventing shell help text or assuming its exit status.
    for (const argv of [["/d", "/s", "/c", "ver /?"], ["/d", "/s", "/c", "ver", "/?"]]) {
      const reference = await processes.run(process.env.ComSpec, argv);
      assert.ok(reference.stdout.length > 0, "step=capture native shell help; recovery=cmd.exe /d /s /c ver /?");
      assert.equal(reference.stderr, "", "step=capture native shell help; recovery=cmd.exe /d /s /c ver /?");
      const owned = await processes.runOwned(process.env.ComSpec, argv);
      assert.deepEqual(owned, reference,
        "step=shared Windows command-shell transport; recovery=node --test --test-concurrency=1 scripts/graphcoder-e2e/command.test.mjs");
    }
  } finally { await processes.close(); }
});

if (process.platform === "win32") test("external command transport runs real npm shims in literal Windows paths", async () => {
  const root = await mkdtemp(join(tmpdir(), "npm shim "));
  const processes = new Processes();
  const recovery = "node --test --test-reporter=tap --test-concurrency=1 scripts/graphcoder-e2e/command.test.mjs";
  try {
    const where = join(process.env.SystemRoot, "System32", "where.exe");
    await copyFile(where, join(root, "where.exe"));
    const shim = join(root, "acyclic-transport.cmd");
    // Exact output captured from real npm cmd-shim 7.0.0; provenance is next
    // to the fixture. The executable and query output come from this OS.
    await copyFile(new URL("./fixtures/npm-command.cmd", import.meta.url), shim);
    const literal = join(root, "argument &%");
    await mkdir(literal);
    await copyFile(where, join(literal, "where.exe"));
    const argv = ["/R", literal, "*.exe"];
    const literalReference = await processes.run(where, argv);
    assert.equal(literalReference.code, 0, `step=capture literal Windows path reference; recovery=${recovery}`);
    assert.ok(literalReference.stdout.length > 0, `step=capture literal Windows path reference; recovery=${recovery}`);
    const literalOwned = await processes.runOwned(shim, argv);
    assert.deepEqual(literalOwned, literalReference, `step=literal npm cmd arguments; recovery=${recovery}`);
    const options = { cwd: literal, env: { ...process.env, PATH: `${root};${process.env.PATH}` } };
    const reference = await processes.run(where, ["where.exe"], options);
    assert.equal(reference.code, 0, `step=capture PATH Windows reference; recovery=${recovery}`);
    assert.ok(reference.stdout.length > 0, `step=capture PATH Windows reference; recovery=${recovery}`);
    const throughPath = await processes.runOwned(process.env.ComSpec, ["/d", "/s", "/c", "acyclic-transport where.exe"], options);
    assert.deepEqual(throughPath, reference, `step=PATH npm cmd shim; recovery=${recovery}`);
  } finally {
    await processes.close();
    await rm(root, { recursive: true });
  }
});
