import assert from "node:assert/strict";
import { cp, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import test from "node:test";
import { Processes } from "./process.mjs";

test("headless selection owns no terminal dependency and PTY imports require it", async () => {
  const root = await mkdtemp(join(tmpdir(), "headless-dependencies-"));
  const processes = new Processes();
  try {
    await Promise.all(["graphcoder-e2e", "graphcoder-pty"].map(name =>
      cp(fileURLToPath(new URL(`../${name}`, import.meta.url)), join(root, name), {
        recursive: true, filter: path => !["node_modules", "__pycache__"].includes(basename(path)),
      })));
    // Negative control: the dependency is genuinely absent, and a real PTY
    // import must fail instead of silently substituting a different renderer.
    const terminal = await processes.run(process.execPath, ["--input-type=module", "-e",
      `await import(${JSON.stringify(pathToFileURL(join(root, "graphcoder-e2e/pty.mjs")).href)})`]);
    assert.equal(terminal.code, 1);
    assert.match(terminal.stderr, /ERR_MODULE_NOT_FOUND/);
    assert.match(terminal.stderr, /@xterm\/headless/);

    const headless = await processes.run(process.execPath, [join(root, "graphcoder-e2e/run.mjs"),
      "missing-binary", "missing-mock", "misspelled-case"]);
    assert.equal(headless.code, 1);
    assert.match(headless.stderr, /unknown case misspelled-case/);
    assert.doesNotMatch(headless.stderr, /ERR_MODULE_NOT_FOUND|ENOENT/);
  } finally {
    await processes.close();
    await rm(root, { recursive: true, force: true });
  }
});
