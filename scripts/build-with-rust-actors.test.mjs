import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { mock, test } from "bun:test";

const calls = [];
mock.module("node:child_process", () => ({
  spawnSync(command, args, options) {
    calls.push({ command, args, options });
    return { status: 1 };
  },
}));

class ProcessExit extends Error {
  constructor(code) {
    super(`process.exit(${code})`);
    this.code = code;
  }
}

let importId = 0;
async function runWrapper(args) {
  const originalArgv = process.argv;
  const originalExit = process.exit;
  process.argv = [originalArgv[0], originalArgv[1], ...args];
  process.exit = code => {
    throw new ProcessExit(code);
  };
  try {
    await import(`./build-with-rust-actors.mjs?test=${importId++}`);
  } finally {
    process.argv = originalArgv;
    process.exit = originalExit;
  }
}

function bundle(channel, version = "0.2.0") {
  const directory = mkdtempSync(join(tmpdir(), "acyclic-rust-actors-wrapper-"));
  writeFileSync(
    join(directory, "generation-manifest.json"),
    JSON.stringify({ version, tool: { channel } }),
  );
  return directory;
}

test("drift runs first and release forwards manifest provenance", async () => {
  calls.length = 0;
  const directory = bundle("release");
  try {
    await assert.rejects(
      runWrapper(["check", directory]),
      error => error instanceof ProcessExit && error.code === 1,
    );
    assert.equal(calls.length, 1);
    assert.equal(calls[0].command, "cargo");
    assert.deepEqual(calls[0].args.slice(-6), [
      "--output",
      directory,
      "--version",
      "0.2.0",
      "--channel",
      "release",
    ]);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("preview forwards the supplied Rustdoc input and does not stage after drift failure", async () => {
  calls.length = 0;
  const directory = bundle("preview");
  const rustdoc = join(directory, "rustdoc.json");
  writeFileSync(rustdoc, "{}");
  try {
    await assert.rejects(
      runWrapper(["build", directory, "--rustdoc-json", rustdoc]),
      error => error instanceof ProcessExit && error.code === 1,
    );
    assert.equal(calls.length, 1);
    assert.deepEqual(calls[0].args.slice(-8), [
      "--output",
      directory,
      "--version",
      "0.2.0",
      "--channel",
      "preview",
      "--rustdoc-json",
      rustdoc,
    ]);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("release rejects a Rustdoc override before invoking any process", async () => {
  calls.length = 0;
  const directory = bundle("release");
  try {
    await assert.rejects(
      runWrapper(["check", directory, "--rustdoc-json", join(directory, "rustdoc.json")]),
      /release generation bundles must not receive --rustdoc-json/,
    );
    assert.equal(calls.length, 0);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("preview requires an explicit Rustdoc input", async () => {
  calls.length = 0;
  const directory = bundle("preview");
  try {
    await assert.rejects(
      runWrapper(["check-generated", directory]),
      /preview generation bundles require --rustdoc-json <path>/,
    );
    assert.equal(calls.length, 0);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
