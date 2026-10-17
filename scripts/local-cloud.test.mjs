import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { spawnSync } from "node:child_process";
import { dependencyDirectories, endpoints, logStdio, parseArgs, requireStateDir, verdict, within, wrapperPath, wrapperSource } from "./local-cloud.mjs";

const topology = {
  ca_file: "/dev/shm/cloud-e2e-x/ca.pem",
  edge: { streams: "10.0.0.20:8446", objects: "10.0.0.10:443", actors: "10.0.0.30:443" },
  bearers: { streams: "stream-bearer", objects: "object-bearer", actors: "actor-bearer" },
};

test("endpoints project only the gate's Edge addresses, CA and locally minted bearers", () => {
  assert.deepEqual(endpoints(topology), {
    version: 1,
    streams: { endpoint: "https://10.0.0.20:8446", bearer: "stream-bearer", ca_file: topology.ca_file },
    objects: { endpoint: "https://10.0.0.10:443", bearer: "object-bearer", ca_file: topology.ca_file },
  });
  assert.throws(() => endpoints({ ...topology, bearers: { objects: "o" } }), /no streams bearer/);
  assert.throws(() => endpoints({ ...topology, edge: { streams: "s" } }), /no objects Edge/);
  assert.throws(() => endpoints({ ...topology, ca_file: "ca.pem" }), /absolute ca_file/);
});

test("operator state is refused at or below the shared .cloudctl, through any alias", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "local-cloud-state-"));
  try {
    const shared = path.join(root, "cloud", ".cloudctl");
    fs.mkdirSync(shared, { recursive: true });
    const out = path.join(root, "out");
    assert.equal(requireStateDir(path.join(out, "state"), shared), path.join(out, "state"));
    assert.equal(requireStateDir(path.join(root, "cloud", "..cloudctl"), shared), path.join(root, "cloud", "..cloudctl"));
    assert.throws(() => requireStateDir(undefined, shared), /absolute/);
    assert.throws(() => requireStateDir("state", shared), /absolute/);
    assert.throws(() => requireStateDir(shared, shared), /shared operator state/);
    assert.throws(() => requireStateDir(path.join(shared, "bin"), shared), /shared operator state/);
    // A child whose name merely starts with ".." is still below the shared state.
    assert.throws(() => requireStateDir(path.join(shared, "..cache"), shared), /shared operator state/);
    // A lane path that aliases the shared state is the shared state.
    const alias = path.join(root, "lane");
    fs.symlinkSync(shared, alias, process.platform === "win32" ? "junction" : "dir");
    assert.throws(() => requireStateDir(path.join(alias, "state"), shared), /shared operator state/);
    assert.equal(within(path.join(alias, "a", "b"), shared), true);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("only untracked dependency directories are linked into the snapshot", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "local-cloud-deps-"));
  try {
    for (const directory of ["node_modules/pg", "svc/workers/node_modules/x", ".git/node_modules", "svc/fs"]) {
      fs.mkdirSync(path.join(root, directory), { recursive: true });
    }
    assert.deepEqual(dependencyDirectories(root).sort(), ["node_modules", path.join("svc", "workers", "node_modules")]);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("a passing gate is not a pass unless the test command ran and passed", () => {
  assert.deepEqual(verdict(0, { code: 0, signal: null }), { passed: true });
  assert.match(verdict(0, undefined).reason, /never ran/);
  assert.match(verdict(1, { code: 0 }).reason, /gate failed/);
  assert.match(verdict(0, { code: 101 }).reason, /exited 101/);
  assert.match(verdict(0, { code: null, signal: "SIGKILL" }).reason, /exited SIGKILL/);
});

test("arguments are absolute and a test command is required", () => {
  const argv = ["--cloud", "/c", "--binaries", "/b", "--out", "/o", "--", "/t", "--ignored"];
  assert.deepEqual(parseArgs(argv), { cloud: "/c", binaries: "/b", out: "/o", test: ["/t", "--ignored"] });
  assert.throws(() => parseArgs(argv.slice(0, 6)), /test command/);
  assert.throws(() => parseArgs(["--cloud", "c", ...argv.slice(2)]), /absolute/);
  assert.throws(() => parseArgs(argv.slice(2)), /--cloud is required/);
});

test("the wrapper hooks only the gate's last command and embeds only plain paths", () => {
  const source = wrapperSource();
  assert.match(source, /strcmp\(argv\[1\], "identity-check"\)/);
  assert.match(source, /execv\(REAL, argv\)/);
  assert.equal(wrapperPath("/work/target/debug/cloud-e2e"), "/work/target/debug/cloud-e2e");
  assert.throws(() => wrapperPath('/tmp/a"b'), /unsupported/);
  assert.throws(() => wrapperPath("relative/cloud-e2e"), /unsupported/);
});

test("one log descriptor keeps interleaved stdout and stderr lines", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "local-cloud-log-"));
  const expected = "o0\ne0\no1\ne1\no2\ne2\no3\ne3\n";
  // No double quotes or backslashes: Windows argument quoting would rewrite them.
  const writer = "const fs = require('node:fs'), end = String.fromCharCode(10); for (let i = 0; i < 4; i++) { fs.writeSync(1, 'o' + i + end); fs.writeSync(2, 'e' + i + end); }";
  const run = (stdio) => {
    const result = spawnSync(process.execPath, ["-e", writer], { stdio });
    for (const fd of new Set(stdio.slice(1))) fs.closeSync(fd);
    assert.equal(result.status, 0);
  };
  try {
    const shared = path.join(dir, "shared.log");
    run(logStdio(shared));
    assert.equal(fs.readFileSync(shared, "utf8"), expected);
    // Negative control: two opens of one file lose lines to overwrites.
    const split = path.join(dir, "split.log");
    run(["ignore", fs.openSync(split, "w"), fs.openSync(split, "a")]);
    assert.notEqual(fs.readFileSync(split, "utf8"), expected);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
