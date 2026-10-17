import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { spawnSync } from "node:child_process";
import { STOPPED, TOPOLOGIES, phaseRecords, storageBucket, keptRun, dependencyDirectories, endpoints, logStdio, parseArgs, requireStateDir, verdict, within, wrapperPath, wrapperSource } from "./local-cloud.mjs";

const topology = {
  ca_file: "/dev/shm/cloud-e2e-x/ca.pem",
  edge: { streams: "10.0.0.20:8446", objects: "10.0.0.10:443", actors: "10.0.0.30:443", fs: "10.0.0.40:8456" },
  bearers: { streams: "stream-bearer", objects: "object-bearer", actors: "actor-bearer", "actors-migration": "migration-bearer", fs: "fs-bearer" },
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

test("the full topology also projects Filesystem and Actors, and requires both", () => {
  const full = endpoints(topology, TOPOLOGIES.full.products);
  assert.deepEqual(Object.keys(full), ["version", "streams", "objects", "fs", "actors"]);
  assert.deepEqual(full.fs, { endpoint: "https://10.0.0.40:8456", bearer: "fs-bearer", ca_file: topology.ca_file });
  assert.equal(full.actors.migration_bearer, "migration-bearer");
  const { "actors-migration": omitted, ...bearers } = topology.bearers;
  assert.throws(() => endpoints({ ...topology, bearers }, TOPOLOGIES.full.products), /no actors-migration bearer/);
  const { fs: _, ...edge } = topology.edge;
  assert.throws(() => endpoints({ ...topology, edge }, TOPOLOGIES.full.products), /no fs Edge/);
  // The identity topology does not serve Filesystem, so it never requires it.
  assert.equal(endpoints({ ...topology, edge }).fs, undefined);
});

test("Filesystem storage is the one bucket every Filesystem Host names", () => {
  assert.equal(storageBucket([{ objects_bucket: "e2e" }, { objects_bucket: "e2e" }]), "e2e");
  assert.throws(() => storageBucket([{ objects_bucket: "e2e" }, { objects_bucket: "other" }]), /2 storage buckets/);
  assert.throws(() => storageBucket([]), /0 storage buckets/);
  assert.throws(() => storageBucket([{}]), /1 storage buckets/);
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

test("a stopping topology passes only for the stop, its aggregate failure and a passing teardown", () => {
  const phase = (name, passed, code) => JSON.stringify({ operation: "local.e2e.phase", phase: name, passed,
    last_error: passed ? null : { message: `hooked command failed: ${JSON.stringify({ code, signal: null })}` } });
  const log = (...lines) => phaseRecords(["not json", "{broken", ...lines].join("\n"));
  const full = TOPOLOGIES.full, ok = { code: 0 };
  // The gate's real sequence after the wrapper stops it.
  const before = [phase("readiness.identity", true)];
  const stopAt = phase("checks.worker_url", false, STOPPED);
  const teardown = ["teardown.actors", "teardown.hosts", "teardown.provider", "teardown", "teardown.release"].map(name => phase(name, true));
  const total = phase("total", false, 1);
  const stopped = log(...before, stopAt, ...teardown, total);
  assert.deepEqual(verdict(1, ok, full, stopped), { passed: true });
  assert.match(verdict(1, { code: 3 }, full, stopped).reason, /exited 3/);
  assert.match(verdict(0, ok, full, stopped).reason, /it passed/);
  const rejects = (records, pattern) => assert.match(verdict(1, ok, full, records).reason, pattern);
  // The test passed, but the hooked command failed for another reason.
  rejects(log(...before, phase("checks.worker_url", false, 71), ...teardown, total), /did not exit 75/);
  rejects(log(...before, phase("checks.worker_url", false, STOPPED * 10), ...teardown, total), /did not exit 75/);
  // An earlier failure, so the stop is not the gate's only failure.
  rejects(log(phase("readiness.identity", false, STOPPED), stopAt, ...teardown, total), /are not \[checks.worker_url, total\]/);
  // A later failure: the stop's teardown failed.
  const failedTeardown = teardown.map(line => JSON.parse(line).phase === "teardown.hosts" ? phase("teardown.hosts", false, 1) : line);
  rejects(log(...before, stopAt, ...failedTeardown, total), /\[checks.worker_url, teardown.hosts, total\]/);
  rejects(log(...before, stopAt, ...teardown.slice(0, 3), phase("teardown", false, 1), phase("teardown.release", true), total), /teardown, total\]/);
  // A phase without a verdict is not a pass.
  rejects(log(...before, stopAt, ...teardown, JSON.stringify({ operation: "local.e2e.phase", phase: "extra" }), total), /extra/);
  // A truncated log: nothing after the stop, or teardown without the final total.
  rejects(log(...before, stopAt), /are not \[checks.worker_url, total\]/);
  rejects(log(...before, stopAt, ...teardown), /are not \[checks.worker_url, total\]/);
  // An incomplete teardown, or a record after the aggregate.
  rejects(log(...before, stopAt, ...teardown.slice(0, 3), total), /\[teardown, teardown.release\] were not reported/);
  rejects(log(...before, stopAt, ...teardown, total, phase("teardown.release", true)), /not the gate's last phase/);
  // Only the one gate-created run directory a failed gate kept is released.
  assert.equal(keptRun("x\nrun directory: /dev/shm/cloud-e2e-AgCqlP\n"), "/dev/shm/cloud-e2e-AgCqlP");
  assert.equal(keptRun("run directory: /dev/shm/cloud-e2e-AgCqlP\nretained run custody: /dev/shm/cloud-e2e-AgCqlP\n"), undefined);
  assert.equal(keptRun("run directory: /dev/shm/cloud-e2e-AgCqlP/..\n"), undefined);
  assert.equal(keptRun("run directory: /home/var/cloud-e2e-AgCqlP\n"), undefined);
  assert.equal(keptRun("run directory: /dev/shm/cloud-e2e-AAAAAA\nrun directory: /dev/shm/cloud-e2e-BBBBBB\n"), undefined);
  // The identity topology never tolerates a gate failure, stop or not.
  assert.match(verdict(1, ok, TOPOLOGIES.identity, stopped).reason, /gate failed/);
});

test("arguments are absolute and a test command is required", () => {
  const argv = ["--cloud", "/c", "--binaries", "/b", "--out", "/o", "--", "/t", "--ignored"];
  assert.deepEqual(parseArgs(argv), { cloud: "/c", binaries: "/b", out: "/o", topology: "identity", test: ["/t", "--ignored"] });
  assert.equal(parseArgs(["--topology", "full", ...argv]).topology, "full");
  assert.throws(() => parseArgs(["--topology", "toString", ...argv]), /unknown topology/);
  assert.throws(() => parseArgs(argv.slice(0, 6)), /test command/);
  assert.throws(() => parseArgs(["--cloud", "c", ...argv.slice(2)]), /absolute/);
  assert.throws(() => parseArgs(argv.slice(2)), /--cloud is required/);
});

test("the wrapper hooks only the topology's one command and embeds only plain paths", () => {
  const source = wrapperSource();
  assert.match(source, /strcmp\(argv\[1\], "identity-check"\)/);
  assert.match(source, /execv\(REAL, argv\)/);
  const full = wrapperSource(TOPOLOGIES.full);
  assert.doesNotMatch(full, /identity-check/);
  assert.match(full, /"worker-ingress"/);
  // The full topology stops the gate after a passing test, before the real command.
  const stop = full.indexOf(`return ${STOPPED};`);
  assert.ok(stop > full.indexOf("return 71;") && stop < full.indexOf("execv"));
  assert.equal(source.includes(`return ${STOPPED};`), false);
  assert.throws(() => wrapperSource({ hooked: 'x"); system("y', replace: false }), /unsupported hooked/);
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
