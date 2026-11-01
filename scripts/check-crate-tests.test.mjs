import assert from "node:assert/strict";
import { spawn, execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join, resolve, sep } from "node:path";
import { createServer } from "node:net";
import test from "node:test";
import { checkCrateTests, workspacePackages } from "./check-crate-tests.mjs";

test("checks original default members alone in bounded worker targets", async () => {
  // Capture the real workspace; fabricated metadata cannot establish Kani routing.
  const metadata = JSON.parse(execFileSync("cargo",
    ["metadata", "--locked", "--no-deps", "--format-version", "1"],
    { encoding: "utf8", maxBuffer: 64 * 1024 * 1024 }));
  const selected = workspacePackages(metadata);
  const ids = new Set(metadata.workspace_default_members);
  assert.deepEqual(selected, metadata.packages.filter(item => ids.has(item.id)).map(item => item.name).sort());
  assert.ok(selected.includes("acyclic-harness-fuzz"));
  assert.ok(selected.includes("acyclic-fs-fuzz"));
  assert.ok(selected.includes("acyclic-sdk-fuzz"));
  assert.ok(!selected.includes("agent-spawn-slot-proofs"));
  for (const mutate of [
    item => { delete item.workspace_default_members; },
    item => { item.workspace_default_members = []; },
    item => { item.packages = item.packages.filter(pkg => pkg.id !== item.workspace_default_members[0]); },
  ]) {
    const incomplete = structuredClone(metadata);
    mutate(incomplete);
    assert.throws(() => workspacePackages(incomplete), /STEP_FAILED workspace-default-selection/);
  }
  const packages = ["first", "second", "third"];
  let active = 0, peak = 0;
  let releaseFirst;
  const secondAdmitted = new Promise(resolve => { releaseFirst = resolve; });
  const commands = [], buildDirectories = [];
  const failures = await checkCrateTests(packages, 2, "/shared target", async (args, buildDirectory) => {
    commands.push(args);
    buildDirectories.push(buildDirectory);
    peak = Math.max(peak, ++active);
    if (args.includes("first")) await secondAdmitted;
    else releaseFirst();
    --active;
    return args.includes("second") ? 101 : 0;
  });
  assert.equal(peak, 2);
  assert.deepEqual(failures, ["second"]);
  const targets = [0, 1, 0].map(index => resolve("/shared target", "crate-test-checks", String(index)));
  assert.deepEqual(commands, packages.map((name, index) =>
    ["check", "--locked", "--tests", "-p", name, "--target-dir", targets[index]]));
  assert.deepEqual(buildDirectories, targets);
});

test("real Cargo compilations overlap; restoring the shared build lock is rejected", async () => {
  // Keep the real fixture and both build directories under the original
  // caller-selected target storage.
  const fixtureRoot = resolve(process.env.CARGO_TARGET_DIR ?? "target", "crate-test-fixtures");
  mkdirSync(fixtureRoot, { recursive: true });
  const directory = mkdtempSync(join(fixtureRoot, "crate-check-"));
  try {
    for (const shared of [false, true]) {
      const fixture = join(directory, shared ? "shared" : "parallel");
      mkdirSync(fixture, { recursive: true });
      writeFileSync(join(fixture, "Cargo.toml"), '[workspace]\nmembers = ["first", "second"]\nresolver = "2"\n');
      writeFileSync(join(fixture, "Cargo.lock"), 'version = 4\n\n[[package]]\nname = "first"\nversion = "0.0.0"\n\n[[package]]\nname = "second"\nversion = "0.0.0"\n');
      for (const name of ["first", "second"]) {
        const crate = join(fixture, name);
        mkdirSync(join(crate, "src"), { recursive: true });
        writeFileSync(join(crate, "Cargo.toml"), `[package]\nname = "${name}"\nversion = "0.0.0"\nedition = "2024"\n`);
        writeFileSync(join(crate, "src/lib.rs"), "");
        writeFileSync(join(crate, "build.rs"), `use std::io::{Read, Write};
fn main() {
    println!("cargo:rerun-if-env-changed=COMPILE_BARRIER");
    let Some(barrier) = std::env::var_os("COMPILE_BARRIER") else { return };
    let mut stream = std::net::TcpStream::connect(barrier.to_str().unwrap()).unwrap();
    stream.write_all(b"${name}\\n").unwrap();
    let mut admitted = [0];
    stream.read_exact(&mut admitted).expect("compilations did not overlap: producer closed");
    assert_eq!(admitted, [1], "compilations did not overlap: invalid admission");
}
`);
      }
      let output = "";
      let lockWait = "";
      let refusal;
      const arrivals = new Map();
      const connections = new Set();
      const refuse = error => {
        refusal ??= error;
        for (const socket of connections) socket.destroy();
      };
      // Subscribe before either real build script can publish readiness. A
      // retained refusal also ends scripts arriving after the first failure.
      const server = createServer(socket => {
        connections.add(socket);
        socket.on("error", refuse);
        socket.on("close", () => connections.delete(socket));
        if (refusal) { socket.destroy(); return; }
        let name = "";
        socket.setEncoding("utf8");
        socket.on("data", chunk => {
          name += chunk;
          if (!name.endsWith("\n")) return;
          const selected = name.trim();
          if (!["first", "second"].includes(selected) || arrivals.has(selected)) {
            refuse(new Error(`invalid original build-script arrival: ${name}`));
            return;
          }
          arrivals.set(selected, socket);
          if (arrivals.size === 2) {
            for (const ready of arrivals.values()) ready.end(Buffer.from([1]));
          }
        });
        socket.on("end", () => {
          if (arrivals.size !== 2) refuse(new Error("build-script producer ended before overlap"));
        });
      });
      server.on("error", refuse);
      await new Promise((accept, reject) => {
        server.once("error", reject);
        server.listen(0, "127.0.0.1", accept);
      });
      const address = server.address();
      assert.ok(address && typeof address !== "string");
      const barrier = `127.0.0.1:${address.port}`;
      const target = join(fixture, "target");
      const run = async (args, buildDirectory, measure) => {
        const env = { ...process.env, CODEX_HOME: join(fixture, "codex"),
          CARGO_BUILD_BUILD_DIR: shared ? target : buildDirectory };
        delete env.COMPILE_BARRIER;
        if (measure) env.COMPILE_BARRIER = barrier;
        // Negative control restores the original shared target and build lock.
        const cargoArgs = shared
          ? args.map((arg, index) => args[index - 1] === "--target-dir" ? target : arg)
          : args;
        const child = spawn("cargo", cargoArgs, {
          cwd: fixture, windowsHide: true, stdio: ["ignore", "pipe", "pipe"],
          env,
        });
        child.stdout.on("data", chunk => { output += chunk; });
        let stderr = "";
        child.stderr.setEncoding("utf8");
        child.stderr.on("data", chunk => {
          output += chunk;
          stderr += chunk;
          // This is the actual second Cargo invocation's lock-wait event,
          // not an elapsed-time inference about the held first compilation.
          if (measure && /Blocking waiting for file lock on build directory/.test(stderr)) {
            lockWait = stderr;
            refuse(new Error("original shared build lock prevented overlap"));
          }
        });
        return await new Promise(resolve => {
          child.on("error", error => { refuse(error); resolve(1); });
          child.on("close", status => {
            if (status !== 0) refuse(new Error(`original Cargo producer exited: ${status}`));
            resolve(status);
          });
        });
      };
      // Compile/link both build scripts before the barrier measures overlap.
      // The tracked environment change reruns them without rebuilding them.
      try {
        assert.deepEqual(await checkCrateTests(["first", "second"], 2, target,
          (args, buildDirectory) => run(args, buildDirectory, false)), [], output);
        const failures = await checkCrateTests(["first", "second"], 2, target,
          (args, buildDirectory) => run(args, buildDirectory, true));
        if (shared) {
          assert.ok(failures.length > 0, "the original shared lock must prevent overlap");
          assert.match(lockWait, /Blocking waiting for file lock on build directory/);
          assert.match(output, /compilations did not overlap/);
          console.log(`Original Cargo lock-wait event: ${lockWait.trim()}`);
        } else {
          assert.deepEqual(failures, [], output);
          assert.equal(arrivals.size, 2, "both original build scripts reached the same event");
        }
      } finally {
        refuse(new Error("original overlap owner closed"));
        await new Promise((accept, reject) => server.close(error => error ? reject(error) : accept()));
      }
    }
  } finally {
    assert.equal(dirname(directory), fixtureRoot);
    assert.ok(directory.startsWith(fixtureRoot + sep));
    rmSync(directory, { recursive: true, force: true });
  }
});
