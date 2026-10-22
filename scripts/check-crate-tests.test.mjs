import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join, resolve, sep } from "node:path";
import test from "node:test";
import { checkCrateTests, workspacePackages } from "./check-crate-tests.mjs";

test("checks every workspace member alone, reusing one target with bounded workers", async () => {
  const packages = workspacePackages({
    workspace_members: ["c", "a", "b"],
    packages: [
      { id: "a", name: "first" }, { id: "external", name: "external" },
      { id: "b", name: "second" }, { id: "c", name: "third" },
    ],
  });
  assert.deepEqual(packages, ["first", "second", "third"]);
  let active = 0, peak = 0;
  const commands = [], buildDirectories = [];
  const failures = await checkCrateTests(packages, 2, "/shared target", async (args, buildDirectory) => {
    commands.push(args);
    buildDirectories.push(buildDirectory);
    peak = Math.max(peak, ++active);
    if (args.includes("first")) await new Promise(resolve => setImmediate(resolve));
    --active;
    return args.includes("second") ? 101 : 0;
  });
  assert.equal(peak, 2);
  assert.deepEqual(failures, ["second"]);
  assert.deepEqual(commands, packages.map(name =>
    ["check", "--locked", "--tests", "-p", name, "--target-dir", "/shared target"]));
  assert.deepEqual(buildDirectories, [0, 1, 0].map(index => resolve("/shared target", "crate-test-checks", String(index))));
});

test("real Cargo compilations overlap; restoring the shared build lock is rejected", async () => {
  mkdirSync(".tools", { recursive: true });
  const directory = resolve(mkdtempSync(".tools/crate-check-"));
  try {
    for (const shared of [false, true]) {
      const fixture = join(directory, shared ? "shared" : "parallel");
      const barrier = join(fixture, "barrier");
      mkdirSync(barrier, { recursive: true });
      writeFileSync(join(fixture, "Cargo.toml"), '[workspace]\nmembers = ["first", "second"]\nresolver = "2"\n');
      writeFileSync(join(fixture, "Cargo.lock"), 'version = 4\n\n[[package]]\nname = "first"\nversion = "0.0.0"\n\n[[package]]\nname = "second"\nversion = "0.0.0"\n');
      for (const name of ["first", "second"]) {
        const crate = join(fixture, name);
        mkdirSync(join(crate, "src"), { recursive: true });
        writeFileSync(join(crate, "Cargo.toml"), `[package]\nname = "${name}"\nversion = "0.0.0"\nedition = "2024"\n`);
        writeFileSync(join(crate, "src/lib.rs"), "");
        writeFileSync(join(crate, "build.rs"), `fn main() {
    let barrier = std::path::PathBuf::from(std::env::var_os("COMPILE_BARRIER").unwrap());
    std::fs::write(barrier.join("${name}"), b"ready").unwrap();
    let started = std::time::Instant::now();
    while !barrier.join("${name === "first" ? "second" : "first"}").exists() {
        assert!(started.elapsed() < std::time::Duration::from_secs(2), "compilations did not overlap");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}
`);
      }
      let output = "";
      const target = join(fixture, "target");
      const failures = await checkCrateTests(["first", "second"], 2, target, async (args, buildDirectory) => {
        const child = spawn("cargo", args, {
          cwd: fixture, windowsHide: true, stdio: ["ignore", "pipe", "pipe"],
          env: { ...process.env, COMPILE_BARRIER: barrier, CODEX_HOME: join(fixture, "codex"),
            CARGO_BUILD_BUILD_DIR: shared ? target : buildDirectory },
        });
        child.stdout.on("data", chunk => { output += chunk; });
        child.stderr.on("data", chunk => { output += chunk; });
        return await new Promise((resolve, reject) => child.on("error", reject).on("close", resolve));
      });
      if (shared) {
        assert.ok(failures.length > 0, "the original shared lock must prevent overlap");
        assert.match(output, /compilations did not overlap/);
      } else {
        assert.deepEqual(failures, [], output);
      }
    }
  } finally {
    assert.equal(dirname(directory), resolve(".tools"));
    assert.ok(directory.startsWith(resolve(".tools") + sep));
    rmSync(directory, { recursive: true, force: true });
  }
});

test("feature unification cannot hide an individual failure", async () => {
  const run = async args => args.filter(arg => arg === "-p").length === 1 ? 101 : 0;
  assert.deepEqual(await checkCrateTests(["first", "second"], 2, "target", run), ["first", "second"]);
  // Negative control: combining the roots hides both failures in this fixture.
  assert.equal(await run(["check", "--tests", "-p", "first", "-p", "second"]), 0);
});

test("qualification gate runs the standalone checks and their scheduling control", () => {
  const source = readFileSync(new URL("./qualify-ci.sh", import.meta.url), "utf8").replaceAll("\r\n", "\n");
  const commands = ["node --test scripts/check-crate-tests.test.mjs", "node scripts/check-crate-tests.mjs 2"];
  const verify = text => {
    const gate = text.match(/^  gate\)\n([\s\S]*?)^    ;;$/m)?.[1] ?? assert.fail("missing gate branch");
    for (const command of commands) assert.ok(gate.includes(command), `gate must run ${command}`);
  };
  verify(source);
  // Moving either command to another lane must not satisfy the gate assertion.
  for (const command of commands) {
    const moved = source.replace(`    ${command}\n`, "").replace("  docs)\n", `  docs)\n    ${command}\n`);
    assert.throws(() => verify(moved), /gate must run/);
  }
});
