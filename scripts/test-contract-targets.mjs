import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import test from "node:test";
import { buildContractTargets, captureContractCommand, ContractTargetCollision, contractTargetOutputs, runContractTarget } from "./contract-targets.mjs";
import { fuzz as fuzzContractJson } from "./contract-targets.fuzz.mjs";

test("contract command refusals retain original native diagnostics and terminal metadata", () => {
  const root = mkdtempSync(join(tmpdir(), "sdk-contract-refusal-"));
  const previousOptions = process.env.NODE_OPTIONS;
  try {
    const producer = join(root, "producer.mjs");
    // This native producer records its actual OS refusal; no Cargo or provider
    // wire response is invented. Both public capture paths must preserve it.
    writeFileSync(producer, `import { readFileSync, writeSync } from "node:fs";
      try { readFileSync("original-missing-input"); }
      catch (error) { writeSync(1, JSON.stringify({code:error.code, syscall:error.syscall, path:error.path, message:error.message})); }
      process.exitCode = 37;
    `);
    process.env.NODE_OPTIONS = `--import=${pathToFileURL(producer).href}`;
    const original = spawnSync(process.execPath, [], { cwd: root, encoding: "utf8", stdio: ["ignore", "pipe", "inherit"] });
    assert.equal(original.status, 37);
    assert.equal(JSON.parse(original.stdout).code, "ENOENT");
    const retainsRefusal = error => {
      assert.equal(error.stdout, original.stdout);
      assert.equal(error.status, original.status);
      assert.equal(error.signal, original.signal);
      assert.equal(error.executable, process.execPath);
      assert.match(error.message, /STEP_FAILED contract-.*recovery: bun scripts\/generate-contracts\.mjs check/);
      assert.ok(error.message.includes(original.stdout));
      return true;
    };
    assert.throws(() => runContractTarget(root, process.execPath), retainsRefusal);
    // Removing diagnostic metadata must fail even with an unchanged message.
    assert.throws(() => retainsRefusal(new Error(`STEP_FAILED contract-run; recovery: bun scripts/generate-contracts.mjs check\n${original.stdout}`)));

    writeFileSync(producer, 'import { writeSync } from "node:fs"; writeSync(1, "before-signal"); process.kill(process.pid, "SIGTERM");\n');
    const signaled = spawnSync(process.execPath, [], { cwd: root, encoding: "utf8", stdio: ["ignore", "pipe", "inherit"] });
    assert.notEqual(signaled.status, 0);
    assert.throws(() => runContractTarget(root, process.execPath), error => {
      assert.equal(error.stdout, signaled.stdout);
      assert.equal(error.status, signaled.status);
      assert.equal(error.signal, signaled.signal);
      return true;
    });
    const missing = join(root, "original-missing-command");
    const retainsSpawnError = error => {
      assert.equal(error.cause.code, "ENOENT");
      assert.equal(error.cause.path, missing);
      assert.equal(error.executable, missing);
      assert.match(error.message, /STEP_FAILED contract-(run|toolchain):.*recovery:/);
      assert.match(String(error), /ENOENT/);
      return true;
    };
    assert.throws(() => runContractTarget(root, missing), retainsSpawnError);
    const report = {};
    assert.throws(() => buildContractTargets(root, [["unused", "bin", "unused"]], missing, report), retainsSpawnError);
    assert.equal(report.phase, "toolchain");
    assert.equal(typeof report.toolchain_ms, "number");
  } finally {
    if (previousOptions === undefined) delete process.env.NODE_OPTIONS;
    else process.env.NODE_OPTIONS = previousOptions;
    rmSync(root, { recursive: true, force: true });
  }
});

test("contract source lookup keeps the real Git refusal and exact selected source", () => {
  const root = mkdtempSync(join(tmpdir(), "sdk-contract-source-"));
  try {
    const repository = fileURLToPath(new URL("..", import.meta.url));
    const args = ["rev-parse", "HEAD"];
    const original = spawnSync("git", args, { cwd: repository, encoding: "utf8" });
    assert.equal(original.status, 0, original.stderr);
    assert.equal(captureContractCommand(repository, "git", args, "source"), original.stdout);
    const refused = spawnSync("git", args, { cwd: root, encoding: "utf8" });
    assert.notEqual(refused.status, 0);
    assert.throws(() => captureContractCommand(root, "git", args, "source"), error => {
      assert.equal(error.stdout, refused.stdout);
      assert.equal(error.status, refused.status);
      assert.equal(error.signal, refused.signal);
      assert.deepEqual(error.args, args);
      assert.equal(error.executable, "git");
      assert.match(error.message, /STEP_FAILED contract-source:.*recovery:/);
      return true;
    });
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test("contract outputs retire at their selected last consumer without rerunning the producer", () => {
  const root = mkdtempSync(join(tmpdir(), "sdk-contract-consumers-"));
  const previousOptions = process.env.NODE_OPTIONS;
  const parse = JSON.parse;
  try {
    // Capture a real Node executable's stdout. This program owns the test
    // protocol; it does not stand in for Cargo or a Rust artifact inventory.
    const producer = join(root, "producer.mjs");
    writeFileSync(producer, `import { appendFileSync } from "node:fs";
      appendFileSync("starts", "x");
      console.log(JSON.stringify({left: "λ".repeat(65537), right: "終", unused: "discard"}));
    `);
    process.env.NODE_OPTIONS = `--import=${pathToFileURL(producer).href}`;
    const captured = runContractTarget(root, process.execPath);
    const original = parse(captured);
    const bytes = Buffer.from(captured);
    assert.equal(fuzzContractJson(bytes), true);
    for (const at of [0, 1, Math.floor(bytes.length / 2), bytes.length - 1]) {
      const mutation = Buffer.from(bytes);
      mutation[at] ^= 255;
      fuzzContractJson(mutation);
      fuzzContractJson(bytes.subarray(0, at));
    }
    const consumers = [
      [0, undefined], [0, "left"], [0, "right"], [0, "left"],
      [1, "left"], [1, undefined],
    ];
    const exercise = (keepDeleted) => {
      const admitted = [];
      let decodes = 0;
      JSON.parse = (text, ...args) => {
        const result = parse(text, ...args);
        if (text !== captured) return result;
        decodes += 1;
        admitted.push(result);
        // The negative control restores retention on the actual decoded
        // object while allowing the exact same output and command lifecycle.
        return keepDeleted ? new Proxy(result, { deleteProperty: () => true }) : result;
      };
      const outputFor = contractTargetOutputs(root, [process.execPath, process.execPath], consumers);
      assert.equal(outputFor(0), captured); // Raw first must still permit one later decode.
      assert.equal(outputFor(0, "left"), original.left);
      const afterFirst = Object.keys(admitted[0]);
      assert.equal(outputFor(0, "right"), original.right);
      const afterRight = Object.keys(admitted[0]);
      assert.equal(outputFor(0, "left"), original.left);
      const afterLast = Object.keys(admitted[0]);
      assert.throws(() => outputFor(0, "left"), /STEP_FAILED contract-consumption:.*recovery:/);
      assert.throws(() => outputFor(2, "left"), /STEP_FAILED contract-consumption:.*recovery:/);
      assert.equal(outputFor(1, "left"), original.left); // Bundle first must preserve raw bytes.
      assert.equal(outputFor(1), captured);
      assert.throws(() => outputFor(1), /STEP_FAILED contract-consumption:.*recovery:/);
      assert.equal(decodes, 2);
      assert.equal(readFileSync(join(root, "starts"), "utf8").length, keepDeleted ? 5 : 3);
      return { afterFirst, afterRight, afterLast, afterSecond: Object.keys(admitted[1]) };
    };
    const expected = { afterFirst: ["left", "right"], afterRight: ["left"], afterLast: [], afterSecond: [] };
    assert.deepEqual(exercise(false), expected);
    assert.notDeepEqual(exercise(true), expected, "restored decoded-bundle retention must fail the independent custody counter");
    JSON.parse = parse;

    writeFileSync(producer, 'console.log("{malformed");\n');
    const malformed = contractTargetOutputs(root, [process.execPath], [[0, "left"]]);
    let originalCause;
    JSON.parse = (...args) => {
      try { return parse(...args); }
      catch (error) { originalCause = error; throw error; }
    };
    assert.throws(() => malformed(0, "left"), error => {
      assert.ok(originalCause instanceof SyntaxError);
      assert.equal(error.cause, originalCause);
      assert.match(error.message, /STEP_FAILED contract-payload: malformed bundle.*recovery:/);
      return true;
    });
    JSON.parse = parse;
    writeFileSync(producer, 'console.log(JSON.stringify("scalar"));\n');
    const scalar = contractTargetOutputs(root, [process.execPath], [[0, "left"]]);
    assert.throws(() => scalar(0, "left"), /STEP_FAILED contract-payload: missing left; recovery:/);
  } finally {
    JSON.parse = parse;
    if (previousOptions === undefined) delete process.env.NODE_OPTIONS;
    else process.env.NODE_OPTIONS = previousOptions;
    rmSync(root, { recursive: true, force: true });
  }
});

test("contract selection refuses every unknown family before Cargo or publication", () => {
  const root = mkdtempSync(join(tmpdir(), "sdk-contract-selection-"));
  try {
    for (const families of [["absent-family"], ["stream", "absent-family"], [""]]) {
      const result = spawnSync(process.execPath, [
        fileURLToPath(new URL("./generate-contracts.mjs", import.meta.url)), "check", ...families,
      ], { cwd: root, encoding: "utf8", env: {
        ...process.env, ACYCLIC_CARGO_BIN: join(root, "cargo-must-not-start"), SDK_TEMP_DIR: root,
      } });
      assert.equal(result.status, 1, result.stderr);
      assert.match(result.stderr, /STEP_FAILED contract-selection: unknown family/);
      assert.match(result.stderr, /recovery: bun scripts\/generate-contracts\.mjs check/);
      assert.equal(result.stdout, "");
      assert.equal(statSync(root).isDirectory(), true);
      assert.throws(() => statSync(join(root, "observability")), { code: "ENOENT" });
    }
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

// Exercise real Cargo freshness, feature resolution and executable discovery
// with a dependency-free workspace. No substitute artifact planner is mocked.
test("contract binaries use exact Cargo identities and current source/configuration", () => {
  const root = mkdtempSync(join(tmpdir(), "sdk-contract-targets-"));
  const targetOverride = process.env.CARGO_TARGET_DIR;
  delete process.env.CARGO_TARGET_DIR;
  const put = (path, content) => writeFileSync(join(root, path), content);
  const cargo = process.env.ACYCLIC_CARGO_BIN || "cargo";
  const command = args => {
    const result = spawnSync(cargo, args, { cwd: root, encoding: "utf8" });
    assert.equal(result.status, 0, result.error?.message ?? result.stderr);
    return result.stdout;
  };
  try {
    put("Cargo.toml", '[workspace]\nmembers = ["alpha", "beta", "dependency", "gamma"]\nresolver = "2"\n');
    // Force the artifact directory outside the default root, as CI does.
    mkdirSync(join(root, ".cargo"));
    put(".cargo/config.toml", '[build]\ntarget-dir = "configured-target"\n');
    mkdirSync(join(root, "dependency", "src"), { recursive: true });
    put("dependency/Cargo.toml", '[package]\nname = "dependency"\nversion = "0.1.0"\nedition = "2024"\n[features]\ndefault = ["propagated"]\npropagated = []\n');
    put("dependency/src/lib.rs", 'pub fn value() -> bool { cfg!(feature = "propagated") }\n');
    for (const name of ["alpha", "beta"]) {
      mkdirSync(join(root, name, "src"), { recursive: true });
      put(`${name}/Cargo.toml`, `[package]\nname = "${name}"\nversion = "0.1.0"\nedition = "2024"\n[lib]\ncrate-type = ["rlib", "cdylib"]\n[[${name === "alpha" ? "bin" : "example"}]]\nname = "contract"\npath = "src/main.rs"\nrequired-features = ["contract"]\n[features]\ndefault = ["alternate"]\nalternate = []\ncontract = []\n`);
      put(`${name}/src/lib.rs`, 'pub fn value() -> bool { cfg!(feature = "alternate") }\n');
      put(`${name}/src/main.rs`, `fn main() { println!("${name}:{}:{}", ${name}::value(), dependency::value()); }\n`);
      writeFileSync(join(root, name, "Cargo.toml"), '\n[dependencies]\ndependency = { path = "../dependency" }\n', { flag: "a" });
    }
    // gamma's example has beta's example name: Cargo links both to one path.
    mkdirSync(join(root, "gamma", "examples"), { recursive: true });
    put("gamma/Cargo.toml", '[package]\nname = "gamma"\nversion = "0.1.0"\nedition = "2024"\n');
    put("gamma/examples/contract.rs", 'fn main() { println!("gamma"); }\n');
    command(["generate-lockfile", "--offline"]);
    const targets = [["alpha", "bin", "contract"], ["beta", "example", "contract"]];
    const report = {};
    const build = () => buildContractTargets(root, targets, cargo, report);
    const run = binaries => binaries.map(binary => runContractTarget(root, binary).trim());
    // Record the exact text received by the production JSON boundary during
    // this SAME real build. Mutation controls below replay this captured wire;
    // they neither invent compiler messages nor commission another build.
    const recorded = { metadata: undefined, messages: [] }, decode = JSON.parse;
    let binaries;
    JSON.parse = (text, ...args) => {
      const value = decode(text, ...args);
      if (value?.packages) recorded.metadata = text;
      else if (value?.reason) recorded.messages.push(text);
      return value;
    };
    try { binaries = build(); }
    finally { JSON.parse = decode; }
    assert.deepEqual(run(binaries), ["alpha:false:true", "beta:false:true"]);
    // --no-default-features disables selected packages' defaults; a dependency
    // keeps its defaults unless its dependency declaration disables them.
    assert.deepEqual(report.artifacts.filter(item => item.requested).map(item => item.features), [["contract"], ["contract"]]);
    assert.ok(report.artifacts.some(item => item.name === "dependency" && item.features.includes("propagated")));
    assert.ok(binaries.every(binary => binary.includes("configured-target")));
    assert.deepEqual(run(buildContractTargets(root, targets, cargo, report, () => {}, ["alpha/alternate"])), ["alpha:true:true", "beta:false:true"]);
    assert.deepEqual(run(build()), ["alpha:false:true", "beta:false:true"]);
    const before = binaries.map(binary => statSync(binary).mtimeMs);
    assert.deepEqual(run(build()), ["alpha:false:true", "beta:false:true"]);
    assert.ok(report.artifacts.every(item => item.fresh));
    assert.deepEqual(binaries.map(binary => statSync(binary).mtimeMs), before);
    // An intervening build of a different feature set cannot substitute its
    // executable output; Cargo must qualify the requested configuration again.
    command(["build", "-p", "alpha", "-p", "beta", "--all-features", "--locked", "--offline"]);
    assert.deepEqual(run(build()), ["alpha:false:true", "beta:false:true"]);

    // A source edit must invalidate the executable, including library inputs.
    put("alpha/src/main.rs", 'fn main() { println!("changed:{}:{}", alpha::value(), dependency::value()); }\n');
    assert.deepEqual(run(build()), ["changed:false:true", "beta:false:true"]);
    put("dependency/src/lib.rs", "pub fn value() -> bool { false }\n");
    assert.deepEqual(run(build()), ["changed:false:false", "beta:false:false"]);
    put("beta/src/lib.rs", "pub fn value() -> bool { true }\n");
    assert.deepEqual(run(build()), ["changed:false:false", "beta:true:false"]);
    rmSync(binaries[0]);
    assert.deepEqual(run(build()), ["changed:false:false", "beta:true:false"]);
    const goodSource = readFileSync(join(root, "alpha/src/main.rs"));
    put("alpha/src/main.rs", "this is not Rust\n");
    let originalRefusal;
    assert.throws(build, error => {
      originalRefusal = error;
      return /STEP_FAILED contract-build:/.test(error.message);
    });
    put("alpha/src/main.rs", goodSource);
    assert.deepEqual(run(build()), ["changed:false:false", "beta:true:false"]);

    // Replaying is confined to boundary mutations; the real Cargo builds above
    // independently qualify freshness, features, source and executable bytes.
    put("metadata", 'process.stdout.write(require("node:fs").readFileSync("metadata.json"));');
    put("metadata.json", recorded.metadata);
    put("build", 'process.stdout.write(require("node:fs").readFileSync("artifacts.jsonl"));');
    const messages = recorded.messages.map(text => decode(text));
    const emit = rows => put("artifacts.jsonl", rows.map(item => JSON.stringify(item)).join("\n"));
    const replay = () => buildContractTargets(root, targets, process.execPath);
    const first = messages.findIndex(message => message.executable === binaries[0]);
    const second = messages.findIndex(message => message.executable === binaries[1]);
    assert.ok(first >= 0 && second >= 0);
    assert.equal(fuzzContractJson(Buffer.from(recorded.metadata)), true);
    for (const text of recorded.messages) assert.equal(fuzzContractJson(Buffer.from(text)), true);
    emit(messages);
    assert.deepEqual(replay(), binaries);
    // The real executable still exists: guessing its path must not admit it
    // when Cargo's original artifact identity is missing or substituted.
    emit(messages.filter((_, index) => index !== second));
    assert.throws(replay, /Rust contract target was not built/);
    for (const replacement of [
      { package_id: messages[first].package_id },
      { target: { ...messages[second].target, kind: ["bin"] } },
      { target: { ...messages[second].target, name: "another-target" } },
      { executable: null },
    ]) {
      emit(messages.map((message, index) => index === second ? { ...message, ...replacement } : message));
      assert.throws(replay, /Rust contract target was not built/);
    }
    emit([...messages, messages[first]]);
    assert.throws(replay, /duplicate Rust contract artifact/);
    // Two requested targets answered by one executable path are refused, not
    // run twice as whichever target Cargo linked last.
    emit(messages.map((message, index) => index === second ? { ...message, executable: binaries[0] } : message));
    assert.throws(replay, error => error instanceof ContractTargetCollision && error.output === binaries[0]);
    put("artifacts.jsonl", recorded.messages[first].slice(0, -1));
    assert.throws(replay, error => error.cause instanceof SyntaxError && /STEP_FAILED contract-artifact:/.test(error.message));
    const previousRustc = process.env.RUSTC;
    try {
      process.env.RUSTC = join(root, "missing-diagnostic-compiler");
      emit(messages);
      const refusedProbe = {};
      assert.deepEqual(buildContractTargets(root, targets, process.execPath, refusedProbe), binaries);
      assert.match(refusedProbe.rustc_error, /ENOENT/);
    } finally {
      if (previousRustc === undefined) delete process.env.RUSTC;
      else process.env.RUSTC = previousRustc;
    }
    put("compiler-refusal.jsonl", originalRefusal.stdout);
    put("build", `process.stdout.write(require("node:fs").readFileSync("compiler-refusal.jsonl")); process.exit(${originalRefusal.status});`);
    assert.throws(replay, error => error.stdout === originalRefusal.stdout && error.status === originalRefusal.status
      && error.signal === originalRefusal.signal && error.message.includes(originalRefusal.stdout));
    assert.ok(binaries.every(binary => statSync(binary).isFile()));
    assert.throws(() => buildContractTargets(root, [], process.execPath), /target set is empty/);

    // Capture output from the original Cargo-selected binaries. Count actual
    // starts and decodes independently of the generator's cache implementation.
    const bundle = '{"left":"exact\\nbytes","right":"other","invalid":7}\n';
    put("alpha/src/main.rs", `use std::io::Write; fn main() {
      std::fs::OpenOptions::new().create(true).append(true).open("starts").unwrap().write_all(b"x").unwrap();
      print!("{}", r#"${bundle}"#);
    }\n`);
    put("beta/src/main.rs", 'fn main() { println!("{{not JSON"); }\n');
    const outputFor = contractTargetOutputs(root, build(), [
      [0, "left"], [0, "right"], [0, "left"], [0, undefined],
      [0, "absent"], [0, "toString"], [0, "invalid"], [1, "left"],
    ]);
    const parse = JSON.parse;
    let decodes = 0;
    JSON.parse = (text, ...args) => {
      if (text === bundle) decodes += 1;
      return parse(text, ...args);
    };
    try {
      assert.equal(outputFor(0, "left"), "exact\nbytes");
      assert.equal(outputFor(0, "right"), "other");
      assert.equal(outputFor(0, "left"), "exact\nbytes");
      assert.equal(outputFor(0), bundle);
      assert.throws(() => outputFor(0, "absent"), /STEP_FAILED contract-payload: missing absent; recovery:/);
      assert.throws(() => outputFor(0, "toString"), /STEP_FAILED contract-payload: missing toString; recovery:/);
      assert.throws(() => outputFor(0, "invalid"), /STEP_FAILED contract-payload: missing invalid; recovery:/);
      assert.throws(() => outputFor(1, "left"), /STEP_FAILED contract-payload: malformed bundle.*recovery:/);
      assert.equal(decodes, 1);
      assert.equal(readFileSync(join(root, "starts"), "utf8"), "x");
    } finally {
      JSON.parse = parse;
    }
    assert.throws(() => buildContractTargets(root, [...targets, targets[0]], cargo), /duplicate Rust contract target/);
    const colliding = {};
    assert.throws(() => buildContractTargets(root, [...targets, ["gamma", "example", "contract"]], cargo, colliding), error => {
      assert.ok(error instanceof ContractTargetCollision);
      assert.deepEqual(error.targets, [["beta", "example", "contract"], ["gamma", "example", "contract"]]);
      assert.match(error.message, /STEP_FAILED contract-collision:.*recovery:/);
      return true;
    });
    assert.equal(colliding.phase, "metadata", "a colliding target set must be refused before Cargo builds it");
    assert.throws(() => buildContractTargets(root, [["alpha", "bin", "absent"]], cargo), /unknown Rust contract target/);
    assert.throws(() => buildContractTargets(root, [["alpha", "lib", "alpha"]], cargo), /unknown Rust contract target/);
  } finally {
    if (targetOverride !== undefined) process.env.CARGO_TARGET_DIR = targetOverride;
    rmSync(root, { recursive: true, force: true });
  }
});
