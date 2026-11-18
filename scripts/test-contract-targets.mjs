import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import test from "node:test";
import { assertLabel, executablesFromCquery } from "./bazel-executables.mjs";
import { buildContractTargets, captureContractCommand, contractTargetOutputs, runContractTarget } from "./contract-targets.mjs";
import { fuzz as fuzzContractJson } from "./contract-targets.fuzz.mjs";

test("contract command refusals retain original native diagnostics and terminal metadata", () => {
  const root = mkdtempSync(join(tmpdir(), "sdk-contract-refusal-"));
  const previousOptions = process.env.NODE_OPTIONS;
  const previousBazel = process.env.ACYCLIC_BAZEL;
  try {
    const producer = join(root, "producer.mjs");
    // This native producer records its actual OS refusal; no Bazel or provider
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
      assert.match(error.message, /STEP_FAILED contract-(run|build):.*recovery:/);
      assert.match(String(error), /ENOENT/);
      return true;
    };
    assert.throws(() => runContractTarget(root, missing), retainsSpawnError);
    const report = {};
    process.env.ACYCLIC_BAZEL = missing;
    assert.throws(() => buildContractTargets(root, ["//unused:unused"], report), retainsSpawnError);
    assert.equal(report.phase, "build");
  } finally {
    if (previousOptions === undefined) delete process.env.NODE_OPTIONS;
    else process.env.NODE_OPTIONS = previousOptions;
    if (previousBazel === undefined) delete process.env.ACYCLIC_BAZEL;
    else process.env.ACYCLIC_BAZEL = previousBazel;
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

test("contract selection refuses every unknown family before Bazel or publication", () => {
  const root = mkdtempSync(join(tmpdir(), "sdk-contract-selection-"));
  try {
    for (const families of [["absent-family"], ["stream", "absent-family"], [""]]) {
      const result = spawnSync(process.execPath, [
        fileURLToPath(new URL("./generate-contracts.mjs", import.meta.url)), "check", ...families,
      ], { cwd: root, encoding: "utf8", env: {
        ...process.env, ACYCLIC_BAZEL: join(root, "bazel-must-not-start"), SDK_TEMP_DIR: root,
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

test("contract executables are exactly the ones cquery names for the requested labels", () => {
  const labels = ["//rust/crates/a:contract", "//rust/crates/b:contract_example"];
  const root = join(tmpdir(), "execroot");
  const tab = (label, file) => `${label}\t${file}`;
  const answer = lines => executablesFromCquery(labels, lines.join("\n"), root);
  // Bzlmod prints main-repository labels canonically; order follows the request, not cquery.
  assert.deepEqual(answer([tab(`@@${labels[1]}`, "bazel-out/k8/bin/rust/crates/b/contract_example"), tab(labels[0], "bazel-out/k8/bin/rust/crates/a/contract")]),
    [join(root, "bazel-out/k8/bin/rust/crates/a/contract"), join(root, "bazel-out/k8/bin/rust/crates/b/contract_example")]);
  assert.throws(() => answer([tab(labels[0], "bazel-out/a")]), /cquery did not answer \/\/rust\/crates\/b:contract_example/);
  assert.throws(() => answer([tab(labels[0], "bazel-out/a"), tab(labels[0], "bazel-out/b"), tab(labels[1], "bazel-out/c")]), /answered \/\/rust\/crates\/a:contract twice/);
  assert.throws(() => answer([tab(labels[0], "bazel-out/a"), tab(labels[1], "None")]), /is not an executable target/);
  assert.throws(() => answer([tab("//other:target", "bazel-out/a")]), /unexpected cquery answer/);
  assert.throws(() => answer([`${labels[0]} bazel-out/a`]), /unexpected cquery answer/);
  for (const label of ["rust/crates/a:contract", "//rust/crates/a", "@crates//:serde", "//a:b c", ""]) {
    assert.throws(() => assertLabel(label), /is not an absolute Bazel label/);
  }
  assert.throws(() => buildContractTargets(tmpdir(), []), /target set is empty/);
  assert.throws(() => buildContractTargets(tmpdir(), [labels[0], labels[0]]), /duplicate label/);
});
