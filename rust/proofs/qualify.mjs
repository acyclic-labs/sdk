#!/usr/bin/env node
// Qualify a production oracle and its retained mutation.
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { closeSync, mkdirSync, mkdtempSync, openSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../../", import.meta.url));
// One manifest names every retained mutation, the production source it
// changes and the original oracle that must reject it.
const manifest = JSON.parse(readFileSync(new URL("mutations.json", import.meta.url), "utf8"));
if (manifest.version !== 1) throw new Error(`unsupported mutation manifest version ${manifest.version}`);
const { claims } = manifest;
const fromRoot = path => fileURLToPath(new URL(path, new URL("../../", import.meta.url)));

/** The repository paths a patch changes, from its `+++` headers. */
function patchTargets(patch) {
  return [...new Set(readFileSync(fromRoot(patch), "utf8").split(/\r?\n/)
    .filter(line => /^\+\+\+ [ab]\//.test(line)).map(line => line.slice(6).trimEnd()))];
}
/** A claim's patch path when it names no mutations list. */
function defaultPatch(claim, options) {
  return claim === "work-mixed"
    ? "rust/proofs/work-arithmetic/mixed-mutation.patch"
    : options.patchPath ?? `rust/proofs/${claim}/${options.patch ?? "mutation.patch"}`;
}
/** Every mutation a claim retains, each with its patch and source. */
function claimMutations(claim) {
  const [source, , , diagnostic, , options = {}] = claims[claim];
  return (options.mutations ?? [{ patch: defaultPatch(claim, options), diagnostic, label: options.negativeLabel ?? "mutated" }])
    .map(mutation => ({ source, ...mutation }));
}
/** Every retained patch file under the manifest's roots. */
function retainedPatches() {
  return manifest.roots.flatMap(directory => readdirSync(fromRoot(directory), { recursive: true })
    .map(name => `${directory}${String(name).replaceAll("\\", "/")}`)
    .filter(path => path.endsWith(".patch"))).sort();
}

// Report: every retained patch is listed by a claim or as evidence, and every
// listed patch still applies to this tree. Enforcement turns on in the
// manifest once nothing is stale or unlisted.
function report() {
  const listed = new Map();
  for (const claim of Object.keys(claims)) {
    for (const mutation of claimMutations(claim)) {
      for (const patch of [mutation.patch, mutation.priorOraclePatch].filter(Boolean)) listed.set(patch, claim);
    }
  }
  for (const mutation of manifest.selfTest.entry[5].mutations) listed.set(mutation.patch, manifest.selfTest.claim);
  const evidence = new Set(manifest.evidence);
  let stale = 0, unlisted = 0;
  for (const patch of retainedPatches()) {
    if (evidence.has(patch)) continue;
    if (!listed.has(patch)) {
      unlisted += 1;
      console.log(`MUTATION_UNLISTED patch=${patch} targets=${patchTargets(patch).join(",")}`);
    }
  }
  for (const [patch, claim] of listed) {
    const check = spawnSync("git", ["apply", "--check", patch], { cwd: root, encoding: "utf8" });
    if (check.error) throw check.error;
    if (check.status !== 0) {
      stale += 1;
      console.log(`MUTATION_STALE claim=${claim} patch=${patch} targets=${patchTargets(patch).join(",")}`);
    }
  }
  console.log(`MUTATION_REPORT listed=${listed.size} evidence=${evidence.size} stale=${stale} unlisted=${unlisted} enforced=${manifest.enforce}`);
  return !manifest.enforce || stale + unlisted === 0;
}

// Plan: the claims a change touches, by the shared touched-unit selector. A
// claim's files are its sources, oracle, patches and every file they change.
function plan(changed) {
  const units = Object.keys(claims).map(claim => {
    const [, , , , , options = {}] = claims[claim];
    const files = claimMutations(claim).flatMap(mutation => [mutation.source, mutation.patch,
      ...patchTargets(mutation.patch), ...(mutation.priorOraclePatch ? patchTargets(mutation.priorOraclePatch) : [])]);
    return { unit: claim, files: [...new Set([...files, ...(options.oracle ? [options.oracle] : [])])] };
  });
  const work = mkdtempSync(join(tmpdir(), "qualify-plan-"));
  try {
    writeFileSync(join(work, "units.json"), JSON.stringify(units));
    const metadata = spawnSync("cargo", ["metadata", "--format-version", "1", "--no-deps", "--locked"], {
      cwd: root, encoding: "utf8", maxBuffer: 1 << 30 });
    if (metadata.error || metadata.status !== 0) throw new Error(`cargo metadata failed: ${metadata.error?.message ?? metadata.stderr}`);
    writeFileSync(join(work, "metadata.json"), metadata.stdout);
    const selected = spawnSync("python3", ["-I", "-B", fromRoot("scripts/touched_units.py"),
      join(work, "units.json"), join(work, "metadata.json"), changed, ...manifest.homes.flatMap(home => ["--home", home])],
      { cwd: root, encoding: "utf8", stdio: ["ignore", "pipe", "inherit"] });
    if (selected.error || selected.status !== 0) throw new Error(`touched-unit selection failed: ${selected.error?.message ?? selected.status}`);
    return selected.stdout.split("\n").filter(Boolean);
  } finally {
    rmSync(work, { recursive: true, force: true });
  }
}

const [claim, ...extra] = process.argv.slice(2);
const instrumented = extra.length === 1 && extra[0] === "--instrumented" && claims[claim]?.[5]?.instrumentable;
const from = extra.length === 2 && extra[0] === "--from" ? extra[1] : undefined;
const help = `node rust/proofs/qualify.mjs <${Object.keys(claims).join("|")}> [--from mutation-label]
node rust/proofs/qualify.mjs --report
node rust/proofs/qualify.mjs --changed CHANGED_FILES [--plan]
node rust/proofs/qualify.mjs --self-test`;
if (claim === "--help" && extra.length === 0) {
  console.log(help);
} else if (claim === "--report" && extra.length === 0) {
  if (!report()) process.exitCode = 1;
} else if (claim === "--changed" && (extra.length === 1 || (extra.length === 2 && extra[1] === "--plan"))) {
  // Each selected claim qualifies in full; one failure does not hide the rest.
  const selected = plan(extra[0]);
  for (const name of selected) console.log(`MUTATION_SELECT claim=${name}`);
  if (extra.length === 1) {
    const failed = selected.filter(name => !qualify(name, claims[name], false, undefined));
    console.log(`MUTATION_VERDICT selected=${selected.length} failed=${failed.length}${failed.length ? ` claims=${failed.join(",")}` : ""}`);
    if (failed.length) process.exitCode = 1;
  }
} else if (claim === "--self-test" && extra.length === 0) {
  // A planted mutation its oracle cannot see must fail qualification at the
  // mutation step; the runner is broken if it passes or fails anywhere else.
  const { claim: planted, step } = manifest.selfTest;
  const outcome = qualify(planted, manifest.selfTest.entry, false, undefined, true);
  const verdict = outcome.failed && outcome.step.startsWith(step) ? "PASS" : "FAIL";
  console.log(`MUTATION_SELF_TEST planted=${planted} failed=${outcome.failed} step=${JSON.stringify(outcome.step)} verdict=${verdict}`);
  if (verdict !== "PASS") process.exitCode = 1;
} else if (!Object.hasOwn(claims, claim) || (extra.length !== 0 && !instrumented && from === undefined)) {
  console.error(`step=select claim; cause=expected exactly one supported claim; recovery: ${help}`);
  process.exitCode = 2;
} else if (!qualify(claim, claims[claim], instrumented, from)) {
  process.exitCode = 1;
}

/**
 * Qualify one claim: every mutation applies, the unchanged oracle rejects it,
 * every changed file is restored byte for byte and the restored oracle passes.
 * Returns whether it qualified, or with `detail` the failing step as well.
 */
function qualify(claim, entry, instrumented, from, detail = false) {
  let failed = false;
  const [source, crate, test, diagnostic, features, options = {}] = entry;
  const environment = { ...process.env, ...options.env };
  // A mutation gated on environment holds its variables only while it is checked.
  let mutationEnv = {};
  const tests = [test, ...(options.additionalTests ?? [])];
  const patch = defaultPatch(claim, options);
  const target = options.target ?? ["--lib"];
  let fuzzTarget;
  if (options.fuzz) {
    const compiler = spawnSync("rustc", ["-vV"], { encoding: "utf8" });
    fuzzTarget = compiler.stdout?.match(/^host: (\S+)$/m)?.[1];
    if (compiler.error || compiler.status !== 0 || !fuzzTarget) {
      console.error(`step=select native fuzz target; cause=${compiler.error?.message ?? compiler.stderr ?? "rustc host absent"}; recovery: rustc -vV; then node rust/proofs/qualify.mjs ${claim}`);
      return detail ? { failed: true, step: "select native fuzz target" } : false;
    }
  }
  const fuzzCommand = runs => ["run", "--locked", "-p", crate, ...features, "--bin", options.fuzz.bin,
    "--target", fuzzTarget,
    "--", options.fuzz.corpus, `-runs=${runs}`, "-seed=71", "-print_final_stats=1",
    `-artifact_prefix=target/${claim}/`];
  const command = instrumented
    ? ["+nightly", "fuzz", "run", "--fuzz-dir", ".", "stream_persistence", "corpus/stream_persistence/projection-v1-append", "--", "-runs=1000", "-seed=1697"]
    : options.command ?? (options.fuzz ? fuzzCommand(1) : options.nextest
    ? ["nextest", "run", "--locked", "-p", crate, ...features, ...target, "-E", tests.map(name => `test(=${name})`).join(" | "), ...options.nextest]
    : ["test", "--locked", "-p", crate, ...features, ...target, "--", "--exact", "--nocapture", ...tests]);
  const positive = instrumented
    ? ["+nightly", "fuzz", "run", "--fuzz-dir", ".", "stream_persistence", fileURLToPath(new URL("target/projection-fuzz/corpus", new URL("../../", import.meta.url))), "corpus/stream_persistence", "--", "-runs=1000", "-seed=1697"]
    : options.positiveCommand ?? (options.fuzz ? fuzzCommand(options.fuzz.runs) : options.positive
    ? ["nextest", "run", "--locked", "-p", crate, ...features, ...target, "-E",
      options.positive.map(name => `test(=${name})`).join(" | "), ...(options.nextest ?? ["--no-tests=fail"])]
    : command);
  const sources = [...new Set([source, ...(options.mutations ?? []).map(mutation => mutation.source ?? source)])];
  const recovery = `git diff -- ${sources.join(" ")}; inspect target/${claim}/*.log; correct source or command, then node rust/proofs/qualify.mjs ${claim}${instrumented ? " --instrumented" : from === undefined ? "" : ` --from ${from}`}`;
  const directory = new URL(`target/${claim}/`, new URL("../../", import.meta.url));
  const oraclePath = options.oracle && new URL(options.oracle, new URL("../../", import.meta.url));
  let oracle;
  const phases = [];
  let step = "read original production source";
  function require(condition) {
    if (!condition) throw new Error("required qualification outcome absent");
  }
  function run(program, args, log) {
    const descriptor = log && openSync(log, "w");
    let result;
    try {
      result = spawnSync(program, args, {
        cwd: instrumented && program === "cargo" ? fileURLToPath(new URL("rust/codec-fuzz/", new URL("../../", import.meta.url))) : root, encoding: "utf8",
        ...(log ? { stdio: ["ignore", descriptor, descriptor] } : {}),
        env: {
          ...environment, ...mutationEnv, PROPTEST_RNG_SEED: environment.PROPTEST_RNG_SEED ?? "1697",
          ...(options.fuzz ? {
            // RUSTFLAGS replaces Cargo configuration, including replay's Tokio support.
            RUSTFLAGS: `${environment.RUSTFLAGS?.includes("-Cpasses=sancov-module") ? environment.RUSTFLAGS
              : `${environment.RUSTFLAGS ?? ""} -Cpasses=sancov-module -Cllvm-args=-sanitizer-coverage-level=4 -Cllvm-args=-sanitizer-coverage-inline-8bit-counters -Cllvm-args=-sanitizer-coverage-pc-table -Cllvm-args=-sanitizer-coverage-trace-compares`} --cfg tokio_unstable`,
          } : {}),
          ...(options.loom ? {
            RUSTC_WORKSPACE_WRAPPER: fileURLToPath(new URL(
              `../../scripts/loom-rustc.${process.platform === "win32" ? "cmd" : "sh"}`, import.meta.url)),
          } : {}),
        },
      });
    } finally {
      if (descriptor !== undefined) closeSync(descriptor);
    }
    if (result.error) throw result.error;
    return { ...result, output: log ? readFileSync(log, "utf8") : `${result.stdout ?? ""}${result.stderr ?? ""}` };
  }
  function check(label, selected = command, checkedSource = source, expectedOracle = oracle) {
    if (oracle) require(readFileSync(oraclePath).equals(expectedOracle));
    mkdirSync(directory, { recursive: true });
    const invocation = options.runner ?? ["cargo", ...selected];
    const result = run(invocation[0], invocation.slice(1), new URL(`${label}.log`, directory));
    console.log(`step=${label}; command=${JSON.stringify(invocation)}; exit=${result.status}; signal=${result.signal}`);
    const lines = result.output.trimEnd().split(/\r?\n/);
    const firstError = lines.findIndex(line => /^error(?:\[|:)/.test(line));
    if (firstError >= 0 && firstError < lines.length - 35) {
      console.log(lines.slice(firstError, firstError + 12).join("\n"));
    }
    console.log(lines.slice(-35).join("\n"));
    if (options.fuzz) {
      const coverage = result.output.match(/^#.*\bcov:.*$/gm)?.at(-1);
      if (coverage) console.log(coverage);
    }
    if (oracle) {
      const sha256 = bytes => createHash("sha256").update(bytes).digest("hex");
      const checkedPath = new URL(checkedSource, new URL("../../", import.meta.url));
      const phase = { phase: label, exit: result.status, source: checkedSource, source_sha256: sha256(readFileSync(checkedPath)), oracle_sha256: sha256(expectedOracle) };
      phases.push(phase);
      console.log(JSON.stringify(phase));
      require(readFileSync(oraclePath).equals(expectedOracle));
      const count = label === "restored" ? options.positiveCount ?? options.positive?.length ?? tests.length : tests.length;
      const started = options.invoked ?? (invocation[0] === "cargo" && invocation[1] === "nextest"
        ? `Starting ${count} ${count === 1 ? "test" : "tests"} across 1 binary`
        : `running ${count} ${count === 1 ? "test" : "tests"}`);
      require(result.output.includes(started));
    }
    return result;
  }
  try {
    if (oraclePath) oracle = readFileSync(oraclePath);
    if (instrumented) mkdirSync(new URL("corpus/", directory), { recursive: true });
    const mutations = options.mutations ?? [{ patch, diagnostic, label: options.negativeLabel ?? "mutated" }];
    step = "select first mutation";
    const start = from === undefined ? 0 : mutations.findIndex(mutation => mutation.label === from);
    if (start < 0) throw new Error(`unknown mutation label ${JSON.stringify(from)}`);
    if (start !== 0) console.log(`selection=partial; preceding=${start} mutations require their own source-bound receipts`);
    for (const mutation of mutations.slice(start)) {
      // Every file the patch changes is restored, not only the named source.
      const originals = patchTargets(mutation.patch).map(target => [fromRoot(target), readFileSync(fromRoot(target))]);
      step = `check mutation anchors: ${mutation.patch}`;
      const anchor = run("git", ["apply", "--check", mutation.patch]);
      if (anchor.status !== 0) throw new Error(anchor.output);
      try {
        step = `apply production mutation: ${mutation.patch}`;
        const applied = run("git", ["apply", mutation.patch]);
        if (applied.status !== 0) throw new Error(applied.output);
        if (mutation.priorOraclePatch) {
          require(oracle !== undefined);
          try {
            step = `apply prior oracle: ${mutation.priorOraclePatch}`;
            const prior = run("git", ["apply", mutation.priorOraclePatch]);
            if (prior.status !== 0) throw new Error(prior.output);
            step = "prior oracle misses the same production mutation";
            const missed = check(`${mutation.label}-prior`, mutation.command ?? command,
              mutation.source ?? source, readFileSync(oraclePath));
            require(missed.status === 0 && !missed.signal
              && options.completed.every(marker => missed.output.includes(marker)));
          } finally {
            writeFileSync(oraclePath, oracle);
          }
        }
        step = `unchanged oracle rejects production mutation: ${mutation.patch}`;
        mutationEnv = mutation.env ?? {};
        const mutated = check(mutation.label, mutation.command ?? command, mutation.source ?? source);
        const diagnostics = [mutation.diagnostic, ...(mutation.additionalDiagnostics ?? options.additionalDiagnostics ?? [])];
        require(instrumented
          ? mutated.status !== 0 && !mutated.signal && mutated.output.includes("assertion failed: acyclic_stream::codec_fuzz::check(bytes)") && mutated.output.includes("ERROR: libFuzzer:")
          : options.fuzz ? mutated.status !== 0 && !mutated.signal && diagnostics.every(message => mutated.output.includes(message)) && mutated.output.includes("ERROR: libFuzzer:")
          : mutated.status === (options.negativeExit ?? (options.nextest ? 100 : 101)) && !mutated.signal && diagnostics.every(message => mutated.output.includes(message))
            && (options.started === undefined || mutated.output.includes(options.started))
            && (options.nextest || mutated.output.includes(options.failed === true ? mutation.diagnostic : options.failed ?? `0 passed; ${tests.length} failed`)));
      } finally {
        mutationEnv = {};
        for (const [target, original] of originals) writeFileSync(target, original);
      }
      step = "restore exact original source";
      require(originals.every(([target, original]) => readFileSync(target).equals(original)));
    }
    step = "restored original oracle completes";
    const restored = check("restored", positive);
    const completed = options.completed ?? (options.fuzz ? ["DONE", `${options.fuzz.runs} runs`, "cov:"] : [options.positive ? `${options.positive.length} tests run: ${options.positive.length} passed`
      : options.nextest ? `${tests.length} test${tests.length === 1 ? "" : "s"} run: ${tests.length} passed` : `${tests.length} passed; 0 failed`]);
    require(restored.status === 0 && !restored.signal && (instrumented
      ? ["DONE", "1000 runs", "cov:"].every(marker => restored.output.includes(marker))
      : completed.every(marker => restored.output.includes(marker))));
  } catch (error) {
    console.error(`step=${step}; cause=${error.message}; recovery: ${recovery}`);
    failed = true;
  } finally {
    if (oracle) {
      try {
        mkdirSync(directory, { recursive: true });
        writeFileSync(new URL("phases.json", directory), `${JSON.stringify(phases, null, 2)}\n`);
      } catch (error) {
        console.error(`step=write phase report; cause=${error.message}; recovery: ${recovery}`);
        failed = true;
      }
    }
  }
  return detail ? { failed, step } : !failed;
}
