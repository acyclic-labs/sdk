// Check actual typed JSON admission mutations through the shared corpus property.
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, openSync, closeSync, readFileSync, readdirSync, statSync, writeFileSync } from "node:fs";
import { join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";

const root = fileURLToPath(new URL("../../../", import.meta.url));
const recovery = "node rust/crates/harness-fuzz/typed-json-negative-control.mjs";
const digest = (bytes) => createHash("sha256").update(bytes).digest("hex");
const portable = (path) => relative(root, path).replaceAll("\\", "/");
let step = "select typed JSON control";
let out;
try {
  const { values: args } = parseArgs({ options: {
    "raw-domain-prior": { type: "string" }, "depth-prior": { type: "string" },
    "first-cause-prior": { type: "string" }, "hidden-items": { type: "boolean" },
    help: { type: "boolean" },
  } });
  if (args.help) {
    console.log(`Usage: ${recovery} [--raw-domain-prior REVISION | --depth-prior REVISION | --first-cause-prior REVISION | --hidden-items]\nPlants the actual selected production omission, checks its semantic assertion, and restores exact source and oracle bytes. Prior controls also retain the original oracle miss or false positive.`);
  } else {
    const selected = Object.keys(args).filter((key) => args[key]);
    if (selected.length > 1) throw new Error("select at most one admission control");
    const mode = selected[0];
    const nextest = ["depth-prior", "first-cause-prior", "hidden-items"].includes(mode);
    const source = join(root, "rust/crates/harness-core/src/executor/json", mode ? "admission.rs" : "records.rs");
    const oracle = join(root, mode === "hidden-items"
      ? "rust/crates/harness-core/src/executor/json/value_shape_tests.rs"
      : "rust/crates/harness/tests/support/bounded_json.rs");
    const suffix = {
      "raw-domain-prior": "expand-borrowed-raw", "depth-prior": "ignore-depth",
      "first-cause-prior": "drop-first-cause", "hidden-items": "ignore-hidden-items",
    }[mode] ?? "ignore-syntax-admission";
    const patch = join(root, `rust/crates/harness-fuzz/negative-controls/typed-json-${suffix}.patch`);
    const label = { "raw-domain-prior": "raw-domain", "depth-prior": "depth", "first-cause-prior": "first-cause", "hidden-items": "hidden-items" }[mode] ?? "syntax";
    out = join(root, `target/typed-json-${label}-control`);
    const property = mode === "hidden-items"
      ? "executor::json::value_shape_tests::hidden_raw_values_are_bounded_before_decode"
      : "executor::json::tests::object_work::checked_codec_fuzz_corpus";
    const command = nextest
      ? ["cargo", "nextest", "run", "--locked", "--profile", "ci", "--retries", "0", "-p", "acyclic-harness-core", "--lib",
        ...(mode === "hidden-items" ? ["--features", "filesystem"] : []), "-E", `test(=${property})`]
      : ["cargo", "test", "--locked", "-p", "acyclic-harness-core", "--lib", property, "--", "--exact", "--nocapture"];
    function git(...argv) {
      const result = spawnSync("git", argv, { cwd: root, maxBuffer: 32 * 1024 * 1024 });
      if (result.error) throw result.error;
      if (result.status !== 0) throw new Error(result.stderr.toString() || `git ${argv[0]} failed`);
      return result.stdout;
    }
    step = "read original typed JSON source";
    const original = readFileSync(source);
    const originalOracle = readFileSync(oracle);
    const oracleHash = digest(originalOracle);
    const lock = join(root, "Cargo.lock");
    const lockHash = digest(readFileSync(lock));
    mkdirSync(out, { recursive: true });
    const corpus = join(root, "rust/crates/harness-fuzz/corpus/bounded_json");
    const result = { source: digest(original), oracle: oracleHash, patch: digest(readFileSync(patch)), lock: lockHash, command, phases: [],
      corpus: mode === "hidden-items" ? [] : readdirSync(corpus).sort().filter((name) => statSync(join(corpus, name)).isFile())
        .map((name) => ({ name, sha256: digest(readFileSync(join(corpus, name))) })) };
    if (mode === "hidden-items") result.operand_source = portable(oracle);
    const save = () => writeFileSync(join(out, "result.json"), `${JSON.stringify(result, null, 2)}\n`);
    try {
      step = "apply selected typed JSON production mutation";
      git("apply", "--check", patch);
      git("apply", patch);
      const mutated = readFileSync(source);
      let prior;
      const positive = nextest ? "1 test run: 1 passed" : "test result: ok. 1 passed; 0 failed";
      let phases = [["diagnostic-admission-refusal-red", 101, "typed JSON decode must preserve caller charge refusal at frontier 5"],
        ["restored-original-corpus", 0, positive]];
      if (args[mode] && mode !== "hidden-items") {
        step = "read prior shared oracle from the selected revision";
        prior = git("show", `${args[mode]}:${portable(oracle)}`);
        result.prior_oracle = { revision: args[mode], sha256: digest(prior) };
      }
      if (mode === "raw-domain-prior") phases = [["prior-oracle-mutant-miss", 0, positive],
        ["raw-domain-red", 101, "typed raw JSON must admit independently valid raw input"], ["restored-original-corpus", 0, positive]];
      if (mode === "depth-prior") phases = [["prior-depth-false-positive", 100, "typed raw JSON must admit independently valid raw input"],
        ["depth-omission-red", 100, "typed raw JSON must refuse documents beyond the declared depth"], ["restored-original-corpus", 0, positive]];
      if (mode === "first-cause-prior") phases = [["prior-oracle-mutant-miss", 0, positive],
        ["first-cause-loss-red", 100, "materialized JSON admission must preserve the first caller cause"], ["restored-original-corpus", 0, positive]];
      if (mode === "hidden-items") phases = [["hidden-item-omission-red", 100, "assertion failed: !admitted && inspected <= maximum_inspected"],
        ["restored-original-property", 0, positive]];
      for (const [name, expected, assertion] of phases) {
        writeFileSync(source, ["prior-depth-false-positive", "restored-original-corpus", "restored-original-property"].includes(name) ? original : mutated);
        writeFileSync(oracle, ["prior-oracle-mutant-miss", "prior-depth-false-positive"].includes(name) ? prior : originalOracle);
        step = name;
        const log = join(out, `${name}.log`);
        const stream = openSync(log, "w");
        let completed;
        try {
          completed = spawnSync(command[0], command.slice(1), { cwd: root, stdio: ["ignore", stream, stream] });
        } finally {
          closeSync(stream);
        }
        const output = readFileSync(log, "utf8");
        process.stdout.write(output.slice(-6000));
        const phase = { name, exit: completed.status, source: digest(readFileSync(source)), oracle: digest(readFileSync(oracle)), log: digest(readFileSync(log)) };
        const pattern = nextest ? /command:.*?(\/[^\s]+\/acyclic_harness_core-[a-f0-9]+)/g : /Running unittests .*? \(([^)]+)\)/g;
        const executables = [...output.matchAll(pattern)];
        if (executables.length) {
          const executable = resolve(root, executables.at(-1)[1]);
          if (existsSync(executable) && statSync(executable).isFile()) phase.executable = {
            path: executable, sha256: digest(readFileSync(executable)), bytes: statSync(executable).size, exported: false,
          };
        }
        if (!phase.executable) phase.executable_identity = "not emitted by the test runner; no binary hash inferred";
        result.phases.push(phase);
        save();
        if (completed.error) throw completed.error;
        if (completed.status !== expected || !output.includes(assertion)) throw new Error(`expected semantic assertion absent; inspect ${log}`);
        if ([100, 101].includes(expected) && (!output.includes("panicked at ") || !output.includes("test result: FAILED."))) {
          throw new Error(`mutation did not reach the runtime assertion; inspect ${log}`);
        }
      }
    } finally {
      writeFileSync(source, original);
      writeFileSync(oracle, originalOracle);
      result.restored_source = digest(readFileSync(source)) === digest(original);
      result.restored_oracle = digest(readFileSync(oracle)) === oracleHash;
      save();
      if (!result.restored_source || !result.restored_oracle || digest(readFileSync(lock)) !== lockHash) throw new Error("source, oracle or Cargo.lock changed during the control");
    }
    console.log("PASS: actual typed admission mutation rejected; exact restored corpus passes");
  }
} catch (error) {
  console.error(`step=${step}; ${error.message}; recovery=${out ? `inspect ${join(out, "result.json")} and its phase logs, correct the diagnosed source or command, then run ${recovery}` : `run ${recovery} --help`}`);
  process.exitCode = 1;
}
