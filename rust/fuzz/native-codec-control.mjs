#!/usr/bin/env bun
// Qualify native codecs with their original writers and shared oracles.
import { deepStrictEqual } from "node:assert";
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { once } from "node:events";
import { closeSync, cpSync, mkdirSync, openSync, readdirSync, readFileSync, writeFileSync, writeSync } from "node:fs";
import { dirname, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../../", import.meta.url));
const help = "bun rust/fuzz/native-codec-control.mjs <claim|owner-envelope|report-port> <new-evidence-directory> [--routine|--coverage-only]";
const configurations = {
  claim: ["native_claim_record", "claim_codec", "claim checksum bypassed", "claim/record.rs", [],
    ["/claim/record.rs"], "claim::record::Record::encode"],
  "owner-envelope": ["native_owner_record", "owner_codec", "corrupt record decoded", "owner_record/mod.rs", ["--features", "owner-record"],
    ["/owner_record/mod.rs", "/owner_record/protection.rs"], "owner_record::encode_with"],
  "report-port": ["native_report_port", "report_port", "report decoder readmitted its consumed request", "obs/report_port.rs", ["--features", "report-port"],
    ["/obs/report_port.rs"], "ReportRequest serde writer through original native ReportPort"],
};
const digest = bytes => createHash("sha256").update(bytes).digest("hex");
const files = directory => readdirSync(directory).sort();
const hashes = directory => Object.fromEntries(files(directory).map(name => [name, digest(readFileSync(join(directory, name)))]));
function lockContents(bytes) {
  const contents = Bun.TOML.parse(bytes.toString("utf8"));
  contents.package.sort((left, right) => {
    for (const field of ["name", "version", "source"]) {
      const a = left[field] ?? "", b = right[field] ?? "";
      if (a !== b) return a < b ? -1 : 1;
    }
    return 0;
  });
  return contents;
}
function equal(left, right) {
  try { deepStrictEqual(left, right); return true; } catch { return false; }
}
function replaceOnce(original, needle, replacement, step) {
  const offset = original.indexOf(needle);
  if (offset < 0 || original.indexOf(needle, offset + 1) >= 0) {
    throw new Error(`${step}: expected one original mutation anchor; recovery: inspect the selected source and update the exact mutation`);
  }
  return Buffer.concat([original.subarray(0, offset), Buffer.from(replacement), original.subarray(offset + Buffer.byteLength(needle))]);
}
function binariesBelow(directory, name) {
  return readdirSync(directory, { withFileTypes: true }).flatMap(entry => {
    const path = join(directory, entry.name);
    return entry.isDirectory() ? binariesBelow(path, name) : entry.name === name ? [path] : [];
  });
}

async function main() {
  const [kind, destination, mode = "fuzz", ...extra] = process.argv.slice(2);
  if (kind === "--help" && !destination) { console.log(help); return; }
  if (!Object.hasOwn(configurations, kind) || !destination || extra.length
      || (process.argv.length !== 4 && (process.argv.length !== 5 || !["--routine", "--coverage-only"].includes(mode)))) {
    throw new Error(`arguments: expected codec, evidence directory and optional mode; recovery: ${help}`);
  }
  const owner = kind === "owner-envelope", reportPort = kind === "report-port";
  const routine = mode === "--routine", coverageOnly = mode === "--coverage-only";
  const toolchain = "+nightly-2026-07-01";
  const [targetName, subdirectory, failure, source, features, suffixes, producer] = configurations[kind];
  const output = resolve(destination), artifacts = join(output, "artifacts");
  const codec = join(root, "rust/crates/native-runtime/src", source);
  const oracle = join(root, "rust/crates/native-runtime/tests", subdirectory, "oracle.rs");
  const retained = join(root, "rust/crates/native-runtime/tests", subdirectory, "corpus");
  const original = readFileSync(codec), originalOracle = readFileSync(oracle);
  const retainedHashes = hashes(retained), oracleHash = digest(originalOracle);
  const lock = join(root, "Cargo.lock"), originalLock = readFileSync(lock);
  mkdirSync(dirname(output), { recursive: true });
  mkdirSync(output); // Refuse reuse of an existing evidence directory.
  mkdirSync(artifacts);
  let corpus = join(output, "corpus");
  const report = { codec: kind, mode: mode.replace(/^--/, ""), codec_sha256: digest(original), oracle_sha256: oracleHash, commands: [] };
  async function run(step, command, { negative = false, expectedFailure = failure, echo = true, timeoutControl = false } = {}) {
    const log = join(output, `${step}.log`), fd = openSync(log, "w");
    let status, signal;
    try {
      const child = spawn(command[0], command.slice(1), { cwd: root, stdio: ["ignore", "pipe", "pipe"] });
      const completion = once(child, "close");
      try {
        const outcomes = await Promise.all([completion, ...[child.stdout, child.stderr].map(async stream => {
          for await (const bytes of stream) {
            writeSync(fd, bytes);
            if (echo) process.stdout.write(bytes);
          }
        })]);
        [status, signal] = outcomes[0];
      } catch (error) {
        child.kill("SIGKILL");
        await completion.catch(() => {});
        throw error;
      }
    } catch (error) {
      throw new Error(`${step}: ${error.message}; recovery: inspect ${log}, correct source or command, then run ${JSON.stringify(command)}`);
    } finally { closeSync(fd); }
    const text = readFileSync(log, "utf8");
    const row = { step, argv: command, exit: status, signal, log: `${step}.log` };
    const coverage = [...text.matchAll(/cov: (\d+) ft: (\d+)/g)];
    if (coverage.length) row.instrumented_edges_features = coverage.at(-1).slice(1);
    report.commands.push(row);
    console.log(`${step}: exit=${status}; signal=${signal}; log=${log}`);
    const valid = negative
      ? (status !== 0 || signal !== null) && text.includes(expectedFailure) && (timeoutControl || text.includes("panicked at"))
      : status === 0 && signal === null;
    if (!valid) throw new Error(`${step}: deciding oracle failed; recovery: inspect ${log}, correct source or command, then run ${JSON.stringify(command)}`);
    return text;
  }
  const ordinary = ["cargo", "test", "--locked", "-p", "acyclic-native-runtime", "--test", targetName, "--", "--nocapture"];
  try {
    let command;
    if (routine || coverageOnly) {
      corpus = retained;
      command = ordinary;
    } else {
      if (reportPort) {
        cpSync(retained, corpus, { recursive: true });
        const request = readFileSync(join(retained, "request"));
        writeFileSync(join(corpus, "unread-delimiters"), Buffer.alloc(4096, request.at(-1)));
      } else if (owner) {
        await run("capture", ["cargo", "run", "--locked", "-p", "acyclic-native-runtime", "--example", "capture_owner_record", "--", corpus]);
      } else {
        const capture = join(output, process.platform === "win32" ? "capture-native-claim.exe" : "capture-native-claim");
        await run("compile-capture", ["rustc", "--edition=2024", "rust/fuzz/examples/capture_native_claim.rs", "-o", capture]);
        await run("capture", [capture, corpus]);
      }
      command = ["cargo", toolchain, "fuzz", "run", "--fuzz-dir", "rust/fuzz", "--dev", "--no-default-features", ...features, targetName, corpus, "--",
        `-runs=${owner || reportPort ? 1000 : 10000}`, "-seed=1", `-max_len=${reportPort ? 4096 : 8192}`, `-artifact_prefix=${artifacts}${sep}`];
    }
    report.corpus_path = corpus;
    report.corpus = Object.fromEntries(files(corpus).map(name => {
      const bytes = readFileSync(join(corpus, name));
      return [name, { bytes: bytes.length, sha256: digest(bytes), producer }];
    }));
    if (reportPort && !routine && !coverageOnly) {
      report.corpus["unread-delimiters"].producer = "4096 final delimiters derived from retained ReportRequest writer";
      const retirement = `    tokio::pin!(write, read);
    tokio::select! {
        // A decoded line consumes the port even when input remains unread.
        // Retire that input producer with the original request observer.
        observed = &mut read => observed,
        () = &mut write => read.await,
    }`;
      const mutant = replaceOnce(originalOracle, retirement, "    let ((), observed) = tokio::join!(write, read);\n    observed", "plant-writer-retirement");
      report.writer_retirement_mutant_sha256 = digest(mutant);
      writeFileSync(join(output, "writer-retirement-mutant.rs"), mutant);
      const blocked = command.map(arg => arg === corpus ? join(corpus, "unread-delimiters") : arg);
      try {
        writeFileSync(oracle, mutant);
        // libFuzzer detects the planted deadlock; restored success never waits on a timer.
        await run("writer-retirement-negative", [...blocked, "-runs=1", "-timeout=1"], { negative: true, expectedFailure: "ERROR: libFuzzer: timeout", timeoutControl: true });
      } finally { writeFileSync(oracle, originalOracle); }
    }
    if (owner && !routine) for (const name of files(corpus)) {
      console.log(`OWNER_CORPUS ${JSON.stringify({ name, ...report.corpus[name], bytes_hex: readFileSync(join(corpus, name)).toString("hex") })}`);
    }
    if (!coverageOnly) {
      const needle = owner ? "if magic != MAGIC || !bool::from(expected.ct_eq(digest(protected, meter)?.as_bytes())) {" : "if checksum(body) != sum {";
      const replacement = owner ? "if false && (magic != MAGIC || !bool::from(expected.ct_eq(digest(protected, meter)?.as_bytes()))) {" : "if false && checksum(body) != sum {";
      const mutations = reportPort ? [
        ["consumption", "        self.request = RequestState::Finished;\n        let mut line = String::new();", "        let mut line = String::new();", failure],
        ["identity", "if !identity(&request.request_id) || !identity(&request.generation) {", "if false && (!identity(&request.request_id) || !identity(&request.generation)) {", "report decoder admitted a noncanonical identity"],
        ["ready", "        self.request = RequestState::Ready;", "", "ready report port changed its original request outcome"],
      ] : [["integrity", needle, replacement, failure]];
      report.mutants = {};
      for (const [name, anchor, changed, marker] of mutations) {
        const mutant = replaceOnce(original, anchor, changed, `plant-${name}`);
        writeFileSync(join(output, `${name}-mutant.rs`), mutant);
        report.mutants[name] = digest(mutant);
        writeFileSync(codec, mutant);
        await run(`${name}-negative`, command, { negative: true, expectedFailure: marker });
      }
      writeFileSync(codec, original);
      const result = await run("positive", command);
      if (routine && !result.includes("test result: ok. 1 passed;")) throw new Error("routine-count: expected one ordinary corpus test; recovery: inspect positive.log and restore test discovery");
    }
    if (!routine) {
      if ((owner || reportPort) && !coverageOnly) {
        const result = await run("routine-positive", ordinary);
        if (!result.includes("test result: ok. 1 passed;")) throw new Error("routine-count: expected one ordinary corpus test; recovery: inspect routine-positive.log and restore test discovery");
      }
      const target = resolve(root, process.env.CARGO_TARGET_DIR ?? "target", "native-codec-coverage", targetName);
      report.coverage_target = target;
      if (owner) {
        report.coverage_corpus = hashes(corpus);
        for (const name of files(corpus)) console.log(`OWNER_COVERAGE_INPUT ${JSON.stringify({ name, sha256: report.coverage_corpus[name], bytes_hex: readFileSync(join(corpus, name)).toString("hex") })}`);
      }
      await run("coverage", ["cargo", toolchain, "fuzz", "coverage", "--fuzz-dir", "rust/fuzz", "--dev", "--no-default-features", ...features, "--target-dir", target, targetName, corpus]);
      const binaries = binariesBelow(target, targetName);
      if (binaries.length !== 1) throw new Error("coverage-binary: expected one instrumented target; recovery: inspect the coverage-build directory and correct binary selection");
      const sysroot = (await run("sysroot", ["rustc", toolchain, "--print", "sysroot"])).trim();
      const llvm = join(sysroot, "lib/rustlib/x86_64-unknown-linux-gnu/bin/llvm-cov");
      const profile = join(root, "rust/fuzz/coverage", targetName, "coverage.profdata");
      const coverage = JSON.parse(await run("coverage-export", [llvm, "export", binaries[0], `-instr-profile=${profile}`, "-summary-only"], { echo: false }));
      const rows = coverage.data.flatMap(entry => entry.files).filter(row => suffixes.some(suffix => row.filename.replaceAll("\\", "/").endsWith(suffix)));
      if (rows.length !== suffixes.length) throw new Error("coverage-codec: expected original codec rows; recovery: inspect coverage-export.log and correct source selection");
      report.codec_coverage = Object.fromEntries(rows.map(row => [row.filename, row.summary]));
    }
  } finally {
    writeFileSync(codec, original);
    const rewrittenLock = readFileSync(lock);
    writeFileSync(lock, originalLock);
    report.source_restored = readFileSync(codec).equals(original);
    report.oracle_unchanged = digest(readFileSync(oracle)) === oracleHash;
    report.retained_corpus_unchanged = equal(retainedHashes, hashes(retained));
    report.lock_semantics_unchanged = equal(lockContents(rewrittenLock), lockContents(originalLock));
    report.lock_reordered = !rewrittenLock.equals(originalLock);
    report.lock_restored = readFileSync(lock).equals(originalLock);
    writeFileSync(join(output, "report.json"), `${JSON.stringify(report, null, 2)}\n`);
  }
  if (!["source_restored", "oracle_unchanged", "retained_corpus_unchanged", "lock_semantics_unchanged", "lock_restored"].every(key => report[key])) {
    throw new Error("restore: source/oracle/corpus/lock mismatch; recovery: inspect report.json and restore the original checkout before further validation");
  }
  console.log(`NATIVE_CODEC_REPORT ${JSON.stringify(report)}`);
}
main().catch(error => {
  console.error(`native-codec-control: ${error.message}; recovery: inspect retained logs, correct the cause, then ${help}`);
  process.exitCode = 1;
});
