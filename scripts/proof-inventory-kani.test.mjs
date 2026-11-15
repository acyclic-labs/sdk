import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const scripts = dirname(fileURLToPath(import.meta.url));

// Calls the scanner's own functions, so these tests judge exactly what a proof run is judged by.
function scanner(call, ...args) {
  const program = `import json,sys\nsys.path.insert(0,sys.argv[1])\nfrom proof_inventory_scan import kani_units,kani_judgment\nargs=[json.loads(a) for a in sys.argv[2:]]\nprint(json.dumps(${call}))`;
  return spawnSync("python3", ["-I", "-B", "-c", program, scripts, ...args.map(arg => JSON.stringify(arg))], { encoding: "utf8" });
}

function judge(proofs, output) {
  const result = scanner("kani_judgment(*args)", proofs, output);
  assert.equal(result.status, 0, result.stderr);
  return JSON.parse(result.stdout);
}

const run = (...harnesses) => harnesses.map(([name, result]) => `Checking harness ${name}...\nVERIFICATION:- ${result}\n`).join("");

test("every declared proof runs once and succeeds, or the run fails", () => {
  assert.equal(judge(["a", "b"], run(["proofs::a", "SUCCESSFUL"], ["proofs::b", "SUCCESSFUL"])).passed, true);
  const failed = judge(["a", "b"], run(["proofs::a", "SUCCESSFUL"], ["proofs::b", "FAILED"]));
  assert.deepEqual([failed.passed, failed.outcomes[1]], [false, { harness: "proofs::b", result: "FAILED" }]);
  const missing = judge(["a", "b"], run(["proofs::a", "SUCCESSFUL"]));
  assert.deepEqual([missing.passed, missing.outcomes.at(-1)], [false, { harness: "b", result: "MISSING" }]);
  assert.deepEqual(judge([], ""), { outcomes: [], passed: false });
  const repeated = judge(["a"], run(["proofs::a", "SUCCESSFUL"], ["proofs::a", "SUCCESSFUL"]));
  assert.deepEqual([repeated.passed, repeated.outcomes.at(-1)], [false, { harness: "a", result: "REPEATED" }]);
  const unexpected = judge(["a"], run(["proofs::a", "SUCCESSFUL"], ["proofs::z", "SUCCESSFUL"]));
  assert.deepEqual([unexpected.passed, unexpected.outcomes[1]], [false, { harness: "proofs::z", result: "UNEXPECTED" }]);
  // A verdict printed before any harness is checked belongs to no proof.
  assert.equal(judge(["a"], "VERIFICATION:- SUCCESSFUL\n").passed, false);
  // A should_panic proof verifies by finding its counterexample, and Kani says so after the verdict.
  const refuted = "Checking harness proofs::a...\nVERIFICATION:- SUCCESSFUL (encountered one or more panics as expected)\n";
  assert.equal(judge(["a"], refuted).passed, true);
});

test("declared proofs are grouped by the unit Kani compiles them in", t => {
  const root = mkdtempSync(join(tmpdir(), "kani-units-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const files = {
    "lib/src/lib.rs": "#[cfg(kani)]\nmod proofs {\n    #[kani::proof]\n    #[kani::unwind(2)]\n    fn bounded() {}\n    // #[kani::proof]\n    const S: &str = \"#[kani::proof] fn quoted() {}\";\n}\n#[cfg(test)]\nmod tests;\n#[cfg(any(test, kani))]\nmod both_builds {\n    #[kani::proof]\n    fn either() {}\n}\n",
    // A proof declared through cfg_attr is a proof under that cfg.
    "both/src/lib.rs": "#[kani::proof]\nfn library() {}\n#[cfg_attr(all(kani, unix), kani::proof)]\n#[cfg_attr(not(kani), test)]\nfn attributed() {}\n",
    // Declared by a macro the scanner cannot expand; the library's source directory holds it.
    "both/src/generated.rs": "#[kani::proof]\nfn generated() {}\n",
    "both/tests/target.rs": "mod helper;\n#[path = \"shared/linked.rs\"]\nmod linked;\n",
    "both/tests/helper.rs": "fn helper() {}\n",
    "both/tests/shared/linked.rs": "include!(\"spliced.rs\");\n",
    "both/tests/shared/spliced.rs": "fn spliced() {}\n",
    "both/tests/formal/standalone.rs": "#[kani::proof]\npub fn standalone() {}\n",
    "both/src/plain.rs": "fn ordinary() {}\n",
    "unit/src/lib.rs": "fn checked() {}\n",
  };
  const write = (path, text) => {
    mkdirSync(dirname(join(root, path)), { recursive: true });
    writeFileSync(join(root, path), text);
  };
  for (const [path, text] of Object.entries(files)) write(path, text);
  const library = name => ({ kind: ["lib"], src_path: join(root, name, "src/lib.rs") });
  const metadata = { packages: [
    { name: "lib", targets: [library("lib")] },
    { name: "unit", targets: [library("unit")] },
    { name: "both", targets: [library("both"), { kind: ["test"], src_path: join(root, "both/tests/target.rs") }] },
  ] };
  const units = paths => scanner("kani_units(*args)", root, metadata, paths ?? Object.keys(files));
  const grouped = units();
  assert.equal(grouped.status, 0, grouped.stderr);
  assert.deepEqual(JSON.parse(grouped.stdout), [
    { unit: "file:both/tests/formal/standalone.rs", file: "both/tests/formal/standalone.rs", files: ["both/tests/formal/standalone.rs"], proofs: ["standalone"] },
    { unit: "package:both", package: "both", files: ["both/src/lib.rs", "both/src/generated.rs"], proofs: ["library", "attributed", "generated"] },
    { unit: "package:lib", package: "lib", files: ["lib/src/lib.rs"], proofs: ["bounded", "either"] },
  ]);

  // A test build compiles the package's dev-dependencies, so a proof only it compiles is refused by name:
  // reached through a test target's mod, its #[path] module's include!, or under cfg(test) in the library.
  write("both/tests/helper.rs", "#[kani::proof]\nfn helper() {}\n");
  write("both/tests/target.rs", "mod helper;\n#[path = \"shared/linked.rs\"]\nmod linked;\n#[kani::proof]\n#[kani::should_panic]\nfn refuted() {}\n");
  write("both/tests/shared/spliced.rs", "#[kani::proof]\nfn spliced() {}\n");
  write("unit/src/lib.rs", "#[cfg(test)]\nmod checks {\n    #[kani::proof_for_contract(crate::f)]\n    fn contract() {}\n}\n#[cfg(all(test, unix))]\n#[kani::proof]\nfn narrowed() {}\n#[cfg_attr(test, kani::proof)]\nfn attributed() {}\n");
  const refused = units();
  assert.notEqual(refused.status, 0);
  assert.match(refused.stderr, /KANI_PROOF_IN_TEST_TARGET/);
  for (const proof of [/both\/tests\/target\.rs:4 refuted \(package both\)/, /both\/tests\/helper\.rs:1 helper \(package both\)/, /both\/tests\/shared\/spliced\.rs:1 spliced \(package both\)/,
    /unit\/src\/lib\.rs:3 contract \(package unit\)/, /unit\/src\/lib\.rs:7 narrowed \(package unit\)/,
    /unit\/src\/lib\.rs:9 attributed \(package unit\)/]) assert.match(refused.stderr, proof);
  // The library's own proofs and the unconditional ones beside the refused ones are not reported.
  assert.doesNotMatch(refused.stderr, / (?:bounded|either|library|generated|standalone) \(package/);

  write("both/src/plain.rs", "#[kani::proof]\nconst NOT_A_PROOF: u8 = 0;\n");
  const unattached = units(["both/src/plain.rs"]);
  assert.notEqual(unattached.status, 0);
  assert.match(unattached.stderr, /both\/src\/plain\.rs:1: Kani proof attribute precedes no function/);
});
