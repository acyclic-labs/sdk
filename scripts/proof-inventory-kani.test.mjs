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
});

test("declared proofs are grouped by the unit Kani compiles them in", t => {
  const root = mkdtempSync(join(tmpdir(), "kani-units-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const files = {
    "lib/src/lib.rs": "#[cfg(kani)]\nmod proofs {\n    #[kani::proof]\n    #[kani::unwind(2)]\n    fn bounded() {}\n    // #[kani::proof]\n    const S: &str = \"#[kani::proof] fn quoted() {}\";\n}\n",
    "unit/src/lib.rs": "#[cfg(test)]\nmod checks {\n    #[kani::proof_for_contract(crate::f)]\n    fn contract() {}\n}\n",
    "both/src/lib.rs": "#[kani::proof]\nfn library() {}\n",
    "both/tests/target.rs": "#[kani::proof]\n#[kani::should_panic]\nfn refuted() {}\n",
    "both/tests/formal/standalone.rs": "#[kani::proof]\npub fn standalone() {}\n",
    "both/src/plain.rs": "fn ordinary() {}\n",
  };
  for (const [path, text] of Object.entries(files)) {
    mkdirSync(dirname(join(root, path)), { recursive: true });
    writeFileSync(join(root, path), text);
  }
  const library = name => ({ kind: ["lib"], src_path: join(root, name, "src/lib.rs") });
  const metadata = { packages: [
    { name: "lib", targets: [library("lib")] },
    { name: "unit", targets: [library("unit")] },
    { name: "both", targets: [library("both"), { kind: ["test"], src_path: join(root, "both/tests/target.rs") }] },
  ] };
  const units = scanner("kani_units(*args)", root, metadata, Object.keys(files));
  assert.equal(units.status, 0, units.stderr);
  assert.deepEqual(JSON.parse(units.stdout), [
    { unit: "file:both/tests/formal/standalone.rs", file: "both/tests/formal/standalone.rs", files: ["both/tests/formal/standalone.rs"], proofs: ["standalone"] },
    { unit: "package:both", package: "both", tests: true, files: ["both/src/lib.rs", "both/tests/target.rs"], proofs: ["library", "refuted"] },
    { unit: "package:lib", package: "lib", tests: false, files: ["lib/src/lib.rs"], proofs: ["bounded"] },
    { unit: "package:unit", package: "unit", tests: true, files: ["unit/src/lib.rs"], proofs: ["contract"] },
  ]);
  writeFileSync(join(root, "both/src/plain.rs"), "#[kani::proof]\nconst NOT_A_PROOF: u8 = 0;\n");
  const refused = scanner("kani_units(*args)", root, metadata, ["both/src/plain.rs"]);
  assert.notEqual(refused.status, 0);
  assert.match(refused.stderr, /both\/src\/plain\.rs:1: Kani proof attribute precedes no function/);
});
