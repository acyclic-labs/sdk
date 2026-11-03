import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const args = process.argv.slice(2);
const red = args.length === 1 && args[0] === "--red";
const recovery = `node scripts/qualify-js-json.mjs${red ? " --red" : ""}`;
const test = "canonical admission selects Map entries before evaluating their values";
const testFile = "typescript/packages/harness/test/json-admission.test.ts";
const fail = (step, detail) => {
  console.error(`${step}: ${detail}\nRecovery after correcting the cause: ${recovery}`);
  process.exit(1);
};
const run = (step, command, parameters, options = {}) => {
  console.log(`${step}: ${JSON.stringify([command, ...parameters])}`);
  const result = spawnSync(command, parameters, { cwd: root, encoding: "utf8", ...options });
  if (result.error) fail(step, result.error.message);
  return result;
};
if (args.length && !red) fail("arguments", "expected no arguments or --red");

// The full shipping Rust export graph, scoped to its one package. This check
// imports only its generated module and initialization, so other frontend
// packages and their artifacts are not prerequisites.
const build = ["scripts/build-wasm.mjs", "harness"];
const built = run("generated harness", process.execPath, build, {
  env: { ...process.env, ACYCLIC_WASM_GROUPS_PREPARED: "1" }, stdio: "inherit",
});
if (built.status !== 0) fail("generated harness", `exit ${built.status}; signal ${built.signal}`);
const parameters = ["test", testFile, "--test-name-pattern", `^${test}$`];
const result = run("Map admission", "bun", parameters);
const output = `${result.stdout ?? ""}${result.stderr ?? ""}`;
process.stdout.write(output);
const expected = red
  ? result.status !== 0 && result.signal === null && output.includes("canonical admission revisited an appended Map entry")
    && /0 pass\b/u.test(output) && /1 fail\b/u.test(output)
  : result.status === 0 && /1 pass\b/u.test(output) && /0 fail\b/u.test(output);
if (!expected) fail("Map admission", "the exact unchanged semantic control did not produce the required result");
const hash = path => {
  try { return createHash("sha256").update(readFileSync(new URL(`../${path}`, import.meta.url))).digest("hex"); }
  catch (error) { fail(`artifact ${path}`, String(error)); }
};
const git = field => {
  const result = run(`source ${field}`, "git", ["rev-parse", field]);
  if (result.status !== 0) fail(`source ${field}`, `exit ${result.status}`);
  return result.stdout.trim();
};
const artifacts = [testFile, "rust/crates/harness/src/wasm/json.rs",
  "typescript/packages/harness/generated/wasm/acyclic_harness_wasm.js",
  "typescript/packages/harness/generated/wasm/acyclic_harness_wasm_bg.wasm"];
const receipt = `${JSON.stringify({
  source: git("HEAD"), tree: git("HEAD^{tree}"), build, test: ["bun", ...parameters],
  result: red ? "expected semantic red" : "pass", artifacts: artifacts.map(path => ({ path, sha256: hash(path) })),
}, null, 2)}\n`;
try { writeFileSync(new URL("../javascript-json-qualification.json", import.meta.url), receipt); }
catch (error) { fail("qualification receipt", String(error)); }
console.log(red ? "PASS: original Map traversal defect reached the unchanged semantic assertion."
  : "PASS: generated Map admission control passed.");
