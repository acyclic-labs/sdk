import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const args = process.argv.slice(2);
const red = args.includes("--red");
const retirement = args.includes("--retirement");
const bytes = args.includes("--bytes");
const recovery = `node scripts/qualify-js-json.mjs${retirement ? " --retirement" : bytes ? " --bytes" : ""}${red ? " --red" : ""}`;
const test = bytes ? "external JSON bytes preserve engine tokens or refuse before rounding"
  : retirement ? "canonical admission releases JavaScript references after intrinsic failures"
  : "canonical admission selects Map entries before evaluating their values";
const testFile = `typescript/packages/harness/test/${bytes ? "json-bytes" : retirement ? "json-retirement" : "json-admission"}.test.ts`;
const fail = (step, detail) => {
  throw new Error(`${step}: ${detail}\nRecovery after correcting the cause: ${recovery}`);
};
const run = (step, command, parameters, options = {}) => {
  console.log(`${step}: ${JSON.stringify([command, ...parameters])}`);
  const result = spawnSync(command, parameters, { cwd: root, encoding: "utf8", ...options });
  if (result.error) fail(step, result.error.message);
  return result;
};
if (args.some(arg => !["--red", "--retirement", "--bytes"].includes(arg))
  || new Set(args).size !== args.length || retirement && bytes) {
  fail("arguments", "expected one control (--retirement or --bytes) and optional --red once each");
}

const production = new URL("../rust/crates/harness/src/wasm/json.rs", import.meta.url);
let original;
let mutation;
try {
  if (bytes && red) {
    try { original = readFileSync(production, "utf8"); }
    catch (error) { fail("read production integer guard", String(error)); }
    const entrance = "    reject_out_of_range_integer_tokens(bytes)?;";
    if (original.split(entrance).length !== 2) fail("integer guard mutation", "expected exactly one production guard entrance");
    const planted = original.replace(entrance, "");
    mutation = {
      removed: entrance.trim(),
      original_sha256: createHash("sha256").update(original).digest("hex"),
      planted_sha256: createHash("sha256").update(planted).digest("hex"),
    };
    try { writeFileSync(production, planted); }
    catch (error) { fail("plant production integer guard defect", String(error)); }
  }

  // The full shipping Rust export graph, scoped to its one package. This check
  // imports only its generated module and initialization, so other frontend
  // packages and their artifacts are not prerequisites.
  const build = ["scripts/build-wasm.mjs", "harness"];
  const built = run("generated harness", process.execPath, build, {
    env: { ...process.env, ACYCLIC_WASM_GROUPS_PREPARED: "1" }, stdio: "inherit",
  });
  if (built.status !== 0) fail("generated harness", `exit ${built.status}; signal ${built.signal}`);
  const parameters = ["test", testFile, "--test-name-pattern", `^${test}$`];
  const result = run("JSON admission", "bun", parameters);
  const output = `${result.stdout ?? ""}${result.stderr ?? ""}`;
  process.stdout.write(output);
  const expected = red
    ? result.status !== 0 && result.signal === null && output.includes(bytes
      ? "external JSON input admission disagrees with engine token oracle" : retirement
        ? "JavaScript refusal retained original input" : "canonical admission revisited an appended Map entry")
      && /0 pass\b/u.test(output) && /1 fail\b/u.test(output)
    : result.status === 0 && /1 pass\b/u.test(output) && /0 fail\b/u.test(output);
  if (!expected) fail("JSON admission", "the exact unchanged semantic control did not produce the required result");
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
  if (bytes) artifacts.push("rust/crates/harness-fuzz/corpus/retained_skill_records/materialized.json");
  const snippets = "typescript/packages/harness/generated/wasm/snippets";
  try {
    for (const path of readdirSync(new URL(`../${snippets}`, import.meta.url), { recursive: true, withFileTypes: true })) {
      if (path.isFile()) artifacts.push(`${path.parentPath}/${path.name}`.slice(root.length).replaceAll("\\", "/"));
    }
  } catch (error) { fail("generated module dependencies", String(error)); }
  const receipt = `${JSON.stringify({
    source: git("HEAD"), tree: git("HEAD^{tree}"), build, test: ["bun", ...parameters],
    result: red ? "expected semantic red" : "pass", mutation,
    artifacts: artifacts.map(path => ({ path, sha256: hash(path) })),
  }, null, 2)}\n`;
  const receiptName = bytes ? `javascript-json-bytes-${red ? "red" : "fixed"}.json`
    : retirement ? `javascript-json-retirement-${red ? "red" : "fixed"}.json`
    : red ? "javascript-json-qualification.json" : "javascript-json-fixed-qualification.json";
  try { writeFileSync(new URL(`../${receiptName}`, import.meta.url), receipt); }
  catch (error) { fail("qualification receipt", String(error)); }
  console.log(red ? "PASS: original JSON admission defect reached the unchanged semantic assertion."
    : "PASS: generated JSON admission control passed.");
} finally {
  if (original !== undefined) {
    try {
      writeFileSync(production, original);
      if (readFileSync(production, "utf8") !== original) fail("restore original integer guard source", "restored bytes differ");
      console.log("PASS: original integer guard source restored byte-exact.");
    }
    catch (error) { fail("restore original integer guard source", String(error)); }
  }
}
