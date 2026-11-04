import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { cpSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { isWasmArtifactPath, preserveWasmArtifacts } from "./archive-utils.mjs";

const root = fileURLToPath(new URL("../", import.meta.url));
const args = process.argv.slice(2);
const red = args.includes("--red");
const retirement = args.includes("--retirement");
const bytes = args.includes("--bytes");
const metadata = args.includes("--metadata");
const array = args.includes("--array");
const number = args.includes("--number");
const sourceArgument = args.find(arg => arg.startsWith("--source="))?.slice("--source=".length);
const sources = sourceArgument === undefined ? [retirement ? "keys" : "tokens"] : sourceArgument.split(",");
const sourceKinds = retirement ? ["keys", "array", "prototype", "iterator", "callback", "getter", "number"]
  : ["tokens", "quoted", "canonical"];
const selectedControl = retirement || bytes || metadata || array || number;
const reuse = args.find(arg => arg.startsWith("--reuse="))?.slice("--reuse=".length);
const recovery = `node scripts/qualify-js-json.mjs${retirement ? " --retirement" : bytes ? " --bytes" : metadata ? " --metadata" : array ? " --array" : number ? " --number" : ""}${red ? " --red" : ""}${sourceArgument === undefined ? "" : ` --source=${sourceArgument}`}${reuse ? ` --reuse=${JSON.stringify(reuse)}` : ""}`;
const test = bytes ? "external JSON bytes preserve engine tokens or refuse before rounding"
  : retirement ? "canonical admission releases JavaScript references after admission failures"
  : "canonical admission selects entries before evaluating their values";
const testFile = `typescript/packages/harness/test/${bytes ? "json-bytes" : retirement ? "json-retirement" : "json-admission"}.test.ts`;
const fail = (step, detail, command = recovery) => {
  throw new Error(`${step}: ${detail}\nRecovery after correcting the cause: ${command}`);
};
const run = (step, command, parameters, options = {}) => {
  console.log(`${step}: ${JSON.stringify([command, ...parameters])}`);
  const result = spawnSync(command, parameters, { cwd: root, encoding: "utf8", ...options });
  if (result.error) fail(step, result.error.message);
  return result;
};
if (args.some(arg => !["--red", "--retirement", "--bytes", "--metadata", "--array", "--number"].includes(arg) && !arg.startsWith("--reuse=") && !arg.startsWith("--source="))
  || new Set(args).size !== args.length || Number(retirement) + Number(bytes) + Number(metadata) + Number(array) + Number(number) > 1
  || args.filter(arg => arg.startsWith("--reuse=")).length > 1 || args.includes("--reuse=") || reuse && red
  || args.filter(arg => arg.startsWith("--source=")).length > 1 || sourceArgument !== undefined && !(red && (retirement || bytes))
  || new Set(sources).size !== sources.length || sources.some(name => !sourceKinds.includes(name))
  || red && !selectedControl) {
  fail("arguments", "expected at most one control (--retirement, --bytes, --metadata, --array or --number); --red requires a control; --source selects distinct retirement sources (keys,array,prototype,iterator,callback,getter,number) or byte sources (tokens,quoted,canonical) and requires --red; --reuse=<receipt> excludes --red",
    "node scripts/qualify-js-json.mjs --array --red");
}

const production = new URL("../rust/crates/harness/src/wasm/json.rs", import.meta.url);
const generated = "typescript/packages/harness/generated/wasm";
function qualify(source) {
  let original;
  let mutation;
  let artifactsCustody;
  let reused;
  try {
    if (red) {
      try { original = readFileSync(production, "utf8"); }
      catch (error) { fail("read production admission source", String(error)); }
      const selected = retirement ? {
        keys: ["        for key in object_keys(value)?.iter() {",
          "        for key in js_sys::Object::keys(value.unchecked_ref::<js_sys::Object>()).iter() {"],
        array: ["    let array = is_array(value)?;", "    let array = js_sys::Array::is_array(value);"],
        prototype: ["    if !array && !is_map && !is_plain_json_object(value)? {",
          "    if !array && !is_map && !is_plain_json_object(value).unwrap_or_else(|error| wasm_bindgen::throw_val(error)) {"],
        iterator: ["        let items = js_sys::Array::from_iterable(value.unchecked_ref::<js_sys::Array>())?;",
          "        let items = js_sys::Array::from_iterable(value.unchecked_ref::<js_sys::Array>()).unwrap_or_else(|error| wasm_bindgen::throw_val(error));"],
        callback: ["        map.try_for_each(&mut |child, key| {\n            entries.push((key, child));\n            Ok(())\n        })?;",
          "        map.try_for_each(&mut |child, key| {\n            entries.push((key, child));\n            Ok(())\n        }).unwrap_or_else(|error| wasm_bindgen::throw_val(error));"],
        getter: ["            let child = js_sys::Reflect::get(value, &key)?;",
          "            let child = js_sys::Reflect::get(value, &key).unwrap_or_else(|error| wasm_bindgen::throw_val(error));"],
        number: ["            return Err(JsValue::from_str(\n                \"JSON contains an unsafe JavaScript Number\",\n            ));",
          "            wasm_bindgen::throw_val(JsValue::from_str(\"JSON contains an unsafe JavaScript Number\"));"],
      }[source] : undefined;
      const entrance = retirement ? selected[0]
        : number ? "        if !crate::conversation::is_exact_js_number(number) {"
        : array ? "        let items = js_sys::Array::from_iterable(value.unchecked_ref::<js_sys::Array>())?;"
        : metadata ? "        let mut entries = Vec::new();"
        : source === "canonical" ? "    if canonical != bytes {"
        : source === "quoted" ? "        if quoted {" : "    reject_out_of_range_integer_tokens(bytes)?;";
      if (original.split(entrance).length !== 2) fail("admission mutation", "expected exactly one production entrance");
      const replacement = retirement ? selected[1]
        : number ? "        if false && !crate::conversation::is_exact_js_number(number) {"
        : array ? "        let items = value.unchecked_ref::<js_sys::Array>();"
        : metadata ? `        let size = js_sys::Reflect::get(value, &JsValue::from_str("size"))?
            .as_f64()
            .filter(|size| (0.0..=f64::from(u32::MAX)).contains(size) && size.fract() == 0.0)
            .ok_or_else(|| JsValue::from_str("Map size must be an unsigned 32-bit integer"))?;
        let mut entries = Vec::with_capacity(size as usize);`
        : source === "canonical" ? "    if false && canonical != bytes {"
        : source === "quoted" ? "        if false && quoted {" : "";
      const planted = original.replace(entrance, replacement);
      mutation = {
        removed: entrance.trim(),
        replacement,
        original_sha256: createHash("sha256").update(original).digest("hex"),
        planted_sha256: createHash("sha256").update(planted).digest("hex"),
      };
      artifactsCustody = preserveWasmArtifacts(fileURLToPath(new URL(`../${generated}`, import.meta.url)),
        "node scripts/build-wasm.mjs harness");
      try { writeFileSync(join(artifactsCustody.directory, "json.rs"), planted); }
      catch (error) { fail("retain planted admission source", String(error)); }
      try { writeFileSync(production, planted); }
      catch (error) { fail("plant production admission defect", String(error)); }
    }

    // The full shipping Rust export graph, scoped to its one package. This check
    // imports only its generated module and initialization, so other frontend
    // packages and their artifacts are not prerequisites.
    const build = ["scripts/build-wasm.mjs", "harness"];
    if (reuse) {
      try {
        // A retained receipt's artifact paths are relative to its directory. Reuse
        // proves the selected control against that exact module, not a fresh build.
        const receiptPath = resolve(root, reuse);
        const directory = dirname(receiptPath);
        const receiptBytes = readFileSync(receiptPath);
        const receipt = JSON.parse(receiptBytes);
        if (receipt.result !== "pass" || !Array.isArray(receipt.artifacts)) fail("reuse receipt", "expected an original passing qualification");
        const hashes = new Map();
        const modules = new Map();
        for (const artifact of receipt.artifacts) {
          const path = resolve(directory, artifact.path);
          const offset = relative(directory, path);
          if (isAbsolute(offset) || offset === ".." || offset.startsWith(`..${sep}`)) fail("reuse artifact", "path leaves the receipt directory");
          const sha256 = createHash("sha256").update(readFileSync(path)).digest("hex");
          if (sha256 !== artifact.sha256 || hashes.has(artifact.path)) fail("reuse artifact", `hash or uniqueness differs: ${artifact.path}`);
          hashes.set(artifact.path, sha256);
          // Live artifacts name their module entry directly. Retained artifacts
          // keep that identity separately from their saved physical location.
          const modulePath = artifact.module_path ?? (artifact.path.startsWith(`${generated}/`)
            ? artifact.path.slice(generated.length + 1) : undefined);
          if (modulePath !== undefined) {
            const destination = relative(join(root, generated), resolve(root, generated, modulePath));
            if (!isWasmArtifactPath(modulePath) || isAbsolute(destination) || destination === ".."
              || destination.startsWith(`..${sep}`) || modules.has(modulePath)) fail("reuse module", `unsafe or duplicate module entry: ${modulePath}`);
            modules.set(modulePath, path);
          }
        }
        for (const path of ["acyclic_harness_wasm.js", "acyclic_harness_wasm_bg.wasm"]) {
          if (!modules.has(path)) fail("reuse module", `missing artifact: ${path}`);
        }
        const sourceHash = createHash("sha256").update(readFileSync(production)).digest("hex");
        if (sourceHash !== hashes.get("rust/crates/harness/src/wasm/json.rs")) fail("reuse source", "defining admission source differs");
        reused = { source: receipt.source, tree: receipt.tree,
          receipt_sha256: createHash("sha256").update(receiptBytes).digest("hex"),
          qualification: "retained shipping module; no current-tree build or complete dependency parity inferred" };
        artifactsCustody = preserveWasmArtifacts(join(root, generated), "node scripts/build-wasm.mjs harness");
        for (const [modulePath, path] of modules) {
          const destination = join(root, generated, modulePath);
          mkdirSync(dirname(destination), { recursive: true });
          cpSync(artifactsCustody.source(path), destination);
        }
      } catch (error) { fail("reuse retained module", String(error)); }
    } else {
      const built = run("generated harness", process.execPath, build, {
        env: { ...process.env, ACYCLIC_WASM_GROUPS_PREPARED: "1" }, stdio: "inherit",
      });
      if (built.status !== 0) fail("generated harness", `exit ${built.status}; signal ${built.signal}`);
    }
    const retirementFile = "typescript/packages/harness/test/json-retirement.test.ts";
    const restoredBoundary = (metadata || array) && !red;
    const parameters = ["test", `./${testFile}`, ...(restoredBoundary ? [`./${retirementFile}`] : []),
      "--test-name-pattern", restoredBoundary
        ? `^(?:${test}|canonical admission releases JavaScript references after admission failures)$` : `^${test}$`];
    const result = run("JSON admission", "bun", parameters);
    const output = `${result.stdout ?? ""}${result.stderr ?? ""}`;
    process.stdout.write(output);
    const expected = red
      ? result.status !== 0 && result.signal === null && output.includes(bytes
        ? source === "canonical" ? "canonical JSON admitted noncanonical bytes"
          : source === "quoted" ? "external JSON input was refused despite engine token admission"
          : "external JSON input admission disagrees with engine token oracle" : retirement
          ? "JavaScript refusal retained original input" : number
            ? "canonical admission accepted an unsafe JavaScript Number" : array
            ? "canonical admission did not retain selected Array entries"
            : "canonical Map admission evaluated unrelated size metadata")
        && /0 pass\b/u.test(output) && /1 fail\b/u.test(output)
      : result.status === 0 && new RegExp(`\\b${restoredBoundary ? 2 : 1} pass\\b`, "u").test(output) && /0 fail\b/u.test(output);
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
    if (restoredBoundary) artifacts.push(retirementFile);
    if (bytes) artifacts.push("rust/crates/harness-fuzz/corpus/retained_skill_records/materialized.json");
    const snippets = "typescript/packages/harness/generated/wasm/snippets";
    try {
      for (const path of readdirSync(new URL(`../${snippets}`, import.meta.url), { recursive: true, withFileTypes: true })) {
        if (path.isFile()) artifacts.push(`${path.parentPath}/${path.name}`.slice(root.length).replaceAll("\\", "/"));
      }
    } catch (error) { fail("generated module dependencies", String(error)); }
    const retainedPath = path => {
      if (!artifactsCustody) return path;
      const retained = path === "rust/crates/harness/src/wasm/json.rs" && !reused
        ? join(artifactsCustody.directory, "json.rs")
        : path.startsWith(`${generated}/`) ? artifactsCustody.retained(join(root, path)) : undefined;
      return retained === undefined ? path : relative(root, retained).replaceAll("\\", "/");
    };
    const receipt = `${JSON.stringify({
      source: git("HEAD"), tree: git("HEAD^{tree}"), build: reused ? undefined : build, reused, test: ["bun", ...parameters],
      result: red ? "expected semantic red" : "pass", mutation,
      artifacts: artifacts.map(path => ({
        path: retainedPath(path),
        module_path: path.startsWith(`${generated}/`) ? path.slice(generated.length + 1) : undefined,
        sha256: hash(path),
      })),
    }, null, 2)}\n`;
    const receiptName = number ? `javascript-json-number-${red ? "red" : "fixed"}.json`
      : array ? `javascript-json-array-${red ? "red" : "fixed"}.json`
      : metadata ? `javascript-json-metadata-${red ? "red" : "fixed"}.json`
      : bytes ? `javascript-json-bytes-${red && source !== "tokens" ? `${source}-` : ""}${red ? "red" : "fixed"}.json`
      : retirement ? `javascript-json-retirement-${red ? `${source}-red` : "fixed"}.json`
      : red ? "javascript-json-qualification.json" : "javascript-json-fixed-qualification.json";
    try { writeFileSync(new URL(`../${receiptName}`, import.meta.url), receipt); }
    catch (error) { fail("qualification receipt", String(error)); }
    console.log(red ? "PASS: original JSON admission defect reached the unchanged semantic assertion."
      : "PASS: generated JSON admission control passed.");
  } finally {
    try {
      if (original !== undefined) {
        try {
          writeFileSync(production, original);
          if (readFileSync(production, "utf8") !== original) fail("restore original admission source", "restored bytes differ");
          console.log("PASS: original admission source restored byte-exact.");
        }
        catch (error) { fail("restore original admission source", String(error)); }
      }
    } finally { artifactsCustody?.restore(); }
  }
}
for (const source of sources) qualify(source);
