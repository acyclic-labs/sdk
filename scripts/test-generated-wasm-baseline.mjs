import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { cpSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import test from "node:test";
import { snapshotCommittedWasm } from "./generated-wasm-baseline.mjs";

const scriptRoot = dirname(fileURLToPath(import.meta.url));
// Exercise the production comparison without invoking its Cargo/Buf producers.
const source = readFileSync(join(scriptRoot, "check-generated.mjs"), "utf8");
const start = source.indexOf("// Closure invoke names");
const end = source.indexOf("const temporary = mkdtempSync");
assert.ok(start >= 0 && end > start);
const comparison = source.slice(start, end);
assert.ok(comparison.includes("join(baseline.directory, packageName)"));

test("HEAD baseline rejects stale JS, declarations and WASM after worktree rebuild", async () => {
  const temporary = mkdtempSync(join(tmpdir(), "sdk-wasm-baseline-"));
  try {
    const root = join(temporary, "repo");
    const tracked = join(root, "typescript/packages/stream/generated/wasm");
    const fresh = join(temporary, "producer");
    mkdirSync(tracked, { recursive: true }); mkdirSync(fresh);
    writeFileSync(join(root, "package.json"), '{"type":"module"}\n');
    const basename = "acyclic_stream_wasm";
    const artifacts = {
      ".js": 'let exports;\nexport default async function init({ module_or_path }) { exports = (await WebAssembly.instantiate(module_or_path)).instance.exports; return exports; }\nexport function validatePath() { return exports.tag(); }\nexport function validateSequence() { return "0"; }\nexport function is_stream_error_code() { return false; }\n',
      ".d.ts": 'export function validatePath(): string;\n',
      "_bg.wasm.d.ts": 'export const memory: WebAssembly.Memory;\n',
      "_bg.wasm": Buffer.from([0,97,115,109,1,0,0,0,1,5,1,96,0,1,127,3,2,1,0,7,7,1,3,116,97,103,0,0,10,6,1,4,0,65,1,11]),
    };
    for (const [extension, bytes] of Object.entries(artifacts)) writeFileSync(join(fresh, basename + extension), bytes);
    writeFileSync(join(fresh, "package.json"), '{"type":"module"}\n');
    /** @param {...string} args */
    const git = (...args) => execFileSync("git", args, { cwd: root, stdio: "pipe" });
    git("init", "--quiet"); git("config", "user.name", "fixture"); git("config", "user.email", "fixture@example.invalid");
    for (const mutation of ["none", "JS", "declaration", "WASM"]) {
      cpSync(fresh, tracked, { recursive: true });
      if (mutation === "JS") writeFileSync(join(tracked, basename + ".js"), artifacts[".js"].replace('return exports.tag();', 'return 42;'));
      if (mutation === "declaration") writeFileSync(join(tracked, basename + ".d.ts"), 'export function retired(): number;\n');
      if (mutation === "WASM") {
        const stale = Buffer.from(artifacts["_bg.wasm"]); stale[stale.length - 2] = 2;
        assert.ok(WebAssembly.validate(stale));
        writeFileSync(join(tracked, basename + "_bg.wasm"), stale);
      }
      git("add", "."); git("-c", "commit.gpgsign=false", "commit", "--quiet", "--allow-empty", "-m", mutation);
      // Simulate the preceding build: tracked outputs now all look fresh.
      cpSync(fresh, tracked, { recursive: true });
      const directory = join(temporary, `baseline-${mutation}`);
      const baseline = snapshotCommittedWasm(root, directory, [["stream", basename]]);
      assert.equal(baseline.source, git("rev-parse", "HEAD").toString().trim());
      const outputRoot = join(temporary, `check-${mutation}`); mkdirSync(outputRoot);
      /** @param {string} _executable @param {string[]} args */
      const producer = (_executable, args) => {
        cpSync(fresh, args[2], { recursive: true });
        return { status: 0, stdout: "", stderr: "" };
      };
      const check = new Function("spawnSync", "root", "temporary", "baseline", "readFileSync", "join", "pathToFileURL", `${comparison}\nreturn checkWasmPackage;`)(producer, root, outputRoot, baseline, readFileSync, join, pathToFileURL);
      if (mutation === "none") await check(["stream", basename]);
      else await assert.rejects(check(["stream", basename]), mutation === "WASM" ? /WASM runtime semantics drift/ : /generated WASM .* drift/);
    }
    git("rm", "--quiet", "typescript/packages/stream/generated/wasm/acyclic_stream_wasm.d.ts");
    git("-c", "commit.gpgsign=false", "commit", "--quiet", "-m", "missing baseline");
    cpSync(fresh, tracked, { recursive: true });
    assert.throws(() => snapshotCommittedWasm(root, join(temporary, "missing"), [["stream", basename]]));
  } finally { rmSync(temporary, { recursive: true, force: true }); }
});

test("declaration-only and untracked packages validate fresh runtime without inventing HEAD blobs", async () => {
  const temporary = mkdtempSync(join(tmpdir(), "sdk-wasm-policy-"));
  try {
    const root = join(temporary, "repo"); mkdirSync(root);
    /** @param {...string} args */
    const git = (...args) => execFileSync("git", args, { cwd: root, stdio: "pipe" });
    git("init", "--quiet"); git("config", "user.name", "fixture"); git("config", "user.email", "fixture@example.invalid");
    const declaration = "export function decodeAggregateKind(): number;\n";
    const harness = join(root, "typescript/packages/harness/generated/wasm"); mkdirSync(harness, { recursive: true });
    writeFileSync(join(harness, "acyclic_harness_wasm.d.ts"), declaration);
    writeFileSync(join(harness, "acyclic_harness_wasm_bg.wasm.d.ts"), declaration);
    git("add", "."); git("-c", "commit.gpgsign=false", "commit", "--quiet", "-m", "declarations only");
    const packages = /** @type {[string, string][]} */ ([
      ["harness", "acyclic_harness_wasm"], ["objects", "acyclic_objects_wasm"],
      ["machines", "acyclic_machines_wasm"], ["inference", "acyclic_inference_wasm"],
    ]);
    const routes = new Function(`${comparison}\nreturn expectedMachinesRoutes;`)();
    let fault = "none";
    const baseline = snapshotCommittedWasm(root, join(temporary, "baseline"), packages);
    assert.deepEqual(baseline.extensions.harness, [".d.ts", "_bg.wasm.d.ts"]);
    assert.deepEqual(baseline.extensions.objects, []);
    /** @param {string} _executable @param {string[]} args */
    const producer = (_executable, args) => {
      const [, name, output] = args;
      const basename = packages.find(([key]) => key === name)?.[1]; assert.ok(basename);
      mkdirSync(output, { recursive: true });
      writeFileSync(join(output, "package.json"), '{"type":"module"}\n');
      writeFileSync(join(output, basename + ".js"), `
export default async function init() { return {}; }
export function decodeAggregateKind() { return 1; }
export function objects_v1_http_type(route, output) {
  if (route !== "objects/get") { if (${JSON.stringify(fault)} === "objects-route") return "GetObjectRequest"; throw Error("invalid"); }
  if (${JSON.stringify(fault)} === "objects-type") return 42;
  return output ? "GetObjectResponse" : "GetObjectRequest";
}
export function httpRoutes() { return ${fault === "machines-map" ? '{MACHINES_CREATE:"retired"}' : JSON.stringify(routes)}; }
export function validate_customer_wire(kind, bytes) {
  if (${JSON.stringify(fault)} === "inference-accept") return;
  if (kind !== "mutation_receipt" || bytes.length !== 70 || bytes[69] !== 1) throw Error("invalid");
  if (${JSON.stringify(fault)} === "inference-result") return {};
}
`);
      writeFileSync(join(output, basename + ".d.ts"), declaration);
      writeFileSync(join(output, basename + "_bg.wasm.d.ts"), declaration);
      writeFileSync(join(output, basename + "_bg.wasm"), Buffer.from([0,97,115,109,1,0,0,0]));
      return { status: 0, stdout: "", stderr: "" };
    };
    const check = new Function("spawnSync", "root", "temporary", "baseline", "readFileSync", "join", "pathToFileURL", `${comparison}\nreturn checkWasmPackage;`)(producer, root, temporary, baseline, readFileSync, join, pathToFileURL);
    for (const pair of packages) await check(pair);
    for (const [mode, name] of [["objects-type", "objects"], ["objects-route", "objects"], ["machines-map", "machines"], ["inference-accept", "inference"], ["inference-result", "inference"]]) {
      fault = mode;
      const faultRoot = join(temporary, mode); mkdirSync(faultRoot);
      const faultCheck = new Function("spawnSync", "root", "temporary", "baseline", "readFileSync", "join", "pathToFileURL", `${comparison}\nreturn checkWasmPackage;`)(producer, root, faultRoot, baseline, readFileSync, join, pathToFileURL);
      const pair = packages.find(([key]) => key === name); assert.ok(pair);
      await assert.rejects(faultCheck(pair), /contract differs|invalid contract vector|malformed success/);
    }
    fault = "none";
    writeFileSync(join(baseline.directory, "harness", "acyclic_harness_wasm.d.ts"), "export function retired(): boolean;\n");
    await assert.rejects(check(packages[0]), /generated WASM .d.ts drift/);
    assert.throws(() => snapshotCommittedWasm(root, join(temporary, "unknown"), [["unknown", "unknown_wasm"]]), /unknown WASM baseline policy/);
  } finally { rmSync(temporary, { recursive: true, force: true }); }
});
