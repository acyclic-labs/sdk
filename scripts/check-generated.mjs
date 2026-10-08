import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { compatibilityArtifacts, generatedDescriptors, nativeWasmVector, normalizeGeneratedRust, normalizeGeneratedTypeScript, packagedRustBindings, packagedSourceCopies, packagedTypeScriptBindings } from "./generated-bindings.mjs";
import { snapshotCommittedWasm } from "./generated-wasm-baseline.mjs";
import { filesystemDescriptorDigestSource } from "./filesystem-descriptor-digest.mjs";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const generatedFiles = directory => {
  const files = [];
  const visit = (current, prefix) => {
    for (const entry of readdirSync(current, { withFileTypes: true })) {
      const relative = join(prefix, entry.name);
      if (entry.isDirectory()) visit(join(current, entry.name), relative);
      else if (entry.isFile()) files.push(relative);
    }
  };
  visit(directory, "");
  return files.sort();
};
// Closure invoke names include private crate build hashes. Rust 1.98 expands
// those names to include the closure's type, but the hashes still vary by host.
const wasmPrivateClosureName = /wasm_bindgen__convert__closures_____invoke__h[0-9a-f]+|wasm_bindgen_[0-9a-f]{8,16}___convert__closures[A-Za-z0-9_]*/g;
const canonicalPrivateClosureName = name => name.startsWith("wasm_bindgen__")
  ? "wasm_bindgen__convert__closures_____invoke__h<private>"
  : name.replace(/(wasm_bindgen|js_sys|web_sys|core)_[0-9a-f]{8,16}(?=_)/g, "$1_<private>");
const canonicalGeneratedJs = source => source
  .replace(wasmPrivateClosureName, canonicalPrivateClosureName)
  .replace(/shim_idx: \d+/g, "shim_idx: <private>");
const declarationBlocks = source => source
  .split(/\r?\n/)
  .reduce((blocks, line) => {
    if (line.startsWith("export ")) blocks.push(line);
    else if (blocks.length > 0) blocks[blocks.length - 1] += `\n${line}`;
    return blocks;
  }, [])
  .map(block => block
    .replace(wasmPrivateClosureName, canonicalPrivateClosureName)
    .replace(/\s+/g, " ")
    .trim())
  .sort();
const runtimeFingerprint = value => {
  if (typeof value === "bigint") return `${value}n`;
  if (value instanceof Uint8Array) return [...value];
  if (Array.isArray(value)) return value.map(runtimeFingerprint);
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).sort(([left], [right]) => left.localeCompare(right))
        .map(([key, item]) => [key, runtimeFingerprint(item)]),
    );
  }
  return value;
};
// Frozen smoke vectors from the current Rust route and protobuf contracts.
const expectedMachinesRoutes = {
  "IMAGES_QUALIFY": "images/qualify",
  "MACHINES_CREATE": "machines/create",
  "MACHINES_INSPECT": "machines/inspect",
  "MACHINES_LIST": "machines/list",
  "CHECKPOINTS_INSPECT": "checkpoints/inspect",
  "MACHINES_CHECKPOINT": "machines/checkpoint",
  "MACHINES_FORK": "machines/fork",
  "CHECKPOINTS_FORK": "checkpoints/fork",
  "MACHINES_SUSPEND": "machines/suspend",
  "MACHINES_WAKE": "machines/wake",
  "MACHINES_SUSPENSION_POLICY": "machines/suspension-policy",
  "MACHINES_DESTROY": "machines/destroy",
  "CHECKPOINTS_DESTROY": "checkpoints/destroy",
  "MACHINES_EVENTS": "machines/events",
  "MACHINES_USAGE": "machines/usage",
  "OPERATIONS_RECOVER": "operations/recover",
  "OPERATIONS_RECOVER_ID": "operations/recover-id",
  "OPERATIONS_INSPECT": "operations/inspect",
  "OPERATIONS_CANCEL": "operations/cancel",
  "OPERATIONS_WATCH": "operations/watch"
};
/** @param {() => unknown} invoke @param {string} label */
const requireRejected = (invoke, label) => {
  let rejected = false;
  try { invoke(); } catch { rejected = true; }
  if (!rejected) throw new Error(label + " accepted an invalid contract vector");
};
const wasmSmoke = {
  filesystem: module => typeof module.openMemoryFs === "function",
  harness: module => module.decodeAggregateKind(1),
  inference: module => {
    const empty = new Uint8Array();
    // MutationReceipt: nonzero 32-byte revision/digest and publication sequence 1.
    const receipt = new Uint8Array([10, 32, ...new Array(32).fill(1), 18, 32, ...new Array(32).fill(2), 24, 1]);
    if (module.validate_customer_wire("mutation_receipt", receipt, empty, empty) !== undefined) {
      throw new Error("Inference validator returned a malformed success result");
    }
    for (const invalid of [empty, new Uint8Array([255]), receipt.slice(0, -2)]) {
      requireRejected(() => module.validate_customer_wire("mutation_receipt", invalid, empty, empty), "Inference receipt");
    }
    requireRejected(() => module.validate_customer_wire("unknown", receipt, empty, empty), "Inference kind");
    return true;
  },
  machines: module => {
    const routes = module.httpRoutes();
    if (routes === null || typeof routes !== "object" || Array.isArray(routes)
      || JSON.stringify(runtimeFingerprint(routes)) !== JSON.stringify(runtimeFingerprint(expectedMachinesRoutes))) {
      throw new Error("Machines HTTP route contract differs");
    }
    return routes;
  },
  objects: module => {
    const input = module.objects_v1_http_type("objects/get", false);
    const output = module.objects_v1_http_type("objects/get", true);
    if (input !== "GetObjectRequest" || output !== "GetObjectResponse") {
      throw new Error("Objects v1 get route contract differs");
    }
    requireRejected(() => module.objects_v1_http_type("retired", false), "Objects v1 route");
    return [input, output];
  },
  stream: module => {
    if (module.is_stream_error_code("retired")) {
      throw new Error("Stream WASM still accepts a retired-path error");
    }
    return [module.validatePath("/check-generated"), module.validateSequence("0")];
  },
};
/** @type {[string, string][]} */
const wasmPackages = [
  ["filesystem", "acyclic_fs_wasm"],
  ["harness", "acyclic_harness_wasm"],
  ["inference", "acyclic_inference_wasm"],
  ["machines", "acyclic_machines_wasm"],
  ["objects", "acyclic_objects_wasm"],
  ["stream", "acyclic_stream_wasm"],
];
const checkWasmPackage = async ([packageName, basename]) => {
  const output = join(temporary, `${packageName}-wasm`);
  const built = spawnSync(process.execPath, [join(root, "scripts", "build-wasm.mjs"), packageName, output], {
    cwd: root,
    encoding: "utf8",
  });
  if (built.status !== 0) {
    process.stderr.write(built.stdout ?? "");
    process.stderr.write(built.stderr ?? "");
    throw new Error(`${packageName} WASM rebuild failed with status ${built.status ?? "unknown"}`);
  }
  const packageRoot = join(baseline.directory, packageName);
  const tracked = baseline.extensions[packageName];
  for (const extension of [".js", ".d.ts", "_bg.wasm.d.ts"]) {
    const fresh = readFileSync(join(output, `${basename}${extension}`), "utf8");
    if (!tracked.includes(extension)) continue;
    const committed = readFileSync(join(packageRoot, `${basename}${extension}`), "utf8");
    const normalizedFresh = extension === ".js" ? canonicalGeneratedJs(fresh) : declarationBlocks(fresh).join("\n");
    const normalizedCommitted = extension === ".js" ? canonicalGeneratedJs(committed) : declarationBlocks(committed).join("\n");
    if (normalizedFresh !== normalizedCommitted) {
      throw new Error(`generated WASM ${extension} drift: ${packageName}`);
    }
  }
  const freshWasm = readFileSync(join(output, `${basename}_bg.wasm`));
  const committedWasm = tracked.includes("_bg.wasm")
    ? readFileSync(join(packageRoot, `${basename}_bg.wasm`)) : null;
  if (!WebAssembly.validate(freshWasm) || (committedWasm && !WebAssembly.validate(committedWasm))) {
    throw new Error(`packaged ${packageName} WASM failed validation`);
  }
  const load = async (directory, wasm) => {
    const module = await import(pathToFileURL(join(directory, `${basename}.js`)).href);
    const initialized = await module.default({ module_or_path: wasm });
    if (initialized === undefined || initialized === null) {
      throw new Error(`packaged ${packageName} WASM initialization returned no exports`);
    }
    return { module, initialized };
  };
  const freshRuntime = await load(output, freshWasm);
  const smoke = wasmSmoke[packageName];
  const freshSmoke = smoke ? runtimeFingerprint(await smoke(freshRuntime.module)) : undefined;
  if (freshSmoke === false) throw new Error(`packaged ${packageName} WASM smoke failed`);
  if (!committedWasm) {
    console.log(`${packageName}: fresh WASM runtime validated; ${tracked.length} tracked declarations compared with HEAD ${baseline.source}`);
    return;
  }
  const committedRuntime = await load(packageRoot, committedWasm);
  const freshExports = Object.keys(freshRuntime.module).sort();
  const committedExports = Object.keys(committedRuntime.module).sort();
  if (JSON.stringify(freshExports) !== JSON.stringify(committedExports)) {
    throw new Error(`packaged ${packageName} WASM API drift`);
  }
  for (const name of freshExports) {
    if (typeof freshRuntime.module[name] !== typeof committedRuntime.module[name]) {
      throw new Error(`packaged ${packageName} WASM export kind drift: ${name}`);
    }
  }
  if (smoke) {
    const committedSmoke = runtimeFingerprint(await smoke(committedRuntime.module));
    if (JSON.stringify(freshSmoke) !== JSON.stringify(committedSmoke)) {
      throw new Error(`packaged ${packageName} WASM runtime semantics drift`);
    }
  }
};


const temporary = mkdtempSync(join(tmpdir(), "acyclic-sdk-codegen-"));
/** @type {ReturnType<typeof snapshotCommittedWasm>} */
let baseline;
try {
  // Read every tracked WASM baseline even if an earlier build rewrote the worktree.
  baseline = snapshotCommittedWasm(root, join(temporary, "committed-wasm"), wasmPackages);
  for (const [source, packaged] of packagedSourceCopies) {
    if (!readFileSync(join(root, source)).equals(readFileSync(join(root, packaged)))) {
      throw new Error(`packaged source drift: ${packaged}`);
    }
  }
  const harnessSuite = JSON.parse(readFileSync(join(root, compatibilityArtifacts.harness.conformanceDigest), "utf8"));
  const nativeWasmCase = harnessSuite.cases.find(item => item.name === "native-wasm-replay-is-byte-equivalent");
  const vector = JSON.parse(readFileSync(join(root, nativeWasmVector), "utf8"));
  if (JSON.stringify(nativeWasmCase?.vector) !== JSON.stringify(vector)) {
    throw new Error("native/WASM fixture is not bound into the Harness suite");
  }

  const executable = join(root, "node_modules", ".bin", process.platform === "win32" ? "buf.exe" : "buf");
  const generated = spawnSync(
    executable,
    ["generate", "--output", temporary],
    { cwd: root, encoding: "utf8" },
  );
  if (generated.status !== 0) {
    process.stderr.write(generated.stdout ?? "");
    process.stderr.write(generated.stderr ?? "");
    throw new Error(`Buf generation failed with status ${generated.status ?? "unknown"}`);
  }
  const freshTypeScript = join(temporary, "generated/typescript");
  const freshRust = join(temporary, "generated/rust");
  for (const [relative, packaged] of packagedRustBindings) {
    const fresh = normalizeGeneratedRust(relative, readFileSync(join(freshRust, relative), "utf8"));
    if (fresh !== readFileSync(join(root, packaged), "utf8")) throw new Error(`packaged Rust drift: ${packaged}`);
  }
  for (const [source, destination] of generatedDescriptors) {
    const descriptor = join(temporary, destination.replaceAll("/", "-"));
    const built = spawnSync(executable, ["build", "--path", source, "-o", descriptor], {
      cwd: root,
      encoding: "utf8",
    });
    if (built.status !== 0) {
      process.stderr.write(built.stdout ?? "");
      process.stderr.write(built.stderr ?? "");
      throw new Error(`Buf descriptor build failed for ${source}: ${built.status ?? "unknown"}`);
    }
    if (!readFileSync(descriptor).equals(readFileSync(join(root, destination)))) {
      throw new Error(`generated descriptor drift: ${destination}`);
    }
  }
  for (const declaration of [false, true]) {
    const relative = `typescript/packages/filesystem/generated/descriptor-digest.${declaration ? "d.ts" : "js"}`;
    const expected = filesystemDescriptorDigestSource(root, declaration);
    if (readFileSync(join(root, relative), "utf8") !== expected) {
      throw new Error(`generated filesystem descriptor digest drift: ${relative}`);
    }
  }

  for (const [stem, packages] of packagedTypeScriptBindings) {
    for (const extension of [".js", ".d.ts"]) {
      const relative = `${stem}${extension}`;
      const fresh = normalizeGeneratedTypeScript(readFileSync(join(freshTypeScript, relative), "utf8"));
      for (const name of packages) {
        const packaged = readFileSync(join(root, "typescript/packages", name, "generated/proto", relative), "utf8");
        if (fresh !== packaged) throw new Error(`packaged ${name} TypeScript drift: ${relative}`);
      }
    }
  }
  for (const name of new Set(packagedTypeScriptBindings.flatMap(([, packages]) => packages))) {
    const expected = packagedTypeScriptBindings
      .filter(([, packages]) => packages.includes(name))
      .flatMap(([stem]) => [`.d.ts`, `.js`].map(extension => `${stem}${extension}`))
      .sort();
    const actual = generatedFiles(join(root, "typescript/packages", name, "generated/proto"))
      .map(path => path.replaceAll("\\", "/"))
      .sort();
    if (JSON.stringify(actual) !== JSON.stringify(expected)) {
      throw new Error(`packaged ${name} TypeScript file set drift`);
    }
  }
  for (const wasmPackage of wasmPackages) {
    await checkWasmPackage(wasmPackage);
  }
} finally {
  rmSync(temporary, { recursive: true, force: true });
}
