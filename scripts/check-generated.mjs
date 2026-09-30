import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { compatibilityArtifacts, generatedDescriptors, nativeWasmVector, normalizeGeneratedRust, normalizeGeneratedTypeScript, packagedRustBindings, packagedSourceCopies, packagedTypeScriptBindings } from "./generated-bindings.mjs";
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
const wasmSmoke = {
  filesystem: module => typeof module.openMemoryFs === "function",
  harness: module => module.decodeAggregateKind(1),
  inference: module => typeof module.validate_customer_wire === "function",
  machines: module => module.httpRoutes(),
  objects: module => module.objects_v2_http_type("objects/get", false),
  stream: module => {
    if (module.is_stream_error_code("retired")) {
      throw new Error("Stream WASM still accepts a retired-path error");
    }
    return [module.validatePath("/check-generated"), module.validateSequence("0")];
  },
};
const wasmPackages = [
  ["filesystem", "build-filesystem-wasm.mjs", "acyclic_fs_wasm"],
  ["harness", "build-harness-wasm.mjs", "acyclic_harness_wasm"],
  ["inference", "build-inference-wasm.mjs", "acyclic_inference_wasm"],
  ["machines", "build-machines-wasm.mjs", "acyclic_machines_wasm"],
  ["objects", "build-objects-wasm.mjs", "acyclic_objects_wasm"],
  ["stream", "build-stream-wasm.mjs", "acyclic_stream_wasm"],
];
const checkWasmPackage = async ([packageName, buildScript, basename]) => {
  const output = join(temporary, `${packageName}-wasm`);
  const built = spawnSync(process.execPath, [join(root, "scripts", buildScript), output], {
    cwd: root,
    encoding: "utf8",
  });
  if (built.status !== 0) {
    process.stderr.write(built.stdout ?? "");
    process.stderr.write(built.stderr ?? "");
    throw new Error(`${packageName} WASM rebuild failed with status ${built.status ?? "unknown"}`);
  }
  const packageRoot = join(root, `typescript/packages/${packageName}/generated/wasm`);
  for (const extension of [".js", ".d.ts", "_bg.wasm.d.ts"]) {
    const fresh = readFileSync(join(output, `${basename}${extension}`), "utf8");
    const committed = readFileSync(join(packageRoot, `${basename}${extension}`), "utf8");
    const normalizedFresh = extension === ".js" ? canonicalGeneratedJs(fresh) : declarationBlocks(fresh).join("\n");
    const normalizedCommitted = extension === ".js" ? canonicalGeneratedJs(committed) : declarationBlocks(committed).join("\n");
    if (normalizedFresh !== normalizedCommitted) {
      throw new Error(`generated WASM ${extension} drift: ${packageName}`);
    }
  }
  const freshWasm = readFileSync(join(output, `${basename}_bg.wasm`));
  const committedWasm = readFileSync(join(packageRoot, `${basename}_bg.wasm`));
  if (!WebAssembly.validate(freshWasm) || !WebAssembly.validate(committedWasm)) {
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
  const smoke = wasmSmoke[packageName];
  if (smoke) {
    const freshSmoke = runtimeFingerprint(await smoke(freshRuntime.module));
    const committedSmoke = runtimeFingerprint(await smoke(committedRuntime.module));
    if (JSON.stringify(freshSmoke) !== JSON.stringify(committedSmoke)) {
      throw new Error(`packaged ${packageName} WASM runtime semantics drift`);
    }
  }
};


const temporary = mkdtempSync(join(tmpdir(), "acyclic-sdk-codegen-"));
try {
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
  const committedTypeScript = join(root, "generated/typescript");
  const freshFiles = generatedFiles(freshTypeScript);
  if (JSON.stringify(freshFiles) !== JSON.stringify(generatedFiles(committedTypeScript))) {
    throw new Error("generated TypeScript file set drift; run bun run generate");
  }
  for (const relative of freshFiles) {
    const fresh = normalizeGeneratedTypeScript(readFileSync(join(freshTypeScript, relative), "utf8"));
    const committed = readFileSync(join(committedTypeScript, relative), "utf8");
    if (fresh !== committed) throw new Error(`generated TypeScript drift: ${relative}`);
  }
  const freshRust = join(temporary, "generated/rust");
  const committedRust = join(root, "generated/rust");
  const freshRustFiles = generatedFiles(freshRust);
  if (JSON.stringify(freshRustFiles) !== JSON.stringify(generatedFiles(committedRust))) {
    throw new Error("generated Rust file set drift; run bun run generate");
  }
  for (const relative of freshRustFiles) {
    const fresh = normalizeGeneratedRust(relative.replaceAll("\\", "/"), readFileSync(join(freshRust, relative), "utf8"));
    const committed = readFileSync(join(committedRust, relative), "utf8");
    if (fresh !== committed) throw new Error(`generated Rust drift: ${relative}`);
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
  for (const [relative, declaration] of [
    ["typescript/packages/filesystem/generated/descriptor-digest.js", false],
    ["typescript/packages/filesystem/generated/descriptor-digest.d.ts", true],
  ]) {
    const expected = filesystemDescriptorDigestSource(root, declaration);
    if (readFileSync(join(root, relative), "utf8") !== expected) {
      throw new Error(`generated filesystem descriptor digest drift: ${relative}`);
    }
  }

  for (const [relative, packaged] of packagedRustBindings) {
    const canonical = readFileSync(join(committedRust, relative));
    if (!canonical.equals(readFileSync(join(root, packaged)))) {
      throw new Error(`packaged Rust drift: ${relative}`);
    }
  }

  for (const [stem, packages] of packagedTypeScriptBindings) {
    for (const extension of [".js", ".d.ts"]) {
      const relative = `${stem}${extension}`;
      const canonical = readFileSync(join(committedTypeScript, relative));
      for (const name of packages) {
        const packaged = readFileSync(join(root, "typescript/packages", name, "generated/proto", relative));
        if (!canonical.equals(packaged)) throw new Error(`packaged ${name} TypeScript drift: ${relative}`);
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
