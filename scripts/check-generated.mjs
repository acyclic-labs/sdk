import { serviceGenerate } from "./generate-actors.mjs";
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, readdirSync, realpathSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { compatibilityArtifacts, generatedDescriptors, nativeWasmVector, normalizeGeneratedRust, normalizeGeneratedTypeScript, packagedRustBindings, packagedSourceCopies, packagedTypeScriptBindings } from "./generated-bindings.mjs";
import { filesystemDescriptorDigestSource } from "./filesystem-descriptor-digest.mjs";
import { nativeFamily } from "./native-family.mjs";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const args = process.argv.slice(2);
if (args.some(arg => arg !== "--source-only") || args.length > 1) throw new Error("usage: check-generated.mjs [--source-only]");
const sourceOnly = args.includes("--source-only");
for (const key of ["actors", "workers", "stream"]) {
  const facts = spawnSync("cargo", ["run", "--offline", "--locked", "--quiet", "-p", "sdk-proto-codegen", "--", "native-family", root, key], { cwd: root, encoding: "utf8" });
  if (facts.error) throw facts.error;
  if (facts.status !== 0) throw new Error(facts.stderr || `native-family ${key} generation failed`);
  const expected = `${JSON.stringify(JSON.parse(facts.stdout), null, 2)}\n`;
  if (readFileSync(join(root, "scripts/generated/native-families", `${key}.json`), "utf8") !== expected) throw new Error(`Rust native-family ${key} generation drift`);
}
const actors = spawnSync(process.execPath, [join(root, "scripts", "generate-actors.mjs"), "check"], {
  cwd: root,
  encoding: "utf8",
});
if (actors.status !== 0) {
  process.stderr.write(actors.stdout ?? "");
  process.stderr.write(actors.stderr ?? "");
  throw new Error(`Actors Rust generation drift check failed with status ${actors.status ?? "unknown"}`);
}
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
/** @type {[string, string][]} */
const wasmPackages = [
  ["filesystem", "acyclic_fs_wasm"],
  ["harness", "acyclic_harness_wasm"],
  ["inference", "acyclic_inference_wasm"],
  ["machines", "acyclic_machines_wasm"],
  ["objects", "acyclic_objects_wasm"],
  ...["actors", "workers", "stream"].map(key => /** @type {[string, string]} */ ([key, nativeFamily(key).wasm.outName])),
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


// Keep temporary consumers under the workspace so maintained dependencies resolve
// through its installed node_modules without copying or inventing codec packages.
const temporary = mkdtempSync(join(root, "target-sdk-codegen-"));
try {
  const readonly = spawnSync("cargo", ["run", "--offline", "--locked", "--quiet", "-p", "sdk-proto-codegen", "--", "readonly"], { cwd: root, encoding: "utf8" });
  if (readonly.error) throw readonly.error;
  if (readonly.status !== 0) throw new Error(`Rust readonly projection failed: ${readonly.stderr}`);
  for (const family of ["actors", "workers"]) if (readFileSync(join(root, `typescript/packages/${family}/src/generated/readonly.ts`), "utf8") !== readonly.stdout) throw new Error(`${family} readonly projection drift`);
  const readonlyTypes = join(temporary, "readonly-types");
  mkdirSync(readonlyTypes);
  // Bun's isolated install keeps protobuf in the consuming package's
  // node_modules. Resolve the fixture against those maintained dependencies.
  mkdirSync(join(readonlyTypes, "node_modules/@bufbuild"), { recursive: true });
  symlinkSync(realpathSync(join(root, "typescript/packages/actors/node_modules/@bufbuild/protobuf")), join(readonlyTypes, "node_modules/@bufbuild/protobuf"), process.platform === "win32" ? "junction" : "dir");
  writeFileSync(join(readonlyTypes, "readonly.ts"), readonly.stdout);
  writeFileSync(join(readonlyTypes, "consumer.ts"), readFileSync(join(root, "rust/crates/proto-codegen/tests/readonly-consumer.ts")));
  const readonlyConsumer = spawnSync(process.execPath, [join(root, "node_modules/typescript/bin/tsc"), "--ignoreConfig", "--noEmit", "--strict", "--target", "ES2023", "--module", "NodeNext", "--moduleResolution", "NodeNext", "--pretty", "false", join(readonlyTypes, "consumer.ts")], { cwd: root, encoding: "utf8" });
  if (readonlyConsumer.error) throw readonlyConsumer.error;
  if (readonlyConsumer.status !== 0) throw new Error(`readonly TypeScript consumer failed: ${readonlyConsumer.stdout}${readonlyConsumer.stderr}`);
  const freshWorkers = join(temporary, "workers-semantic");
  const freshWorkersProto = join(temporary, "workers-proto");
  const workers = spawnSync("cargo", ["run", "--offline", "--locked", "-p", "acyclic-workers", "--example", "workers-http-routes", "--", freshWorkersProto, freshWorkers], { cwd: root, encoding: "utf8" });
  if (workers.error) throw workers.error;
  if (workers.status !== 0) throw new Error(`Workers Rust generation failed: ${workers.stderr}`);
  const formattedWorkers = spawnSync(join(root, "node_modules/.bin", process.platform === "win32" ? "buf.exe" : "buf"), ["format", "-w", join(freshWorkersProto, "workers/v1/workers.proto")], { cwd: root, encoding: "utf8" });
  if (formattedWorkers.error) throw formattedWorkers.error;
  if (formattedWorkers.status !== 0) throw new Error(`Workers Proto formatting failed: ${formattedWorkers.stderr}`);
  if (!readFileSync(join(freshWorkersProto, "workers/v1/workers.proto")).equals(readFileSync(join(root, "proto/workers/v1/workers.proto")))) throw new Error("Workers Rust-rendered Proto drift");
  const generatedService = join(temporary, "workers-service.ts");
  await serviceGenerate("Workers", temporary, generatedService);
  if (!readFileSync(generatedService).equals(readFileSync(join(root, "typescript/packages/workers/src/generated/workers-service.ts")))) throw new Error("Workers semantic service drift");
  if (!readFileSync(join(temporary, "workers-binding.ts")).equals(readFileSync(join(root, "typescript/packages/workers/src/generated/workers-binding.ts")))) throw new Error("Workers binding identity drift");
  if (!readFileSync(join(temporary, "native-absence.ts")).equals(readFileSync(join(root, "typescript/packages/workers/src/generated/native-absence.ts")))) throw new Error("Workers native absence projection drift");
  const packagedWorkers = join(root, "typescript/packages/workers/src/generated/semantic");
  const expectedWorkers = generatedFiles(freshWorkers);
  if (JSON.stringify(expectedWorkers) !== JSON.stringify(generatedFiles(packagedWorkers))) throw new Error("Workers semantic TypeScript file set drift");
  for (const file of expectedWorkers) if (!readFileSync(join(freshWorkers, file)).equals(readFileSync(join(packagedWorkers, file)))) throw new Error(`Workers semantic TypeScript drift: ${file}`);
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
  for (const wasmPackage of sourceOnly ? [] : wasmPackages) {
    await checkWasmPackage(wasmPackage);
  }
} finally {
  rmSync(temporary, { recursive: true, force: true });
}
