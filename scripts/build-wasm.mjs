import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { cpSync, existsSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { packageClosure } from "./package-closure.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
// Manual caller-array copies must stay behind the admitted byte snapshot.
const assertByteSnapshotBoundary = (file, source) => {
  // These existing expressions copy Rust-owned digests/requests, never JS arrays.
  const rustOwned = source
    .replaceAll("cursor.commit.to_vec()", "")
    .replaceAll("prepared.bytes().to_vec()", "")
    .replaceAll(".as_bytes()\n        .to_vec()", "")
    .replaceAll(".map(|digest| digest.to_vec())", "")
    .replace(/(canonical_json_digest\([^;]+?)\.to_vec\(\)/g, "$1");
  if (/\.\s*(?:to_vec|copy_to|copy_to_uninit|raw_copy_to_ptr)\s*\(|Uint8Array::new\s*\(/.test(rustOwned)) {
    throw new Error(`${file}: copy caller arrays through wasm/bytes.rs`);
  }
};
const checkByteSnapshotBoundary = directory => {
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const file = join(directory, entry.name);
    if (entry.isDirectory()) checkByteSnapshotBoundary(file);
    else if (entry.name.endsWith(".rs")) assertByteSnapshotBoundary(file, readFileSync(file, "utf8").replaceAll("\r\n", "\n"));
  }
};
checkByteSnapshotBoundary(join(root, "rust/crates/harness/src/wasm"));

const usage = "usage: build-wasm.mjs [PACKAGE [OUTPUT_DIR [CARGO [WASM_BINDGEN]]]]";

const closureInvokeShim = /^\s*(?:(?:readonly|export const)\s+)?wasm_bindgen_(?:[0-9a-f]+)?_+convert__closures_+invoke_[^:\s]*:.*\r?\n/gm;
const stripHarnessClosureShims = output => {
  for (const name of ["acyclic_harness_wasm.d.ts", "acyclic_harness_wasm_bg.wasm.d.ts"]) {
    const declaration = resolve(output, name);
    const source = readFileSync(declaration, "utf8");
    const normalized = source.replace(closureInvokeShim, "");
    if (normalized !== source) writeFileSync(declaration, normalized);
  }
};

const narrowMachinesDecoder = output => {
  const declarationPath = resolve(output, "acyclic_machines_wasm.d.ts");
  const declaration = readFileSync(declarationPath, "utf8");
  const decoder = "static decodeHttpResponse(route: MachinesHttpRoute, response_json: string, expected_json: string): MachinesHttpResponseUnion;";
  if (!declaration.includes(decoder)) {
    throw new Error("Machines WASM declaration no longer contains the expected HTTP decoder signature");
  }
  writeFileSync(
    declarationPath,
    declaration.replace(
      decoder,
      "static decodeHttpResponse<Route extends MachinesHttpRoute>(route: Route, response_json: string, expected_json: string): MachinesHttpResponse<Route>;",
    ),
  );
};

const packages = {
  auth: { cargo: ["-p", "acyclic-auth-wasm"], artifact: "acyclic_auth_wasm" },
  actors: { family: true },
  filesystem: { cargo: ["-p", "acyclic-fs-wasm"], artifact: "acyclic_fs_wasm" },
  stream: { family: true },
  objects: { cargo: ["-p", "acyclic-objects-wasm"], artifact: "acyclic_objects_wasm" },
  manifest: { cargo: ["-p", "acyclic-manifest"], artifact: "acyclic_manifest", outputPackage: "workers" },
  machines: {
    cargo: ["-p", "acyclic-machines-wasm"],
    artifact: "acyclic_machines_wasm",
    postprocess: narrowMachinesDecoder,
  },
  inference: { cargo: ["-p", "acyclic-inference-wasm"], artifact: "acyclic_inference_wasm" },
  harness: {
    cargo: ["-p", "acyclic-harness-providers", "--features", "browser"],
    artifact: "acyclic_harness_providers",
    outName: "acyclic_harness_wasm",
    postprocess: stripHarnessClosureShims,
    targetDirectory: "harness-wasm",
  },
};

const [packageArgument, outputArgument, cargoArgument, wasmBindgenArgument, ...unexpected] = process.argv.slice(2);
if (unexpected.length > 0 || (packageArgument && !Object.hasOwn(packages, packageArgument)
  && (!/^[a-z][a-z-]*$/.test(packageArgument) || !existsSync(join(root, "typescript/packages", packageArgument, "package.json"))))) {
  throw new Error(`${usage}\nPACKAGE is one of: ${Object.keys(packages).join(", ")}`);
}

const cargo = cargoArgument || process.env.ACYCLIC_CARGO_BIN || "cargo";
const capture = (executable, args) => {
  const result = spawnSync(executable, args, { cwd: root, encoding: "utf8", windowsHide: true });
  return result.error || result.status !== 0 ? undefined : result.stdout.trim();
};
const run = (executable, args) => {
  const result = spawnSync(executable, args, { cwd: root, stdio: "inherit", windowsHide: true });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`${executable} exited with ${result.status ?? "unknown"}`);
};

// The CLI must match the wasm-bindgen crate pinned in the workspace manifest.
const pinned = readFileSync(resolve(root, "Cargo.toml"), "utf8").match(/^wasm-bindgen = "=([^"]+)"/m)?.[1];
if (!pinned) throw new Error("Cargo.toml no longer pins wasm-bindgen to an exact version");
const expectedVersion = `wasm-bindgen ${pinned}`;
const cargoHome = resolve(process.env.CARGO_HOME ?? join(homedir(), ".cargo"));
const resolveWasmBindgen = () => {
  const explicit = wasmBindgenArgument || process.env.ACYCLIC_WASM_BINDGEN_BIN;
  if (explicit) return explicit;
  if (capture("wasm-bindgen", ["--version"]) === expectedVersion) return "wasm-bindgen";
  if (process.platform === "win32") return resolve(cargoHome, "bin", "wasm-bindgen.exe");
  return capture("bash", [resolve(root, "scripts", "ensure-wasm-bindgen.sh")]);
};
const wasmBindgen = resolveWasmBindgen();
if (!wasmBindgen || capture(wasmBindgen, ["--version"]) !== expectedVersion) {
  throw new Error(`${expectedVersion} is required to build WASM packages`);
}

const metadata = capture(cargo, ["metadata", "--locked", "--no-deps", "--format-version", "1"]);
if (!metadata) throw new Error("cargo metadata failed");
const targetRoot = JSON.parse(metadata).target_directory;
const profile = process.env.ACYCLIC_WASM_PROFILE || "wasm-release";
const profileDirectory = profile === "dev" ? "debug" : profile;

const script = readFileSync(fileURLToPath(import.meta.url));

const grouped = process.env.ACYCLIC_WASM_GROUPS_PREPARED === "1";
const targets = grouped ? [packageArgument] : packageArgument
  ? [...packageClosure(root, packageArgument === "manifest" ? "workers" : packageArgument)]
    .map(name => name === "workers" ? "manifest" : name).filter(name => Object.hasOwn(packages, name))
  : Object.keys(packages);
if (!grouped) run(process.execPath, [join(root, "scripts/generate.mjs"), ...(packageArgument ? [packageArgument === "manifest" ? "workers" : packageArgument] : [])]);

for (const name of targets) {
  let { cargo: selection, artifact, outName = artifact, postprocess, targetDirectory } = packages[name];
  let family, producer, source, revision;
  if (packages[name].family) {
    const { nativeFamily } = await import("./native-family.mjs");
    const { createNativeProducer } = await import("./build-native-family.mjs");
    family = nativeFamily(name);
    ({ artifact, outName, targetDirectory } = family.wasm);
    selection = ["-p", family.wasmPackageName, ...(family.wasm.noDefaultFeatures ? ["--no-default-features"] : []), ...(family.wasm.features.length ? ["--features", family.wasm.features.join(",")] : [])];
    producer = createNativeProducer(family);
    producer.assertCurrentNativeFamily();
    revision = capture("git", ["rev-parse", "HEAD"]);
    const status = capture("git", ["status", "--porcelain=v1", "--untracked-files=all", "--", ...family.sourceRoots]);
    if (!/^[0-9a-f]{40,64}$/u.test(revision ?? "") || status !== "") throw new Error("WASM requires a clean source closure");
    source = await producer.sourceSnapshot();
  }
  const output = outputArgument && name === packageArgument ? resolve(outputArgument) : resolve(root, "typescript/packages", packages[name].outputPackage ?? name, "generated/wasm");
  const target = targetDirectory ? resolve(targetRoot, targetDirectory) : targetRoot;
  const rustflags = [
    "--remap-path-prefix", `${root}=.`,
    "--remap-path-prefix", `${target}=/cargo/build-dir`,
    "--remap-path-prefix", `${cargoHome}=/cargo/home`,
  ];
  const buildArgs = [
    "build", ...selection, "--target", "wasm32-unknown-unknown", "--profile", profile, "--locked",
    "--target-dir", target, "--config", `target.wasm32-unknown-unknown.rustflags = ${JSON.stringify(rustflags)}`,
  ];
  let compilerInvocation, originalReceipt;
  const cachedReceipt = resolve(target, "wasm-bindgen", `${name}.producer-receipt.json`);
  const rustcCapture = producer ? await producer.createRustcInvocationCapture(artifact) : undefined;
  try {
    run(cargo, buildArgs);
    if (rustcCapture) {
      try { compilerInvocation = await rustcCapture.read("wasm32-unknown-unknown"); }
      catch (error) {
        if (!/captured 0 Stream rustc link invocations/u.test(String(error?.message)) || !existsSync(cachedReceipt)) throw error;
        originalReceipt = JSON.parse(readFileSync(cachedReceipt, "utf8"));
      }
    }
  } finally { await rustcCapture?.close(); }
  const module = resolve(target, "wasm32-unknown-unknown", profileDirectory, `${artifact}.wasm`);
  const bindings = resolve(target, "wasm-bindgen", name);
  const stamp = `${bindings}.sha256`;
  const key = createHash("sha256").update(script).update(expectedVersion).update(readFileSync(module)).digest("hex");
  if (!existsSync(stamp) || readFileSync(stamp, "utf8") !== key) {
    for (const stale of [stamp, bindings]) rmSync(stale, { recursive: true, force: true });
    run(wasmBindgen, [
      module, "--target", "web", "--out-dir", bindings, "--out-name", outName,
      "--remove-name-section", "--remove-producers-section",
    ]);
    postprocess?.(bindings);
    writeFileSync(stamp, key);
  }
  mkdirSync(output, { recursive: true });
  cpSync(bindings, output, { recursive: true });
  if (producer) {
    await producer.assertSourceSnapshot(source);
    if (capture("git", ["rev-parse", "HEAD"]) !== revision) throw new Error("WASM source revision changed during build");
    const artifacts = readdirSync(bindings, { withFileTypes: true }).map(entry => {
      if (!entry.isFile() || entry.isSymbolicLink()) throw new Error("WASM bindings contain a non-regular artifact");
      const bytes = readFileSync(resolve(output, entry.name));
      return { path: `generated/wasm/${entry.name}`, bytes: bytes.length, sha256: `sha256:${createHash("sha256").update(bytes).digest("hex")}` };
    }).sort((a, b) => a.path < b.path ? -1 : a.path > b.path ? 1 : 0);
    const moduleSha256 = `sha256:${createHash("sha256").update(readFileSync(module)).digest("hex")}`;
    const receipt = originalReceipt ?? {
      schema: "acyclic.wasm-build-receipt.v1", family: name, package: family.wasmPackageName, version: family.version,
      source_commit: revision, source_sha256: source.sha256, source_files: source.files,
      build: { cargo: { command: cargo, version: capture(cargo, ["--version"]), args: buildArgs }, rustc: { invocation: compilerInvocation, identity: producer.capturedCompilerIdentity(compilerInvocation, "wasm32-unknown-unknown") }, wasm_bindgen: { command: wasmBindgen, version: expectedVersion }, module_sha256: moduleSha256 },
      artifacts,
    };
    if (!receipt.build?.cargo?.version || !receipt.build?.rustc?.identity?.output) throw new Error("WASM compiler identity is missing");
    if (originalReceipt) {
      const actualCompiler = producer.capturedCompilerIdentity(receipt.build.rustc.invocation, "wasm32-unknown-unknown");
      if (receipt.schema !== "acyclic.wasm-build-receipt.v1" || receipt.family !== name || receipt.package !== family.wasmPackageName || receipt.version !== family.version || !/^[0-9a-f]{40,64}$/u.test(receipt.source_commit ?? "") || receipt.source_sha256 !== source.sha256 || JSON.stringify(receipt.source_files) !== JSON.stringify(source.files) || receipt.build.module_sha256 !== moduleSha256 || JSON.stringify(receipt.build.cargo) !== JSON.stringify({ command: cargo, version: capture(cargo, ["--version"]), args: buildArgs }) || JSON.stringify(receipt.build.rustc.identity) !== JSON.stringify(actualCompiler) || JSON.stringify(receipt.build.wasm_bindgen) !== JSON.stringify({ command: wasmBindgen, version: expectedVersion }) || JSON.stringify(receipt.artifacts) !== JSON.stringify(artifacts)) throw new Error("Cached WASM compiler receipt differs; a fresh owned target build is required");
      receipt.source_commit = revision;
    }
    const receiptBytes = `${JSON.stringify(receipt, null, 2)}\n`;
    writeFileSync(resolve(output, "producer-receipt.json"), receiptBytes);
    writeFileSync(cachedReceipt, receiptBytes);
  }
}
