import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { delimiter, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { copyGeneratedFiles, runCli, generationOutput, inventory, loadAuthority, sha256, stageDescriptorSets } from "../../../shared/authority.mjs";

const directory = dirname(fileURLToPath(import.meta.url));
const pinned = JSON.parse(readFileSync(join(directory, "../toolchains/toolchain.json"), "utf8"));

// The command and toolchain injection are for offline staging controls. The CLI
// always uses spawnSync and the checked-in C++ plugin build pins.
export function generate(args, { command = spawnSync, toolchain = pinned } = {}) {
  const source = realpathSync(args["source-root"]);
  const authority = realpathSync(args.authority);
  const protoc = realpathSync(args.protoc);
  const plugin = realpathSync(args["cpp-plugin"]);
  const output = generationOutput(args.output, [source, authority, protoc, plugin, dirname(directory)]);
  const { bytes: manifestBytes, manifest, inputs } = loadAuthority(authority);
  const inputHashes = Object.fromEntries([...inputs].map(([name, bytes]) => [name, sha256(bytes)]));
  const producerFiles = ["generate.mjs", "../../../shared/authority.mjs", "../toolchains/toolchain.json", "../toolchains/build-runtime.cmake"];
  const producerBytes = Object.fromEntries(producerFiles.map(name => [name, readFileSync(join(directory, name))]));
  const metadata = Object.fromEntries(["LICENSE", "NOTICE"].map(name => [name, readFileSync(join(source, name))]));
  for (const name of ["CMakeLists.txt", "AcyclicTransportConfig.cmake.in"]) metadata[name] = readFileSync(join(directory, "../templates/package", name));
  const host = `${process.platform}-${process.arch}`;
  const pluginPin = toolchain.cpp_plugin[host];
  const pluginHash = sha256(readFileSync(plugin));
  if (!pluginPin || pluginHash !== pluginPin.sha256) throw new Error("cpp-plugin executable differs from admitted host pin");
  const compilerHash = sha256(readFileSync(protoc));
  if (compilerHash !== toolchain.protoc?.[host]?.sha256) throw new Error("compiler executable differs from admitted host pin");
  const options = { encoding: "utf8", timeout: 180_000, maxBuffer: 4 * 1024 * 1024,
    env: { ...process.env, LC_ALL: "C" } };
  const version = command(protoc, ["--version"], options);
  if (version.error || version.status !== 0 || version.stdout.trim() !== toolchain.protoc_version) {
    throw new Error("protoc version differs from pin");
  }
  const temporary = mkdtempSync(join(tmpdir(), "sdk-cpp-descriptors-"));
  try {
    const paths = stageDescriptorSets(manifest, inputs, temporary);
    mkdirSync(output);
    for (const folder of ["src", "include"]) mkdirSync(join(output, folder));
    const emitted = new Set();
    for (const [index, name] of manifest.families.map(family => family.source).sort().entries()) {
      const familyOutput = join(temporary, `family-${index}`);
      mkdirSync(familyOutput);
      const result = command(protoc, [`--descriptor_set_in=${paths.join(delimiter)}`,
        `--plugin=protoc-gen-grpc=${plugin}`, `--cpp_out=${familyOutput}`, `--grpc_out=${familyOutput}`,
        name], { ...options, cwd: temporary });
      if (result.error || result.status !== 0) throw new Error(`C++ generation failed: ${result.stderr ?? ""}`);
      const files = inventory(familyOutput);
      const stem = name.slice(0, -6);
      if (![".pb.h", ".pb.cc", ".grpc.pb.h", ".grpc.pb.cc"].every(suffix => files.includes(stem + suffix))) throw new Error(`generator lacks complete C++ bindings for ${name}`);
      for (const file of files) {
        if (!/\.(?:grpc\.)?pb\.(?:h|cc)$/.test(file)) throw new Error("unexpected generated C++ output");
        copyGeneratedFiles(familyOutput, join(output, file.endsWith(".h") ? "include" : "src"), [file], emitted);
      }
    }
    const resources = join(output, "authority");
    mkdirSync(resources, { recursive: true });
    for (const [name, bytes] of Object.entries(metadata)) {
      writeFileSync(join(output, name), bytes, { flag: "wx" });
    }
    writeFileSync(join(resources, "rust-authority.json"), manifestBytes, { flag: "wx" });
    if (!readFileSync(join(authority, "rust-authority.json")).equals(manifestBytes)
      || [...inputs].some(([name, bytes]) => !readFileSync(join(authority, name)).equals(bytes))) throw new Error("authority changed during generation");
    if (sha256(readFileSync(protoc)) !== compilerHash || sha256(readFileSync(plugin)) !== pluginHash) throw new Error("generation tool changed during generation");
    if (Object.entries(producerBytes).some(([name, bytes]) => !readFileSync(join(directory, name)).equals(bytes))) throw new Error("producer source changed during generation");
    for (const [name, bytes] of Object.entries(metadata)) {
      const original = ["LICENSE", "NOTICE"].includes(name) ? join(source, name) : join(directory, "../templates/package", name);
      if (!readFileSync(original).equals(bytes)) throw new Error("package metadata changed during generation");
    }
    const outputs = inventory(output);
    const receipt = {
      schema: "acyclic.sdk.cpp-producer-receipt.v1", authority: "rust", target: "cpp",
      source_revision: manifest.source_revision, authority_manifest_sha256: sha256(manifestBytes),
      input_sha256: inputHashes, protoc_version: version.stdout.trim(),
      cpp_plugin_version: toolchain.cpp_plugin_version, cpp_plugin_coordinate: pluginPin.url,
      tool_sha256: { protoc: compilerHash, cpp_plugin: pluginHash },
      generator_sha256: sha256(readFileSync(fileURLToPath(import.meta.url))),
      authority_reader_sha256: sha256(readFileSync(join(directory, "../../../shared/authority.mjs"))),
      source_sha256: Object.fromEntries(Object.entries(producerBytes).map(([name, bytes]) => [name, sha256(bytes)])),
      template_sha256: Object.fromEntries(["CMakeLists.txt", "AcyclicTransportConfig.cmake.in"].map(name => [name, sha256(metadata[name])])),
      toolchain_sha256: sha256(JSON.stringify(toolchain)), node_version: process.version,
      outputs, output_sha256: Object.fromEntries(outputs.map(name => [name, sha256(readFileSync(join(output, name)))])),
    };
    writeFileSync(join(output, "generation-receipt.json"), JSON.stringify(receipt, null, 2) + "\n", { flag: "wx" });
    return receipt;
  } finally {
    if (realpathSync(temporary) !== temporary) throw new Error("temporary directory identity changed");
    rmSync(temporary, { recursive: true });
  }
}

runCli(import.meta.url, generate, ["source-root", "authority", "protoc", "cpp-plugin", "output"]);
