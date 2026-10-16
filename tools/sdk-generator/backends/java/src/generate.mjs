import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir, endianness } from "node:os";
import { delimiter, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { runCli, generationOutput, copyGeneratedFiles, inventory, loadAuthority, sha256, stageDescriptorSets } from "../../../shared/authority.mjs";

const directory = dirname(fileURLToPath(import.meta.url));
const pinned = { ...JSON.parse(readFileSync(join(directory, "../toolchains/toolchain.json"), "utf8")),
  ...JSON.parse(readFileSync(join(directory, "../../../shared/protoc.json"), "utf8")) };

// The command and toolchain injection are for offline staging controls. The CLI
// always uses spawnSync and the checked-in Maven Central tool pins.
export function generate(args, { command = spawnSync, toolchain = pinned } = {}) {
  const source = realpathSync(args["source-root"]);
  const authority = realpathSync(args.authority);
  const protoc = realpathSync(args.protoc);
  const plugin = realpathSync(args["grpc-java"]);
  const output = generationOutput(args.output, [source, authority, protoc, plugin, dirname(directory)]);
  const { bytes: manifestBytes, manifest, inputs } = loadAuthority(authority);
  const inputHashes = Object.fromEntries([...inputs].map(([name, bytes]) => [name, sha256(bytes)]));
  const metadata = Object.fromEntries(["LICENSE", "NOTICE"].map(name => [name, readFileSync(join(source, name))]));
  metadata["pom.xml"] = readFileSync(join(directory, "../templates/package", "pom.xml"));
  const host = `${process.platform}-${process.arch}`;
  if (process.arch === "ppc64" && endianness() !== "LE") throw new Error("grpc-java pin requires little endian ppc64");
  const pluginPin = toolchain.grpc_java[host];
  const pluginHash = sha256(readFileSync(plugin));
  if (!pluginPin || pluginHash !== pluginPin.sha256) throw new Error("grpc-java executable differs from published host pin");
  const compilerHash = sha256(readFileSync(protoc));
  if (compilerHash !== toolchain.protoc?.[host]?.sha256) throw new Error("compiler executable differs from published host pin");
  const options = { encoding: "utf8", timeout: 180_000, maxBuffer: 4 * 1024 * 1024,
    env: { ...process.env, LC_ALL: "C" } };
  const version = command(protoc, ["--version"], options);
  if (version.error || version.status !== 0 || version.stdout.trim() !== toolchain.protoc_version) {
    throw new Error("protoc version differs from pin");
  }
  const temporary = mkdtempSync(join(tmpdir(), "sdk-java-descriptors-"));
  try {
    const paths = stageDescriptorSets(manifest, inputs, temporary);
    mkdirSync(output);
    const java = join(output, "src", "main", "java");
    mkdirSync(java, { recursive: true });
    const emitted = new Set();
    for (const [index, name] of manifest.families.map(family => family.source).sort().entries()) {
      const familyOutput = join(temporary, `family-${index}`);
      mkdirSync(familyOutput);
      const result = command(protoc, [`--descriptor_set_in=${paths.join(delimiter)}`,
        `--java_out=${familyOutput}`, `--plugin=protoc-gen-grpc-java=${plugin}`, `--grpc-java_out=${familyOutput}`,
        name], { ...options, cwd: temporary });
      if (result.error || result.status !== 0) throw new Error(`Java generation failed: ${result.stderr ?? ""}`);
      const files = inventory(familyOutput);
      if (!files.some(file => file.endsWith(".java"))) throw new Error(`generator produced no Java bindings for ${name}`);
      copyGeneratedFiles(familyOutput, java, files, emitted);
    }
    const resources = join(output, "src", "main", "resources", "META-INF");
    mkdirSync(resources, { recursive: true });
    for (const [name, bytes] of Object.entries(metadata)) {
      writeFileSync(name === "pom.xml" ? join(output, name) : join(resources, name), bytes, { flag: "wx" });
    }
    writeFileSync(join(resources, "rust-authority.json"), manifestBytes, { flag: "wx" });
    const outputs = inventory(output);
    const receipt = {
      schema: "acyclic.sdk.java-producer-receipt.v1", authority: "rust", target: "java",
      source_revision: manifest.source_revision, authority_manifest_sha256: sha256(manifestBytes),
      input_sha256: inputHashes, protoc_version: version.stdout.trim(),
      grpc_java_version: toolchain.grpc_java_version, grpc_java_coordinate: pluginPin.url,
      tool_sha256: { protoc: compilerHash, grpc_java: pluginHash },
      generator_sha256: sha256(readFileSync(fileURLToPath(import.meta.url))),
      authority_reader_sha256: sha256(readFileSync(join(directory, "../../../shared/authority.mjs"))),
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

runCli(import.meta.url, generate, ["source-root", "authority", "protoc", "grpc-java", "output"]);
