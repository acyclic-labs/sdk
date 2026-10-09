import { spawnSync } from "node:child_process";
import { lstatSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, delimiter, dirname, join, posix, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { loadAuthority, sha256, within } from "../../shared/authority.mjs";

const directory = dirname(fileURLToPath(import.meta.url));
const pinned = { ...JSON.parse(readFileSync(join(directory, "toolchain.json"), "utf8")),
  ...JSON.parse(readFileSync(join(directory, "../../shared/protoc.json"), "utf8")) };

function inventory(root, prefix = "") {
  return readdirSync(join(root, prefix), { withFileTypes: true }).flatMap(entry => {
    const name = posix.join(prefix, entry.name);
    if (entry.isSymbolicLink()) throw new Error("generated output contains a link");
    if (entry.isDirectory()) return inventory(root, name);
    if (!entry.isFile()) throw new Error("generated output is not a regular file");
    return [name];
  }).sort();
}

// The command and toolchain injection are for offline staging controls. The CLI
// always uses spawnSync and the checked-in NuGet package tool pins.
export function generate(args, { command = spawnSync, toolchain = pinned } = {}) {
  const source = realpathSync(args["source-root"]);
  const authority = realpathSync(args.authority);
  const protoc = realpathSync(args.protoc);
  const plugin = realpathSync(args["grpc-csharp"]);
  const output = join(realpathSync(dirname(resolve(args.output))), basename(resolve(args.output)));
  try { lstatSync(output); throw new Error("output must be absent"); }
  catch (error) { if (error.code !== "ENOENT") throw error; }
  for (const input of [source, authority, protoc, plugin, directory]) {
    if (within(input, output) || within(output, input)) throw new Error("output overlaps protected input");
  }
  const { bytes: manifestBytes, manifest, inputs } = loadAuthority(authority);
  const descriptors = new Map();
  for (const family of manifest.families) {
    if (family.descriptor) descriptors.set(family.descriptor_sha256, inputs.get(family.descriptor));
  }
  const inputHashes = Object.fromEntries([...inputs].map(([name, bytes]) => [name, sha256(bytes)]));
  const metadata = Object.fromEntries(["LICENSE", "NOTICE"].map(name => [name, readFileSync(join(source, name))]));
  for (const name of ["Acyclic.Sdk.Transport.csproj", "packages.lock.json", "global.json", "README.md"]) metadata[name] = readFileSync(join(directory, "package", name));
  const host = `${process.platform}-${process.arch}`;
  const pluginPin = toolchain.grpc_csharp[host];
  const pluginHash = sha256(readFileSync(plugin));
  if (!pluginPin || pluginHash !== pluginPin.sha256) throw new Error("grpc-csharp executable differs from published package host pin");
  const compilerHash = sha256(readFileSync(protoc));
  if (compilerHash !== toolchain.protoc?.[host]?.sha256) throw new Error("compiler executable differs from published host pin");
  const options = { encoding: "utf8", timeout: 180_000, maxBuffer: 4 * 1024 * 1024,
    env: { ...process.env, LC_ALL: "C" } };
  const version = command(protoc, ["--version"], options);
  if (version.error || version.status !== 0 || version.stdout.trim() !== toolchain.protoc_version) {
    throw new Error("protoc version differs from pin");
  }
  const temporary = mkdtempSync(join(tmpdir(), "sdk-dotnet-descriptors-"));
  try {
    const paths = [...descriptors].sort(([a], [b]) => a.localeCompare(b)).map(([digest, bytes]) => {
      const path = join(temporary, `${digest}.bin`);
      writeFileSync(path, bytes, { flag: "wx" });
      return path;
    });
    mkdirSync(output);
    const generated = join(output, "generated");
    mkdirSync(generated, { recursive: true });
    const emitted = new Set();
    for (const [index, name] of manifest.families.map(family => family.source).sort().entries()) {
      const familyOutput = join(temporary, `family-${index}`);
      mkdirSync(familyOutput);
      const result = command(protoc, [`--descriptor_set_in=${paths.join(delimiter)}`,
        `--csharp_out=${familyOutput}`, `--plugin=protoc-gen-grpc=${plugin}`, `--grpc_out=${familyOutput}`,
        name], { ...options, cwd: temporary });
      if (result.error || result.status !== 0) throw new Error(`C# generation failed: ${result.stderr ?? ""}`);
      const files = inventory(familyOutput);
      if (!files.some(file => file.endsWith(".cs"))) throw new Error(`generator produced no C# bindings for ${name}`);
      for (const file of files) {
        const key = process.platform === "win32" ? file.toLowerCase() : file;
        if (emitted.has(key)) throw new Error("generated family outputs collide");
        emitted.add(key);
        const destination = join(generated, file);
        mkdirSync(dirname(destination), { recursive: true });
        writeFileSync(destination, readFileSync(join(familyOutput, file)), { flag: "wx" });
      }
    }
    const resources = output;
    for (const [name, bytes] of Object.entries(metadata)) {
      writeFileSync(join(resources, name), bytes, { flag: "wx" });
    }
    writeFileSync(join(resources, "rust-authority.json"), manifestBytes, { flag: "wx" });
    const outputs = inventory(output);
    const receipt = {
      schema: "acyclic.sdk.dotnet-producer-receipt.v1", authority: "rust", target: "dotnet",
      source_revision: manifest.source_revision, authority_manifest_sha256: sha256(manifestBytes),
      input_sha256: inputHashes, protoc_version: version.stdout.trim(),
      grpc_csharp_version: toolchain.grpc_csharp_version, grpc_csharp_coordinate: pluginPin.url,
      tool_sha256: { protoc: compilerHash, grpc_csharp: pluginHash },
      generator_sha256: sha256(readFileSync(fileURLToPath(import.meta.url))),
      authority_reader_sha256: sha256(readFileSync(join(directory, "../../shared/authority.mjs"))),
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

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const options = Object.fromEntries(["source-root", "authority", "protoc", "grpc-csharp", "output"].map(name => [name, { type: "string" }]));
  const { values } = parseArgs({ options });
  if (Object.keys(options).some(name => !values[name])) throw new Error("--source-root, --authority, --protoc, --grpc-csharp and --output are required");
  console.log(JSON.stringify(generate(values), null, 2));
}
