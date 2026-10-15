import { tmpdir } from "node:os";
import { createHash } from "node:crypto";
import { lstatSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { basename, delimiter, dirname, isAbsolute, join, posix, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";

export const sha256 = bytes => createHash("sha256").update(bytes).digest("hex");
export const within = (root, path) => {
  const child = relative(root, path);
  return child === "" || (!isAbsolute(child) && child !== ".." && !child.startsWith(`..${sep}`));
};
export const canonical = name => typeof name === "string" && name !== "" && name !== "."
  && !posix.isAbsolute(name) && !name.includes("\\") && !name.includes(":")
  && !name.includes("\0") && !name.split("/").includes("..") && posix.normalize(name) === name;

export const equalEntries = (left, right) => JSON.stringify(Object.entries(left).sort()) === JSON.stringify(Object.entries(right).sort());

export function verifyPackageOutputs(outputs, digests, required, unexpectedMessage) {
  const names = new Set(), folded = new Set();
  for (const name of outputs) {
    if (!canonical(name) || folded.has(name.toLowerCase()) || !required.has(name)) throw new Error(unexpectedMessage);
    names.add(name); folded.add(name.toLowerCase());
  }
  if (names.size !== required.size || [...required].some(name => !names.has(name))) throw new Error("package output inventory incomplete");
  if (!equalEntries(digests, Object.fromEntries([...names].map(name => [name, digests[name]])))
    || [...names].some(name => !/^[a-f0-9]{64}$/.test(digests[name]))) throw new Error("package digest inventory differs");
}

export function runCli(url, execute, names, missingMessage) {
  if (!process.argv[1] || resolve(process.argv[1]) !== fileURLToPath(url)) return;
  const options = Object.fromEntries(names.map(name => [name, { type: "string" }]));
  const { values } = parseArgs({ options });
  if (names.some(name => !values[name])) {
    throw new Error(missingMessage ?? names.slice(0, -1).map(name => "--" + name).join(", ") + " and --" + names.at(-1) + " are required");
  }
  console.log(JSON.stringify(execute(values), null, 2));
}

// Shared output checks remain covered by the existing authority-reader receipt digest.
function protectedOutput(path, inputs, absentMessage, overlapMessage) {
  const output = join(realpathSync(dirname(resolve(path))), basename(resolve(path)));
  try { lstatSync(output); throw new Error(absentMessage); }
  catch (error) { if (error.code !== "ENOENT") throw error; }
  for (const input of inputs) {
    if (within(input, output) || within(output, input)) throw new Error(overlapMessage);
  }
  return output;
}

export function generationOutput(path, inputs) {
  return protectedOutput(path, inputs, "output must be absent", "output overlaps protected input");
}

export function qualificationOutput(path, inputs) {
  return protectedOutput(path, inputs, "qualification output must be absent", "qualification output overlaps an input");
}

export function inventory(root, prefix = "") {
  return readdirSync(join(root, prefix), { withFileTypes: true }).flatMap(entry => {
    const name = posix.join(prefix, entry.name);
    if (entry.isSymbolicLink()) throw new Error("generated output contains a link");
    if (entry.isDirectory()) return inventory(root, name);
    if (!entry.isFile()) throw new Error("generated output is not a regular file");
    return [name];
  }).sort();
}

export function verifyInstalledPayload(root, payload) {
  if (JSON.stringify(inventory(root)) !== JSON.stringify([...payload.keys()].sort())) throw new Error("installed payload inventory differs");
  for (const [name, bytes] of payload) readInput(root, name, sha256(bytes));
}

export function sourceInventory(root, prefix = "") {
  const directory = join(root, prefix), kind = lstatSync(directory);
  if (!kind.isDirectory() || kind.isSymbolicLink()) throw new Error("inventory directory kind differs");
  return readdirSync(directory, { withFileTypes: true }).flatMap(entry => {
    const name = posix.join(prefix, entry.name);
    if (entry.isSymbolicLink()) throw new Error("inventory contains a link: " + name);
    if (entry.isDirectory()) return sourceInventory(root, name);
    if (!entry.isFile()) throw new Error("inventory contains a non-file");
    return [name];
  }).sort();
}

export function stageDescriptorSets(manifest, inputs, directory) {
  const descriptors = new Map();
  for (const family of manifest.families) {
    if (family.descriptor) descriptors.set(family.descriptor_sha256, inputs.get(family.descriptor));
  }
  return [...descriptors].sort(([a], [b]) => a.localeCompare(b)).map(([digest, bytes]) => {
    const path = join(directory, `${digest}.bin`);
    writeFileSync(path, bytes, { flag: "wx" });
    return path;
  });
}

export function copyGeneratedFiles(source, output, files, emitted) {
  for (const file of files) {
    const key = process.platform === "win32" ? file.toLowerCase() : file;
    if (emitted.has(key)) throw new Error("generated family outputs collide");
    emitted.add(key);
    const destination = join(output, file);
    mkdirSync(dirname(destination), { recursive: true });
    writeFileSync(destination, readFileSync(join(source, file)), { flag: "wx" });
  }
}

export function readInput(root, name, digest) {
  if (!canonical(name)) throw new Error("unsafe authority path");
  const path = realpathSync(join(root, name));
  if (!within(root, path)) throw new Error("authority input escapes root");
  const bytes = readFileSync(path);
  if (!/^[a-f0-9]{64}$/.test(digest) || sha256(bytes) !== digest) throw new Error("authority input digest mismatch");
  return bytes;
}

export function loadAuthority(root) {
  const bytes = readFileSync(join(root, "rust-authority.json"));
  const manifest = JSON.parse(bytes);
  if (manifest.schema !== "acyclic.sdk.rust-authority.v1" || manifest.authority !== "rust"
    || !/^[a-f0-9]{40}$/.test(manifest.source_revision) || !Array.isArray(manifest.families)
    || manifest.families.length === 0) throw new Error("not an immutable Rust authority export");
  const sources = new Set();
  const inputs = new Map();
  const descriptors = new Map();
  for (const family of manifest.families) {
    const key = process.platform === "win32" ? family.source?.toLowerCase() : family.source;
    if (sources.has(key)) throw new Error("duplicate authority source");
    sources.add(key);
    for (const field of ["source", ...(family.descriptor ? ["descriptor"] : [])]) {
      inputs.set(family[field], readInput(root, family[field], family[`${field}_sha256`]));
    }
    if (!family.source.endsWith(".proto")) throw new Error("authority source is not a proto file");
    if (family.descriptor) descriptors.set(family.source, family.descriptor);
  }
  if (descriptors.size === 0) throw new Error("authority contains no descriptor sets");
  return { bytes, manifest, inputs, descriptors };
}

export function generateProtocPackage(args, {
  command, toolchain, directory, backend, language, pluginArgument, pluginKey,
  pluginMismatch, bindingsDirectory, authorityDirectory, extension, compilerArguments, packageMetadata,
}) {
  const source = realpathSync(args["source-root"]);
  const authority = realpathSync(args.authority);
  const protoc = realpathSync(args.protoc);
  const plugin = realpathSync(args[pluginArgument]);
  const output = generationOutput(args.output, [source, authority, protoc, plugin, dirname(directory)]);
  const { bytes: manifestBytes, manifest, inputs } = loadAuthority(authority);
  const inputHashes = Object.fromEntries([...inputs].map(([name, bytes]) => [name, sha256(bytes)]));
  const metadata = Object.fromEntries(["LICENSE", "NOTICE"].map(name => [name, readFileSync(join(source, name))]));
  Object.assign(metadata, packageMetadata(manifest));
  const host = `${process.platform}-${process.arch}`;
  const pluginPin = toolchain[pluginKey][host];
  const pluginHash = sha256(readFileSync(plugin));
  if (!pluginPin || pluginHash !== pluginPin.sha256) throw new Error(pluginMismatch);
  const compilerHash = sha256(readFileSync(protoc));
  if (compilerHash !== toolchain.protoc?.[host]?.sha256) throw new Error("compiler executable differs from published host pin");
  const options = { encoding: "utf8", timeout: 180_000, maxBuffer: 4 * 1024 * 1024,
    env: { ...process.env, LC_ALL: "C" } };
  const version = command(protoc, ["--version"], options);
  if (version.error || version.status !== 0 || version.stdout.trim() !== toolchain.protoc_version) {
    throw new Error("protoc version differs from pin");
  }
  const temporary = mkdtempSync(join(tmpdir(), `sdk-${backend}-descriptors-`));
  try {
    const paths = stageDescriptorSets(manifest, inputs, temporary);
    mkdirSync(output);
    const bindings = join(output, bindingsDirectory);
    mkdirSync(bindings, { recursive: true });
    const emitted = new Set();
    for (const [index, name] of manifest.families.map(family => family.source).sort().entries()) {
      const familyOutput = join(temporary, `family-${index}`);
      mkdirSync(familyOutput);
      const result = command(protoc, [`--descriptor_set_in=${paths.join(delimiter)}`,
        ...compilerArguments(familyOutput, plugin),
        name], { ...options, cwd: temporary });
      if (result.error || result.status !== 0) throw new Error(`${language} generation failed: ${result.stderr ?? ""}`);
      const files = inventory(familyOutput);
      if (!files.some(file => file.endsWith(extension))) throw new Error(`generator produced no ${language} bindings for ${name}`);
      copyGeneratedFiles(familyOutput, bindings, files, emitted);
    }
    const resources = join(output, authorityDirectory);
    mkdirSync(resources, { recursive: true });
    for (const [name, bytes] of Object.entries(metadata)) {
      writeFileSync(join(output, name), bytes, { flag: "wx" });
    }
    writeFileSync(join(resources, "rust-authority.json"), manifestBytes, { flag: "wx" });
    const outputs = inventory(output);
    const receipt = {
      schema: `acyclic.sdk.${backend}-producer-receipt.v1`, authority: "rust", target: backend,
      source_revision: manifest.source_revision, authority_manifest_sha256: sha256(manifestBytes),
      input_sha256: inputHashes, protoc_version: version.stdout.trim(),
      [`${pluginKey}_version`]: toolchain[`${pluginKey}_version`], [`${pluginKey}_coordinate`]: pluginPin.url,
      tool_sha256: { protoc: compilerHash, [pluginKey]: pluginHash },
      generator_sha256: sha256(readFileSync(join(directory, "generate.mjs"))),
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
