import { spawnSync } from "node:child_process";
import { copyFileSync, lstatSync, mkdirSync, readFileSync, readdirSync, realpathSync, writeFileSync } from "node:fs";
import { basename, dirname, isAbsolute, join, posix, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { readBoundedGzip, sha256, tarEntries } from "../../../../../scripts/archive-utils.mjs";

const directory = dirname(fileURLToPath(import.meta.url));
const targets = ["actors/v1/actors.proto", "workers/v1/workers.proto", "stream/v1/stream.proto"];
const within = (root, path) => {
  const child = relative(root, path);
  return child === "" || (!isAbsolute(child) && child !== ".." && !child.startsWith(`..${sep}`));
};
const canonical = name => typeof name === "string" && name !== "." && name !== "" && !posix.isAbsolute(name)
  && !name.includes("\\") && !name.includes(":") && !name.split("/").includes("..")
  && posix.normalize(name) === name;

export function validatePayload(entries, receipt, platform = process.platform) {
  const names = new Set();
  const aliases = new Set();
  for (const entry of entries) {
    const name = entry.path;
    if (!canonical(name) || entry.type !== "0") throw new Error("unsafe package entry");
    const alias = platform === "win32" ? name.toLowerCase() : name;
    if (names.has(name) || aliases.has(alias)) throw new Error("package entries collide");
    if (platform === "win32" && name.split("/").some(part => /[. ]$/.test(part)
      || /^(con|prn|aux|nul|com[1-9]|lpt[1-9])(?:\.|$)/i.test(part))) {
      throw new Error("unsafe Windows package entry");
    }
    names.add(name);
    aliases.add(alias);
    if (!Object.hasOwn(receipt.output_sha256, name) || sha256(entry.body) !== receipt.output_sha256[name]) {
      throw new Error("archive payload digest mismatch");
    }
  }
  if (new Set(receipt.outputs).size !== receipt.outputs.length || names.size !== receipt.outputs.length
    || receipt.outputs.some(name => !names.has(name))) throw new Error("archive inventory differs from generation receipt");
}

export function verifyArchive(path, expected, receipt) {
  const archive = readBoundedGzip(path, 64 * 1024 * 1024, 256 * 1024 * 1024);
  if (sha256(archive.compressed) !== expected) throw new Error("package archive digest mismatch");
  const entries = tarEntries(archive.expanded);
  validatePayload(entries, receipt);
  return entries;
}

export function qualify(args, command = spawnSync) {
  const archive = realpathSync(args.package);
  const authority = realpathSync(args.authority);
  const receiptPath = realpathSync(args.receipt);
  const go = realpathSync(args.go);
  const work = join(realpathSync(dirname(resolve(args.output))), basename(resolve(args.output)));
  try { lstatSync(work); throw new Error("qualification output must be absent"); }
  catch (error) { if (error.code !== "ENOENT") throw error; }
  for (const source of [archive, authority, receiptPath, dirname(directory), go]) {
    if (within(work, source) || within(source, work)) throw new Error("qualification output overlaps an input");
  }
  const receiptBytes = readFileSync(receiptPath);
  const receipt = JSON.parse(receiptBytes.toString());
  if (receipt.schema !== "acyclic.sdk.go-producer-receipt.v1" || receipt.authority !== "rust" || receipt.target !== "go") {
    throw new Error("unsupported generation receipt");
  }
  const manifestBytes = readFileSync(join(authority, "rust-authority.json"));
  if (sha256(manifestBytes) !== receipt.authority_manifest_sha256) throw new Error("Rust authority manifest digest mismatch");
  const manifest = JSON.parse(manifestBytes.toString());
  if (manifest.schema !== "acyclic.sdk.rust-authority.v1" || manifest.authority !== "rust"
    || manifest.source_revision !== receipt.source_revision) throw new Error("Rust source authority mismatch");
  const inputs = new Map();
  const descriptors = new Map();
  const sources = new Set();
  for (const family of manifest.families) {
    if (sources.has(family.source)) throw new Error("duplicate authority family");
    sources.add(family.source);
    for (const field of ["source", ...(family.descriptor ? ["descriptor"] : [])]) {
      const name = family[field];
      if (!canonical(name)) throw new Error("unsafe authority path");
      const path = realpathSync(join(authority, name));
      if (!within(authority, path)) throw new Error("authority input escapes its root");
      const bytes = readFileSync(path);
      if (sha256(bytes) !== family[`${field}_sha256`]) throw new Error("Rust authority input digest mismatch");
      inputs.set(name, bytes);
    }
    if (family.descriptor) descriptors.set(family.source, family.descriptor);
  }
  if (targets.some(name => !descriptors.has(name))) throw new Error("tested family lacks an attested descriptor");
  const entries = verifyArchive(archive, args.sha256, receipt);
  const env = { ...process.env, GOMAXPROCS: "1", GOTOOLCHAIN: "local", GOPROXY: "off", GOSUMDB: "off",
    GOWORK: "off", GOENV: "off", GOFLAGS: "" };
  const execute = arguments_ => command(go, arguments_, { env, encoding: "utf8", timeout: 180_000, maxBuffer: 4 * 1024 * 1024 });
  const version = execute(["version"]);
  if (version.error || version.status !== 0 || version.stdout.trim().split(/\s+/)[2] !== receipt.go_version) {
    throw new Error("Go toolchain differs from generation receipt");
  }
  mkdirSync(work);
  const installed = join(work, "installed");
  for (const entry of entries) {
    const path = join(installed, entry.path);
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, entry.body, { flag: "wx" });
  }
  // Consumers read these exact verified bytes, including shared/custom paths.
  for (const [name, bytes] of inputs) {
    const path = join(work, "authority", name);
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, bytes, { flag: "wx" });
  }
  env.SDK_DESCRIPTOR_FILES = JSON.stringify(Object.fromEntries(targets.map(name =>
    [name, join(work, "authority", descriptors.get(name))])));
  const consumer = join(work, "consumer");
  mkdirSync(consumer);
  for (const name of ["go.mod", "go.sum"]) copyFileSync(join(installed, name), join(consumer, name));
  const controls = join(directory, "../testdata", "consumer");
  const controlNames = readdirSync(controls).filter(name => name.endsWith(".go")).sort();
  for (const name of controlNames) copyFileSync(join(controls, name), join(consumer, name));
  const run = (name, arguments_, failure = false) => {
    const result = execute(["-C", consumer, ...arguments_]);
    const output = (result.stdout ?? "") + (result.stderr ?? "");
    writeFileSync(join(work, `${name}.log`), output);
    if (result.error || result.status === null || (result.status !== 0) !== failure) throw new Error(`${name} failed: ${output}`);
    return output;
  };
  run("module", ["mod", "edit", "-module=qualification.example/installed-go",
    "-require=github.com/acyclic-labs/sdk/go@v0.2.0", "-replace=github.com/acyclic-labs/sdk/go=../installed"]);
  run("positive", ["test", "-mod=readonly", "-p=1", "-parallel=1", "-count=1", "-v", "./..."]);
  const negative = run("negative", ["test", "-mod=readonly", "-p=1", "-parallel=1", "-count=1", "-tags=negative", "./..."], true);
  if ((negative.match(/cannot use/g) ?? []).length !== 3 || !negative.includes("as []byte") || !negative.includes("as *uint64")) {
    throw new Error("negative controls failed for an unrelated reason");
  }
  const result = {
    scope: "installed-go-transport-bindings", source_revision: receipt.source_revision,
    archive_sha256: args.sha256, generation_receipt_sha256: sha256(receiptBytes),
    authority_manifest_sha256: sha256(manifestBytes), go_version: version.stdout.trim(),
    go_executable_sha256: sha256(readFileSync(go)), node_version: process.version,
    qualifier_sha256: sha256(readFileSync(fileURLToPath(import.meta.url))),
    control_sha256: Object.fromEntries(controlNames.map(name => [name, sha256(readFileSync(join(controls, name)))])),
    log_sha256: Object.fromEntries(["module", "positive", "negative"].map(name => [`${name}.log`, sha256(readFileSync(join(work, `${name}.log`)))])),
    descriptor_equality: "all API fields; source comments and Buf image tag 8042 excluded",
    positive_controls_passed: true, negative_type_controls_rejected: 3,
    rust_backed_rpc_qualified: false, embedded_runtime_qualified: false,
  };
  writeFileSync(join(work, "qualification.json"), JSON.stringify(result, null, 2) + "\n");
  return result;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const options = Object.fromEntries(["package", "sha256", "receipt", "authority", "go", "output"].map(name => [name, { type: "string" }]));
  const { values } = parseArgs({ options });
  if (Object.keys(options).some(name => !values[name])) throw new Error("--package, --sha256, --receipt, --authority, --go and --output are required");
  console.log(JSON.stringify(qualify(values), null, 2));
}
