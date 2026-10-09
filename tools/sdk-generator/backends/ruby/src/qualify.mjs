import { spawnSync } from "node:child_process";
import { lstatSync, mkdirSync, readFileSync, readdirSync, realpathSync, writeFileSync } from "node:fs";
import { basename, delimiter, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { canonical, loadAuthority, readInput, sha256, within } from "../../../shared/authority.mjs";

const directory = dirname(fileURLToPath(import.meta.url));
const pinned = JSON.parse(readFileSync(join(directory, "../toolchains/toolchain.json"))).qualification;
const coordinate = "acyclic-sdk-transport";
const version = "0.2.0.alpha.1";
const packageName = `${coordinate}-${version}.gem`;
const specName = `${coordinate}.gemspec`;
const targets = ["actors/v1/actors.proto", "workers/v1/workers.proto", "stream/v2/stream.proto"];
const negatives = { InvalidActorBytes: "code_sha256", InvalidWorkerBytes: "javascript_module", InvalidOptionalInteger: "if_tail" };

function files(root, prefix = "") {
  return readdirSync(join(root, prefix), { withFileTypes: true }).flatMap(entry => {
    const name = prefix ? `${prefix}/${entry.name}` : entry.name;
    if (entry.isSymbolicLink()) throw new Error("tool or package inventory contains a link");
    if (entry.isDirectory()) return files(root, name);
    if (!entry.isFile()) throw new Error("tool or package inventory contains a non-file");
    return [name];
  }).sort();
}

// Injected commands/pins support offline controls. The CLI uses real pinned tools.
export function qualify(args, { command = spawnSync, toolchain = pinned } = {}) {
  if (process.platform !== "win32" && command === spawnSync) throw new Error("installed Ruby qualification currently requires the pinned Windows runtime");
  const packageRoot = realpathSync(args.package);
  const authorityRoot = realpathSync(args.authority);
  const rubyHome = realpathSync(args["ruby-home"]);
  const cache = realpathSync(args.cache);
  const output = join(realpathSync(dirname(resolve(args.output))), basename(resolve(args.output)));
  try { lstatSync(output); throw new Error("qualification output must be absent"); }
  catch (error) { if (error.code !== "ENOENT") throw error; }
  for (const input of [packageRoot, authorityRoot, rubyHome, cache, dirname(directory)]) {
    if (within(input, output) || within(output, input)) throw new Error("qualification output overlaps an input");
  }
  const receiptBytes = readFileSync(join(packageRoot, "generation-receipt.json"));
  const receipt = JSON.parse(receiptBytes);
  if (receipt.schema !== "acyclic.sdk.ruby-producer-receipt.v1" || receipt.authority !== "rust"
    || receipt.target !== "ruby" || !Array.isArray(receipt.outputs)) throw new Error("unsupported generation receipt");
  const approved = loadAuthority(authorityRoot);
  if (sha256(approved.bytes) !== receipt.authority_manifest_sha256 || approved.manifest.source_revision !== receipt.source_revision) {
    throw new Error("generation authority differs");
  }
  if (targets.some(name => !approved.descriptors.has(name))) throw new Error("tested family lacks an attested descriptor");
  const payload = new Map();
  const metadata = [specName, "LICENSE", "NOTICE", "authority/rust-authority.json"];
  for (const name of receipt.outputs) {
    if (!canonical(name) || payload.has(name) || (!metadata.includes(name) && !/^lib\/.+\.rb$/.test(name))) {
      throw new Error("unsafe, duplicate or unexpected package output");
    }
    payload.set(name, readInput(packageRoot, name, receipt.output_sha256[name]));
  }
  const expectedSpec = readFileSync(join(directory, "../templates/package", specName), "utf8")
    .replace("@RUST_SOURCE_REVISION@", approved.manifest.source_revision);
  if (!payload.has(specName) || sha256(payload.get(specName)) !== sha256(expectedSpec)) throw new Error("gem specification differs from pinned template");
  if (metadata.some(name => !payload.has(name)) || sha256(payload.get("authority/rust-authority.json")) !== sha256(approved.bytes)) {
    throw new Error("package lacks required authority or license metadata");
  }
  if (![...payload.keys()].some(name => name.startsWith("lib/"))) throw new Error("package lacks generated Ruby bindings");
  const runtimePins = Object.entries(toolchain.runtime_files ?? {});
  if (runtimePins.length === 0) throw new Error("runtime tool pins missing");
  for (const [name, digest] of runtimePins) readInput(rubyHome, name, digest);
  const ruby = realpathSync(join(rubyHome, "bin/ruby.exe"));
  const gem = realpathSync(join(rubyHome, "bin/gem"));
  const archives = new Map();
  for (const pin of toolchain.dependencies ?? []) {
    if (!/^[a-z0-9-]+$/.test(pin.name) || pin.name === coordinate || !/^[0-9.]+$/.test(pin.version)
      || !/^[a-z0-9_-]+$/.test(pin.platform)) throw new Error("unsafe dependency coordinate");
    const filename = `${pin.name}-${pin.version}${pin.platform === "ruby" ? "" : `-${pin.platform}`}.gem`;
    if (archives.has(filename)) throw new Error("duplicate dependency archive");
    archives.set(filename, readInput(cache, filename, pin.sha256));
  }
  if (archives.size === 0) throw new Error("dependency archive closure missing");
  const env = { ...process.env };
  for (const key of Object.keys(env)) if (/^(RUBY|GEM|BUNDLE)/i.test(key)) delete env[key];
  const installedCache = join(output, "cache");
  Object.assign(env, { GEM_HOME: installedCache,
    GEM_PATH: [installedCache, join(rubyHome, "lib/ruby/gems", toolchain.ruby_api_version)].join(delimiter),
    HOME: join(output, "home"), USERPROFILE: join(output, "home"), SOURCE_DATE_EPOCH: toolchain.source_date_epoch });
  const execute = (argv, cwd) => command(ruby, argv, { cwd, env, encoding: "utf8", timeout: 180_000, maxBuffer: 8 * 1024 * 1024 });
  const runtimeVersion = execute(["--version"], directory);
  const versionText = (runtimeVersion.stdout ?? "") + (runtimeVersion.stderr ?? "");
  if (runtimeVersion.error || runtimeVersion.status !== 0 || !versionText.startsWith(`ruby ${toolchain.ruby_version} `)
    || !versionText.includes(`[${toolchain.ruby_platform}]`)) throw new Error("Ruby runtime version differs from pin");
  mkdirSync(output);
  const put = (name, bytes) => {
    const path = join(output, name); mkdirSync(dirname(path), { recursive: true }); writeFileSync(path, bytes, { flag: "wx" }); return path;
  };
  const project = join(output, "project");
  for (const [name, bytes] of payload) put(`project/${name}`, bytes);
  mkdirSync(env.HOME);
  const logs = [];
  const run = (name, argv, cwd = output) => {
    const result = execute(argv, cwd); const text = (result.stdout ?? "") + (result.stderr ?? "");
    const log = `${name}.log`; put(log, text); logs.push(log);
    if (result.error || result.status !== 0) throw new Error(`${name} failed: ${text}`);
    return text;
  };
  for (const [filename, bytes] of archives) {
    const path = put(`feed/${filename}`, bytes);
    run(`install-${filename}`, [gem, "install", "--local", "--no-document", "--ignore-dependencies", "--install-dir", installedCache, path]);
  }
  run("build-package", [gem, "build", specName], project);
  const archive = readFileSync(join(project, packageName));
  const built = put(`package/${packageName}`, archive);
  run("install-sdk", [gem, "install", "--local", "--no-document", "--ignore-dependencies", "--install-dir", installedCache, built]);
  const installed = join(installedCache, "gems", `${coordinate}-${version}`);
  if (sha256(readFileSync(join(installedCache, "cache", packageName))) !== sha256(archive)) throw new Error("installed SDK archive differs");
  const installedPayload = [...payload.keys()].filter(name => name !== specName);
  if (JSON.stringify(files(installed)) !== JSON.stringify([...installedPayload].sort())) throw new Error("installed SDK payload inventory differs");
  for (const name of installedPayload) {
    if (sha256(readFileSync(join(installed, name))) !== sha256(payload.get(name))) throw new Error("installed SDK payload differs");
  }
  env.SDK_QUALIFIED_GEM = installed;
  const controls = join(directory, "../tests/fixtures/consumer");
  const controlFiles = ["installed_consumer.rb", ...Object.keys(negatives).map(name => `${name}.rb`)];
  for (const name of controlFiles) put(`controls/${name}`, readFileSync(join(controls, name)));
  const descriptors = targets.map((name, index) => put(`authority/${index}.bin`, approved.inputs.get(approved.descriptors.get(name))));
  const positive = run("positive", [join(output, "controls/installed_consumer.rb"), ...descriptors]);
  if (!positive.includes("PASS: installed Ruby descriptors, bytes, unsigned bounds, optional presence, oneof and gRPC shapes")) {
    throw new Error("positive consumer did not report completed controls");
  }
  for (const [name, field] of Object.entries(negatives)) {
    const text = run(`negative-${name}`, [join(output, "controls", `${name}.rb`)]);
    if (!text.includes(`PASS: intended ${field} TypeError:`)) throw new Error(`${name} did not reject the intended assignment`);
  }
  const result = {
    schema: "acyclic.sdk.ruby.installed-qualification.v1", scope: "installed-ruby-transport-bindings",
    source_revision: approved.manifest.source_revision, authority_manifest_sha256: sha256(approved.bytes), generation_receipt_sha256: sha256(receiptBytes),
    ruby_version: versionText.trim(), gem_sha256: sha256(archive), source_date_epoch: toolchain.source_date_epoch,
    package_payload_sha256: Object.fromEntries([...payload].map(([name, bytes]) => [name, sha256(bytes)])),
    dependency_archive_sha256: Object.fromEntries([...archives].map(([name, bytes]) => [name, sha256(bytes)])),
    runtime_source: toolchain.runtime_source,
    runtime_file_sha256: Object.fromEntries(files(rubyHome).map(name => [name, sha256(readFileSync(join(rubyHome, name)))])),
    installed_dependency_file_sha256: Object.fromEntries(files(installedCache).map(name => [name, sha256(readFileSync(join(installedCache, name)))])),
    qualifier_sha256: sha256(readFileSync(fileURLToPath(import.meta.url))), authority_reader_sha256: sha256(readFileSync(join(directory, "../../../shared/authority.mjs"))),
    toolchain_sha256: sha256(JSON.stringify(toolchain)),
    control_sha256: Object.fromEntries(controlFiles.map(name => [name, sha256(readFileSync(join(output, "controls", name)))])),
    log_sha256: Object.fromEntries(logs.map(name => [name, sha256(readFileSync(join(output, name)))])),
    positive_controls_passed: true, independent_negative_runtime_type_controls_rejected: 3, compile_time_type_controls_supported: false,
    rust_backed_rpc_qualified: false, embedded_runtime_qualified: false,
  };
  put("qualification.json", JSON.stringify(result, null, 2) + "\n"); return result;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const options = Object.fromEntries(["package", "authority", "ruby-home", "cache", "output"].map(name => [name, { type: "string" }]));
  const { values } = parseArgs({ options });
  if (Object.keys(options).some(name => !values[name])) throw new Error("--package, --authority, --ruby-home, --cache and --output are required");
  const result = qualify(values);
  console.log(JSON.stringify({ scope: result.scope, gem_sha256: result.gem_sha256, qualification: resolve(values.output, "qualification.json") }, null, 2));
}
