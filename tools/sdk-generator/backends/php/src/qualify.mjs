import { spawnSync } from "node:child_process";
import { lstatSync, mkdirSync, readFileSync, readdirSync, realpathSync, writeFileSync } from "node:fs";
import { basename, delimiter, dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { parseArgs } from "node:util";
import { gunzipSync } from "node:zlib";
import { tarEntries } from "../../../../../scripts/archive-utils.mjs";
import { canonical, loadAuthority, readInput, sha256, within } from "../../../shared/authority.mjs";

const directory = dirname(fileURLToPath(import.meta.url));
const read = name => readFileSync(join(directory, name));
const inventory = JSON.parse(read("../toolchains/runtime-files.json"));
if (!Array.isArray(inventory) || !inventory.length || inventory.some(entry => !entry || !canonical(entry.path) || !/^[a-f0-9]{64}$/.test(entry.sha256))
  || new Set(inventory.map(entry => entry.path.toLowerCase())).size !== inventory.length) throw new Error("invalid runtime inventory pins");
const pinned = { ...JSON.parse(read("../toolchains/qualification.json")), runtime_files: Object.fromEntries(inventory.map(entry => [entry.path, entry.sha256])) };
const targets = ["actors/v1/actors.proto", "workers/v1/workers.proto", "stream/v2/stream.proto"];
const negatives = { InvalidActorBytes: "InvalidArgumentException: Expect string.", InvalidWorkerBytes: "InvalidArgumentException: Expect string.", InvalidOptionalInteger: "Exception: Expect integer." };
const marker = "PASS: installed PHP descriptors, bytes, unsigned bits, optional zero, oneof and client RPC shapes";
const nativeMarker = "PASS: native PHP gRPC client creation and shutdown";
const packageName = "acyclic-sdk-transport-0.2.0-alpha.1.tar.gz";

function files(root, prefix = "") {
  return readdirSync(join(root, prefix), { withFileTypes: true }).flatMap(entry => {
    const name = prefix ? `${prefix}/${entry.name}` : entry.name;
    if (entry.isSymbolicLink()) throw new Error("inventory contains a link");
    if (entry.isDirectory()) return files(root, name);
    if (!entry.isFile()) throw new Error("inventory contains a non-file");
    return [name];
  }).sort();
}
function verifyPayload(root, payload) {
  if (JSON.stringify(files(root)) !== JSON.stringify([...payload.keys()].sort())) throw new Error("installed payload inventory differs");
  for (const [name, bytes] of payload) readInput(root, name, sha256(bytes));
}
function archivePayload(bytes) {
  if (!bytes.length || bytes.length > 32 * 1024 * 1024) throw new Error("archive exceeds size bound");
  const entries = tarEntries(gunzipSync(bytes, { maxOutputLength: 64 * 1024 * 1024 })), seen = new Set(), payload = new Map();
  for (const entry of entries) {
    const name = entry.type === "5" ? entry.path.replace(/\/$/, "") : entry.path;
    const key = name.toLowerCase();
    if (!canonical(name) || seen.has(key) || !["0", "5"].includes(entry.type)) throw new Error("unsafe or duplicate archive member");
    seen.add(key); if (entry.type === "0") payload.set(name, entry.body);
  }
  if (!payload.size) throw new Error("empty archive payload");
  return payload;
}
function zipPayload(text) {
  const entries = JSON.parse(text), seen = new Set(), payload = new Map();
  if (!Array.isArray(entries) || !entries.length || entries.length > 20000) throw new Error("invalid dependency ZIP inventory");
  let root, expanded = 0;
  for (const entry of entries) {
    if (!entry || typeof entry.directory !== "boolean" || typeof entry.path !== "string" || typeof entry.data !== "string") throw new Error("invalid ZIP member");
    const name = entry.directory ? entry.path.replace(/\/$/, "") : entry.path, key = name.toLowerCase();
    if (!canonical(name) || seen.has(key)) throw new Error("unsafe or duplicate ZIP member");
    seen.add(key);
    const parts = name.split("/"); root ??= parts[0];
    if (parts[0] !== root || (parts.length === 1 && !entry.directory)) throw new Error("ZIP archive lacks a single package root");
    const bytes = Buffer.from(entry.data, "base64");
    if (bytes.toString("base64") !== entry.data || (entry.directory && bytes.length)) throw new Error("invalid ZIP contents");
    expanded += bytes.length; if (expanded > 32 * 1024 * 1024) throw new Error("ZIP expansion exceeds bound");
    if (!entry.directory) payload.set(parts.slice(1).join("/"), bytes);
  }
  if (!payload.has("composer.json")) throw new Error("ZIP package metadata missing");
  return payload;
}

// Commands and pins are injectable for offline staging; the CLI uses real tools.
export function qualify(args, { command = spawnSync, toolchain = pinned } = {}) {
  if (process.platform !== "win32" && command === spawnSync) throw new Error("PHP qualification requires the admitted Windows runtime");
  const packageRoot = realpathSync(args.package), authorityRoot = realpathSync(args.authority), runtime = realpathSync(args["php-home"]);
  const composer = realpathSync(args.composer), extension = realpathSync(args["grpc-extension"]), archiver = realpathSync(args.archiver), cache = realpathSync(args.cache);
  const output = join(realpathSync(dirname(resolve(args.output))), basename(resolve(args.output)));
  try { lstatSync(output); throw new Error("qualification output must be absent"); }
  catch (error) { if (error.code !== "ENOENT") throw error; }
  for (const input of [packageRoot, authorityRoot, runtime, composer, extension, archiver, cache, dirname(directory)]) {
    if (within(input, output) || within(output, input)) throw new Error("qualification output overlaps an input");
  }
  const receiptBytes = readFileSync(join(packageRoot, "generation-receipt.json")), receipt = JSON.parse(receiptBytes), approved = loadAuthority(authorityRoot);
  if (receipt.schema !== "acyclic.sdk.php-producer-receipt.v1" || receipt.target !== "php" || receipt.authority !== "rust" || !Array.isArray(receipt.outputs)) throw new Error("unsupported generation receipt");
  if (sha256(approved.bytes) !== receipt.authority_manifest_sha256 || approved.manifest.source_revision !== receipt.source_revision) throw new Error("generation authority differs");
  if (targets.some(name => !approved.descriptors.has(name))) throw new Error("tested family lacks attested descriptors");
  const payload = new Map(), metadata = ["composer.json", "LICENSE", "NOTICE", "authority/rust-authority.json"], names = new Set();
  for (const name of receipt.outputs) {
    if (!canonical(name) || names.has(name.toLowerCase()) || (!metadata.includes(name) && !/^src\/.+\.php$/.test(name))) throw new Error("unsafe, duplicate or unexpected package output");
    names.add(name.toLowerCase()); payload.set(name, readInput(packageRoot, name, receipt.output_sha256[name]));
  }
  const template = read("../templates/package/composer.json");
  if (metadata.some(name => !payload.has(name)) || sha256(payload.get("composer.json")) !== sha256(template)
    || sha256(payload.get("authority/rust-authority.json")) !== sha256(approved.bytes)) throw new Error("package lacks pinned metadata");
  const sdk = JSON.parse(template);
  for (const [family, version, service] of [["Actors", "V1", "Actors"], ["Workers", "V1", "Workers"], ["Stream", "V2", "Stream"]]) {
    if (!payload.has(`src/GPBMetadata/${family}/${version}/${family}.php`) || !payload.has(`src/Acyclic/${family}/${version}/${service}ServiceClient.php`)) throw new Error("package lacks tested family bindings");
  }
  const verifyRuntime = () => {
    if (!toolchain.runtime_files || !Object.keys(toolchain.runtime_files).length || JSON.stringify(files(runtime)) !== JSON.stringify(Object.keys(toolchain.runtime_files).sort())) throw new Error("runtime inventory differs");
    for (const [name, digest] of Object.entries(toolchain.runtime_files)) readInput(runtime, name, digest);
  };
  verifyRuntime();
  const archiverPin = toolchain.archiver?.[`${process.platform}-${process.arch}`];
  const tools = [[composer, toolchain.composer.sha256], [extension, toolchain.grpc_extension.binary_sha256], [archiver, archiverPin?.sha256]];
  const verifyTools = () => { for (const [file, digest] of tools) if (sha256(readFileSync(file)) !== digest) throw new Error("qualification tool differs from pin"); };
  verifyTools();
  const controls = ["installed_consumer.php", "native_client.php", "archive_inventory.php", ...Object.keys(negatives).map(name => `${name}.php`)];
  const controlBytes = new Map(controls.map(name => [name, read(`../tests/fixtures/consumer/${name}`)]));
  const code = new Map([["qualifier", readFileSync(fileURLToPath(import.meta.url))], ["authority_reader", read("../../../shared/authority.mjs")], ["archive_reader", read("../../../../../scripts/archive-utils.mjs")]]);
  const env = { ...process.env };
  for (const key of Object.keys(env)) if (/^(COMPOSER|PHP|TAR_OPTIONS$|XDG_)/i.test(key)) delete env[key];
  const home = join(output, "home"), project = join(output, "project");
  Object.assign(env, { HOME: home, USERPROFILE: home, APPDATA: join(home, "appdata"), LOCALAPPDATA: join(home, "local"),
    XDG_CONFIG_HOME: join(home, "config"), TMP: join(home, "tmp"), TEMP: join(home, "tmp"),
    COMPOSER_HOME: join(home, "composer"), COMPOSER_CACHE_DIR: join(output, "cache"), COMPOSER_DISABLE_NETWORK: "1",
    PATH: [runtime, dirname(archiver)].join(delimiter) });
  const php = realpathSync(join(runtime, "php.exe"));
  const phpArgs = ["-n", "-d", "memory_limit=256M", "-d", "display_errors=stderr", "-d", `extension=${join(runtime, "ext/php_zip.dll")}`,
    "-d", `extension=${join(runtime, "ext/php_openssl.dll")}`, "-d", `extension=${extension}`];
  const execute = (exe, argv, cwd) => command(exe, argv, { cwd, env, encoding: "utf8", timeout: 180_000, maxBuffer: 48 * 1024 * 1024 });
  const version = execute(php, [...phpArgs, "-r", 'echo json_encode([PHP_VERSION,PHP_INT_SIZE,phpversion("grpc")]);'], directory);
  if (version.error || version.status !== 0 || version.stderr || version.stdout !== JSON.stringify([toolchain.php_version, 8, toolchain.grpc_extension.version])) throw new Error("PHP runtime or extension version differs");
  const composerVersion = execute(php, [...phpArgs, composer, "--version", "--no-ansi"], directory);
  if (composerVersion.error || composerVersion.status !== 0 || !composerVersion.stdout.startsWith(`Composer version ${toolchain.composer.version} `)) throw new Error("Composer version differs");
  const tarVersion = execute(archiver, ["--version"], directory);
  if (tarVersion.error || tarVersion.status !== 0 || tarVersion.stdout.trim() !== archiverPin.version) throw new Error("archiver version differs");
  const archives = new Map(), dependencies = [];
  for (const pin of toolchain.dependencies ?? []) {
    if (!/^[a-z0-9][a-z0-9_.-]*\/[a-z0-9][a-z0-9_.-]*$/.test(pin.name) || pin.name === sdk.name || !/^\d+\.\d+\.\d+$/.test(pin.version) || !/^[a-f0-9]{40}$/.test(pin.source?.reference)) throw new Error("unsafe dependency coordinate");
    const filename = `${pin.name.replace("/", "-")}-${pin.version}.zip`;
    if (archives.has(filename)) throw new Error("duplicate dependency archive");
    const bytes = readInput(cache, filename, pin.archive_sha256);
    if (!bytes.length || bytes.length > 16 * 1024 * 1024) throw new Error("ZIP archive exceeds size bound");
    const inspection = execute(php, [...phpArgs, join(directory, "../tests/fixtures/consumer/archive_inventory.php"), join(cache, filename)], directory);
    if (inspection.error || inspection.status !== 0 || inspection.stderr) throw new Error("dependency ZIP inspection failed");
    const contents = zipPayload(inspection.stdout);
    if (JSON.stringify(JSON.parse(contents.get("composer.json"))) !== JSON.stringify(pin.composer)) throw new Error("dependency metadata differs");
    archives.set(filename, bytes); dependencies.push({ pin, filename, contents });
  }
  if (!dependencies.length) throw new Error("dependency closure missing");
  mkdirSync(output);
  const put = (name, bytes) => { const path = join(output, name); mkdirSync(dirname(path), { recursive: true }); writeFileSync(path, bytes, { flag: "wx" }); return path; };
  for (const [name, bytes] of payload) put(`package-source/${name}`, bytes);
  for (const [name, bytes] of controlBytes) put(`project/${name}`, bytes);
  mkdirSync(env.TEMP, { recursive: true });
  const logs = [];
  const run = (name, exe, argv, cwd = output, negative = false) => {
    const result = execute(exe, argv, cwd), text = (result.stdout ?? "") + (result.stderr ?? "");
    put(`${name}.log`, text); logs.push(`${name}.log`);
    if (result.error || (negative ? result.status === 0 || result.status === null : result.status !== 0)) throw new Error(`${name} failed: ${text}`);
    return text;
  };
  const archiveOptions = ["--format", "ustar", "--options", "gzip:!timestamp", "--mtime", "@1767225600", "--uid", "0", "--gid", "0", "--uname", "root", "--gname", "root"];
  const built = join(output, "package", packageName); mkdirSync(dirname(built));
  run("build-package", archiver, [...archiveOptions, "-czf", built, "-C", join(output, "package-source"), ...[...payload.keys()].sort()]);
  const archive = readFileSync(built), builtPayload = archivePayload(archive);
  if (archive.readUInt32LE(4) !== 0 || JSON.stringify([...builtPayload.keys()].sort()) !== JSON.stringify([...payload.keys()].sort())) throw new Error("built archive metadata or inventory differs");
  for (const [name, bytes] of payload) if (sha256(builtPayload.get(name)) !== sha256(bytes)) throw new Error("built package payload differs");
  const repositories = [{ type: "package", package: { ...sdk, dist: { type: "tar", url: pathToFileURL(built).href } } }];
  for (const { pin, filename } of dependencies) {
    const feed = put(`feed/${filename}`, archives.get(filename));
    repositories.push({ type: "package", package: { name: pin.name, version: pin.version, require: pin.composer.require, autoload: pin.composer.autoload,
      dist: { type: "zip", url: pathToFileURL(feed).href, reference: pin.source.reference } } });
  }
  repositories.push({ "packagist.org": false });
  const projectBytes = Buffer.from(JSON.stringify({ name: "acyclic/qualification-consumer", version: "1.0.0", require: { [sdk.name]: sdk.version }, repositories,
    config: { "allow-plugins": false, "vendor-dir": "vendor" } }, null, 2) + "\n");
  put("project/composer.json", projectBytes);
  const composerOptions = ["--no-plugins", "--no-scripts", "--no-interaction", "--no-dev", "--prefer-dist", "--classmap-authoritative", "--no-ansi", "--no-progress"];
  run("resolve-offline", php, [...phpArgs, composer, "update", "--no-install", "--no-audit", ...composerOptions], project);
  const lock = readFileSync(join(project, "composer.lock")), locked = JSON.parse(lock);
  const expectedPackages = new Map([[sdk.name, { version: sdk.version, url: pathToFileURL(built).href }], ...dependencies.map(({ pin, filename }) => [pin.name, { version: pin.version, url: pathToFileURL(join(output, "feed", filename)).href, reference: pin.source.reference }])]);
  const checkPackages = packages => {
    if (!Array.isArray(packages) || packages.length !== expectedPackages.size) throw new Error("resolved dependency inventory differs");
    const seen = new Set();
    for (const pkg of packages) {
      const expected = expectedPackages.get(pkg.name);
      if (!expected || seen.has(pkg.name) || pkg.version.replace(/^v/, "") !== expected.version || pkg.dist?.url !== expected.url || pkg.dist?.reference !== expected.reference) throw new Error("resolved package provenance differs");
      seen.add(pkg.name);
    }
  };
  checkPackages(locked.packages);
  if (locked["packages-dev"]?.length) throw new Error("unexpected development dependencies");
  run("install-offline", php, [...phpArgs, composer, "install", ...composerOptions], project);
  if (sha256(readFileSync(join(project, "composer.lock"))) !== sha256(lock) || sha256(readFileSync(join(project, "composer.json"))) !== sha256(projectBytes)) throw new Error("consumer lock or metadata changed");
  const vendor = join(project, "vendor"), installed = join(vendor, ...sdk.name.split("/"));
  const installedPackages = JSON.parse(readFileSync(join(vendor, "composer/installed.json")));
  checkPackages(installedPackages.packages);
  const loaderPayload = new Map(["autoload.php", ...files(join(vendor, "composer")).map(name => `composer/${name}`)].map(name => [name, readFileSync(join(vendor, name))]));
  const vendorInventory = [...loaderPayload.keys(), ...[...payload.keys()].map(name => `${sdk.name}/${name}`),
    ...dependencies.flatMap(({ pin, contents }) => [...contents.keys()].map(name => `${pin.name}/${name}`))].sort();
  const verifyInstallation = () => {
    if (JSON.stringify(files(vendor)) !== JSON.stringify(vendorInventory)) throw new Error("installed vendor inventory differs");
    verifyPayload(installed, payload);
    for (const { pin, contents } of dependencies) verifyPayload(join(vendor, ...pin.name.split("/")), contents);
    for (const [name, bytes] of loaderPayload) readInput(vendor, name, sha256(bytes));
  };
  verifyInstallation();
  const autoload = join(vendor, "autoload.php"), descriptors = targets.map((name, index) => put(`authority/${index}.bin`, approved.inputs.get(approved.descriptors.get(name))));
  const positive = run("positive", php, [...phpArgs, "installed_consumer.php", autoload, installed, ...descriptors], project);
  if (positive.trim() !== marker) throw new Error("positive consumer did not report completed controls");
  const native = run("native-client", php, [...phpArgs, "native_client.php", autoload], project);
  if (native.trim() !== nativeMarker) throw new Error("native client did not report completed controls");
  for (const [name, expected] of Object.entries(negatives)) {
    const text = run(`negative-${name}`, php, [...phpArgs, `${name}.php`, autoload], project, true);
    if ((text.match(/Fatal error:/g) ?? []).length !== 1 || !text.includes(`Uncaught ${expected} in `) || !text.includes("GPBUtil.php:") || !text.includes(`${name}.php(3)`)
      || /Warning:|Parse error:|Deprecated:/.test(text)) throw new Error(`${name} did not reject the intended assignment`);
  }
  verifyInstallation(); verifyRuntime(); verifyTools();
  if (sha256(readFileSync(join(project, "composer.lock"))) !== sha256(lock) || sha256(readFileSync(join(project, "composer.json"))) !== sha256(projectBytes)) throw new Error("consumer lock or metadata changed after controls");
  for (const [name, bytes] of controlBytes) if (sha256(readFileSync(join(project, name))) !== sha256(bytes)) throw new Error("consumer control changed");
  if (sha256(readFileSync(fileURLToPath(import.meta.url))) !== sha256(code.get("qualifier"))) throw new Error("qualifier source changed");
  const result = { schema: "acyclic.sdk.php.installed-qualification.v1", scope: "installed-php-transport-bindings", source_revision: approved.manifest.source_revision,
    authority_manifest_sha256: sha256(approved.bytes), generation_receipt_sha256: sha256(receiptBytes), archive_sha256: sha256(archive), archive_options: archiveOptions,
    runtime_version: JSON.parse(version.stdout), composer_version: composerVersion.stdout.trim(), archiver_version: tarVersion.stdout.trim(),
    runtime_source: toolchain.runtime_archive, runtime_files_sha256: toolchain.runtime_files, composer_sha256: toolchain.composer.sha256, grpc_extension_sha256: toolchain.grpc_extension.binary_sha256,
    archiver_sha256: archiverPin.sha256, toolchain_sha256: sha256(JSON.stringify(toolchain)),
    ...Object.fromEntries([...code].map(([name, bytes]) => [`${name}_sha256`, sha256(bytes)])),
    package_payload_sha256: Object.fromEntries([...payload].map(([name, bytes]) => [name, sha256(bytes)])),
    dependency_archive_sha256: Object.fromEntries([...archives].map(([name, bytes]) => [name, sha256(bytes)])),
    autoloader_file_sha256: Object.fromEntries([...loaderPayload].map(([name, bytes]) => [name, sha256(bytes)])),
    installed_dependency_file_sha256: Object.fromEntries(dependencies.map(({ pin }) => [pin.name, Object.fromEntries(files(join(vendor, ...pin.name.split("/"))).map(name => [name, sha256(readFileSync(join(vendor, ...pin.name.split("/"), name)))]))])),
    project_sha256: Object.fromEntries(files(project).filter(name => !name.startsWith("vendor/")).map(name => [name, sha256(readFileSync(join(project, name)))])),
    log_sha256: Object.fromEntries(logs.map(name => [name, sha256(readFileSync(join(output, name)))])), positive_controls_passed: true,
    independent_negative_runtime_type_controls_rejected: Object.keys(negatives).length, native_client_creation_shutdown_passed: true,
    complete_file_descriptor_comparison: true, descriptor_normalization: ["source_code_info", "Buf image tag 8042"], rust_backed_rpc_qualified: false, embedded_runtime_qualified: false };
  put("qualification.json", JSON.stringify(result, null, 2) + "\n");
  return result;
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const options = Object.fromEntries(["package", "authority", "php-home", "composer", "grpc-extension", "archiver", "cache", "output"].map(name => [name, { type: "string" }]));
  const { values } = parseArgs({ options });
  if (Object.keys(options).some(name => !values[name])) throw new Error("all qualification paths are required");
  const result = qualify(values); console.log(JSON.stringify({ scope: result.scope, archive_sha256: result.archive_sha256, qualification: join(resolve(values.output), "qualification.json") }, null, 2));
}
