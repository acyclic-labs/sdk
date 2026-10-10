import { spawnSync } from "node:child_process";
import { lstatSync, mkdirSync, readFileSync, readdirSync, realpathSync, writeFileSync } from "node:fs";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { parseArgs } from "node:util";
import { gunzipSync } from "node:zlib";
import { tarEntries } from "../../../../../scripts/archive-utils.mjs";
import { canonical, loadAuthority, readInput, sha256, within } from "../../../shared/authority.mjs";

const directory = dirname(fileURLToPath(import.meta.url));
const read = name => readFileSync(join(directory, name));
const sdkInventory = JSON.parse(read("../toolchains/sdk-files.json"));
if (!Array.isArray(sdkInventory) || sdkInventory.length === 0 || sdkInventory.some(entry =>
  !entry || !canonical(entry.path) || !/^[a-f0-9]{64}$/.test(entry.sha256))
  || new Set(sdkInventory.map(entry => process.platform === "win32" ? entry.path.toLowerCase() : entry.path)).size !== sdkInventory.length) {
  throw new Error("invalid admitted SDK file inventory");
}
const pinned = { ...JSON.parse(read("../toolchains/toolchain.json")),
  runtime_files: Object.fromEntries(sdkInventory.map(entry => [entry.path, entry.sha256])),
  dependencies: JSON.parse(read("../toolchains/dependencies.json")) };
const targets = ["actors/v1/actors.proto", "workers/v1/workers.proto", "stream/v1/stream.proto"];
const negatives = { InvalidActorBytes: "List<int>?", InvalidWorkerBytes: "List<int>?", InvalidOptionalInteger: "Int64?" };
const marker = "PASS: installed Dart descriptors, bytes, unsigned bits, optional presence, oneof and gRPC shapes";
const packageName = "acyclic_sdk_transport-0.2.0-alpha.1.tar.gz";

function files(root, prefix = "") {
  return readdirSync(join(root, prefix), { withFileTypes: true }).flatMap(entry => {
    const name = prefix ? `${prefix}/${entry.name}` : entry.name;
    if (entry.isSymbolicLink()) throw new Error("inventory contains a link");
    if (entry.isDirectory()) return files(root, name);
    if (!entry.isFile()) throw new Error("inventory contains a non-file");
    return [name];
  }).sort();
}

function archivePayload(bytes) {
  if (bytes.length > 32 * 1024 * 1024) throw new Error("archive exceeds size bound");
  const entries = tarEntries(gunzipSync(bytes, { maxOutputLength: 256 * 1024 * 1024 }));
  const seen = new Set(), payload = new Map();
  for (const entry of entries) {
    const name = entry.type === "5" ? entry.path.replace(/\/$/, "") : entry.path;
    const key = process.platform === "win32" ? name.toLowerCase() : name;
    if (!canonical(name) || seen.has(key) || !["0", "5"].includes(entry.type)) throw new Error("unsafe or duplicate archive member");
    seen.add(key);
    if (entry.type === "0") payload.set(name, entry.body);
  }
  if (payload.size === 0) throw new Error("empty archive payload");
  return payload;
}

function verifyPayload(root, payload) {
  if (JSON.stringify(files(root)) !== JSON.stringify([...payload.keys()].sort())) throw new Error("installed payload inventory differs");
  for (const [name, bytes] of payload) readInput(root, name, sha256(bytes));
}

// This adapter enumerates maintained reflection variables and message factories;
// schema expectations still come from verified Rust descriptor bytes at runtime.
function reflection(payload) {
  let imports = "import 'package:protobuf/protobuf.dart';\nimport 'package:grpc/service_api.dart' as grpc;\n";
  const maps = [], factories = [], clients = [];
  targets.forEach((source, index) => {
    const family = source.slice(0, -6);
    imports += `import 'package:acyclic_sdk_transport/${family}.pbjson.dart' as j${index};\nimport 'package:acyclic_sdk_transport/${family}.pb.dart' as p${index};\nimport 'package:acyclic_sdk_transport/${family}.pbgrpc.dart' as g${index};\n`;
    const json = payload.get(`lib/${family}.pbjson.dart`)?.toString("utf8") ?? "";
    const entries = [...json.matchAll(/\/\/\/ Descriptor for `([^`]+)`\. Decode as a `google\.protobuf\.(?:DescriptorProto|EnumDescriptorProto)`\.\s*final \$typed_data\.Uint8List (\w+) =/g)];
    if (entries.length === 0 || new Set(entries.map(entry => entry[1])).size !== entries.length) throw new Error("missing or duplicate maintained reflection descriptors");
    maps.push(`{${entries.map(([, name, variable]) => `${JSON.stringify(name)}:j${index}.${variable}`).join(",")}}`);
    const messages = payload.get(`lib/${family}.pb.dart`)?.toString("utf8") ?? "";
    const classes = [...messages.matchAll(/class (\w+) extends \$pb\.GeneratedMessage/g)];
    if (classes.length === 0) throw new Error("missing maintained message factories");
    for (const [, name] of classes) factories.push(`p${index}.${name}.create`);
    const grpc = payload.get(`lib/${family}.pbgrpc.dart`).toString("utf8");
    const client = grpc.match(/class (\w+Client) extends \$grpc\.Client/);
    const methods = [...grpc.matchAll(/static final _\$(\w+)\s*=\s*\$grpc\.ClientMethod<\s*\$0\.(\w+),\s*\$0\.(\w+)>\(\s*'([^']+)'/g)];
    if (!client || methods.length === 0 || new Set(methods.map(entry => entry[4])).size !== methods.length) throw new Error("missing or duplicate maintained client descriptors");
    const calls = methods.map(([, name, input, output, path]) => {
      const signature = grpc.match(new RegExp(`\\$grpc\\.(ResponseFuture|ResponseStream)<\\$0\\.(\\w+)>\\s+${name}\\(\\s*(\\$async\\.Stream<\\$0\\.(\\w+)>|\\$0\\.(\\w+))\\s+\\w+`));
      if (!signature || signature[2] !== output || (signature[4] ?? signature[5]) !== input) throw new Error("maintained client signature differs from descriptor");
      const streaming = signature[4] !== undefined;
      const request = streaming ? `Stream<p${index}.${input}>.value(message as p${index}.${input})` : `message as p${index}.${input}`;
      return `${JSON.stringify(path)}:ClientProbe(${streaming},${signature[1] === "ResponseStream"},(channel,message)=>g${index}.${client[1]}(channel).${name}(${request}))`;
    });
    clients.push(`{${calls.join(",")}}`);
  });
  return `${imports}class ClientProbe { final bool clientStreaming,serverStreaming; final dynamic Function(grpc.ClientChannel,GeneratedMessage) invoke; ClientProbe(this.clientStreaming,this.serverStreaming,this.invoke); }\nfinal clientProbes=<Map<String,ClientProbe>>[${clients.join(",")}];\nfinal sourceNames=${JSON.stringify(targets)};\nfinal descriptors=<Map<String,List<int>>>[${maps.join(",")}];\nfinal messageFactories=<String,GeneratedMessage Function()>{for(final factory in <GeneratedMessage Function()>[${factories.join(",")}]) '.\${factory().info_.qualifiedMessageName}':factory};\n`;
}

// Injected commands/pins allow offline staging controls; the CLI uses real tools.
export function qualify(args, { command = spawnSync, toolchain = pinned } = {}) {
  if (process.platform !== "win32" && command === spawnSync) throw new Error("installed Dart qualification currently requires the admitted Windows runtime");
  const packageRoot = realpathSync(args.package), authorityRoot = realpathSync(args.authority);
  const runtime = realpathSync(args["dart-home"]), archiver = realpathSync(args.archiver), cache = realpathSync(args.cache);
  const output = join(realpathSync(dirname(resolve(args.output))), basename(resolve(args.output)));
  try { lstatSync(output); throw new Error("qualification output must be absent"); }
  catch (error) { if (error.code !== "ENOENT") throw error; }
  for (const input of [packageRoot, authorityRoot, runtime, archiver, cache, dirname(directory)]) {
    if (within(input, output) || within(output, input)) throw new Error("qualification output overlaps an input");
  }
  const receiptBytes = readFileSync(join(packageRoot, "generation-receipt.json")), receipt = JSON.parse(receiptBytes);
  if (receipt.schema !== "acyclic.sdk.dart-producer-receipt.v1" || receipt.target !== "dart" || receipt.authority !== "rust"
    || !Array.isArray(receipt.outputs)) throw new Error("unsupported generation receipt");
  const approved = loadAuthority(authorityRoot);
  if (sha256(approved.bytes) !== receipt.authority_manifest_sha256 || approved.manifest.source_revision !== receipt.source_revision) throw new Error("generation authority differs");
  if (targets.some(name => !approved.descriptors.has(name))) throw new Error("tested family lacks an attested descriptor");
  const payload = new Map(), metadata = ["pubspec.yaml", "LICENSE", "NOTICE", "authority/rust-authority.json"];
  for (const name of receipt.outputs) {
    if (!canonical(name) || payload.has(name) || (!metadata.includes(name) && !/^lib\/.+\.dart$/.test(name))) throw new Error("unsafe, duplicate or unexpected package output");
    payload.set(name, readInput(packageRoot, name, receipt.output_sha256[name]));
  }
  if (metadata.some(name => !payload.has(name)) || sha256(payload.get("pubspec.yaml")) !== sha256(read("../templates/package/pubspec.yaml"))
    || sha256(payload.get("authority/rust-authority.json")) !== sha256(approved.bytes)) throw new Error("package lacks pinned metadata");
  for (const target of targets) for (const suffix of ["pb.dart", "pbenum.dart", "pbjson.dart", "pbgrpc.dart"]) {
    if (!payload.has(`lib/${target.slice(0, -6)}.${suffix}`)) throw new Error("package lacks tested family bindings");
  }
  const adapter = reflection(payload);
  if (!toolchain.runtime_files || Object.keys(toolchain.runtime_files).length === 0) throw new Error("runtime pins missing");
  if (JSON.stringify(files(runtime)) !== JSON.stringify(Object.keys(toolchain.runtime_files).sort())) throw new Error("runtime inventory differs");
  for (const [name, digest] of Object.entries(toolchain.runtime_files)) readInput(runtime, name, digest);
  const dart = realpathSync(join(runtime, "bin/dart.exe"));
  const archiverPin = toolchain.archiver[`${process.platform}-${process.arch}`];
  if (!archiverPin || sha256(readFileSync(archiver)) !== archiverPin.sha256) throw new Error("archiver differs from admitted pin");
  const archives = new Map(), dependencies = [];
  for (const pin of toolchain.dependencies ?? []) {
    if (!/^[a-z_][a-z0-9_]*$/.test(pin.name) || pin.name === "acyclic_sdk_transport" || !/^[0-9]+\.[0-9]+\.[0-9]+(?:[+-][a-zA-Z0-9.+-]+)?$/.test(pin.version)) throw new Error("unsafe dependency coordinate");
    const filename = `${pin.name}-${pin.version}.tar.gz`;
    if (archives.has(filename)) throw new Error("duplicate dependency archive");
    const bytes = readInput(cache, filename, pin.sha256), contents = archivePayload(bytes);
    if (!contents.has("pubspec.yaml")) throw new Error("dependency lacks package metadata");
    archives.set(filename, bytes); dependencies.push({ pin, filename, contents });
  }
  if (archives.size === 0) throw new Error("dependency closure missing");
  const templates = new Map(["pubspec.yaml", "pubspec.lock"].map(name => [name, read(`../templates/qualification/${name}`)]));
  const controls = ["installed_consumer.dart", ...Object.keys(negatives).map(name => `${name}.dart`)];
  const controlBytes = new Map(controls.map(name => [name, read(`../tests/fixtures/consumer/${name}`)]));
  const env = { ...process.env };
  for (const key of Object.keys(env)) if (/^(DART|PUB_|FLUTTER|TAR_OPTIONS$)/i.test(key)) delete env[key];
  Object.assign(env, { HOME: join(output, "home"), USERPROFILE: join(output, "home"),
    APPDATA: join(output, "home/appdata"), LOCALAPPDATA: join(output, "home/local"), XDG_CONFIG_HOME: join(output, "home/config"),
    PUB_CACHE: join(output, "cache"), DART_SUPPRESS_ANALYTICS: "true", SDK_QUALIFIED_PACKAGE: join(output, "sdk") });
  const execute = (exe, argv, cwd) => command(exe, argv, { cwd, env, encoding: "utf8", timeout: 180_000, maxBuffer: 8 * 1024 * 1024 });
  const runtimeVersion = execute(dart, ["--version"], directory), versionText = (runtimeVersion.stdout ?? "") + (runtimeVersion.stderr ?? "");
  if (runtimeVersion.error || runtimeVersion.status !== 0 || !versionText.startsWith(`Dart SDK version: ${toolchain.dart_sdk_version} (stable)`) || !versionText.includes('"windows_x64"')) throw new Error("Dart runtime version differs from pin");
  const tarVersion = execute(archiver, ["--version"], directory);
  if (tarVersion.error || tarVersion.status !== 0 || tarVersion.stdout.trim() !== archiverPin.version) throw new Error("archiver version differs from pin");
  mkdirSync(output);
  const put = (name, bytes) => { const path = join(output, name); mkdirSync(dirname(path), { recursive: true }); writeFileSync(path, bytes, { flag: "wx" }); return path; };
  for (const [name, bytes] of payload) put(`package-source/${name}`, bytes);
  for (const [name, bytes] of templates) put(`project/${name}`, bytes);
  for (const [name, bytes] of controlBytes) put(`project/${name}`, bytes);
  put("project/reflection.dart", adapter);
  mkdirSync(env.HOME, { recursive: true });
  const logs = [];
  const run = (name, exe, argv, cwd = output, negative = false) => {
    const result = execute(exe, argv, cwd), text = (result.stdout ?? "") + (result.stderr ?? "");
    put(`${name}.log`, text); logs.push(`${name}.log`);
    if (result.error || (negative ? result.status === 0 || result.status === null : result.status !== 0)) throw new Error(`${name} failed: ${text}`);
    return text;
  };
  const built = join(output, "package", packageName); mkdirSync(dirname(built));
  const archiveOptions = ["--format", "ustar", "--options", "gzip:!timestamp", "--mtime", "@1767225600", "--uid", "0", "--gid", "0", "--uname", "root", "--gname", "root"];
  run("build-package", archiver, [...archiveOptions, "-czf", built, "-C", join(output, "package-source"), ...[...payload.keys()].sort()]);
  const archive = readFileSync(built), builtPayload = archivePayload(archive);
  if (archive.readUInt32LE(4) !== 0 || JSON.stringify([...builtPayload.keys()].sort()) !== JSON.stringify([...payload.keys()].sort())) throw new Error("built archive metadata or inventory differs");
  for (const [name, bytes] of payload) if (sha256(builtPayload.get(name)) !== sha256(bytes)) throw new Error("built package payload differs");
  const installed = join(output, "sdk"); mkdirSync(installed);
  run("install-sdk", archiver, ["-xzf", built, "-C", installed]); verifyPayload(installed, payload);
  for (const { pin, filename, contents } of dependencies) {
    const feed = put(`feed/${filename}`, archives.get(filename));
    const root = join(env.PUB_CACHE, "hosted/pub.dev", `${pin.name}-${pin.version}`); mkdirSync(root, { recursive: true });
    run(`install-${pin.name}`, archiver, ["-xzf", feed, "-C", root]); verifyPayload(root, contents);
  }
  const project = join(output, "project");
  run("resolve-offline", dart, ["pub", "get", "--offline", "--enforce-lockfile"], project);
  if (sha256(readFileSync(join(project, "pubspec.lock"))) !== sha256(templates.get("pubspec.lock"))) throw new Error("consumer lock changed");
  const configPath = join(project, ".dart_tool/package_config.json"), config = JSON.parse(readFileSync(configPath));
  const roots = new Map([["acyclic_sdk_transport", installed], ["sdk_dart_consumer", project], ...dependencies.map(({ pin }) => [pin.name, join(env.PUB_CACHE, "hosted/pub.dev", `${pin.name}-${pin.version}`)])]);
  if (!Array.isArray(config.packages) || config.packages.length !== roots.size) throw new Error("resolved package inventory differs");
  const seen = new Set();
  for (const pkg of config.packages) {
    if (seen.has(pkg.name) || !roots.has(pkg.name) || pkg.packageUri !== "lib/" || realpathSync(fileURLToPath(new URL(pkg.rootUri, pathToFileURL(configPath)))) !== realpathSync(roots.get(pkg.name))) throw new Error("resolved package provenance differs");
    seen.add(pkg.name);
  }
  run("compile-positive", dart, ["compile", "kernel", "installed_consumer.dart", "-o", join(output, "consumer.dill")], project);
  const descriptors = targets.map((name, index) => put(`authority/${index}.bin`, approved.inputs.get(approved.descriptors.get(name))));
  const positive = run("positive", dart, [`--packages=${configPath}`, join(output, "consumer.dill"), ...descriptors], project);
  if (!positive.split(/\r?\n/).includes(marker)) throw new Error("positive consumer did not report completed controls");
  for (const [name, type] of Object.entries(negatives)) {
    const text = run(`negative-${name}`, dart, ["analyze", "--format", "machine", `${name}.dart`], project, true);
    const lines = text.trim().split(/\r?\n/);
    if (lines.length !== 1 || !lines[0].startsWith("ERROR|COMPILE_TIME_ERROR|ARGUMENT_TYPE_NOT_ASSIGNABLE|") || !lines[0].includes(`${name}.dart|2|`) || !lines[0].includes(`parameter type '${type}'`)) throw new Error(`${name} did not reject the intended assignment`);
  }
  verifyPayload(installed, payload);
  for (const { pin, contents } of dependencies) verifyPayload(join(env.PUB_CACHE, "hosted/pub.dev", `${pin.name}-${pin.version}`), contents);
  if (JSON.stringify(files(runtime)) !== JSON.stringify(Object.keys(toolchain.runtime_files).sort())) throw new Error("runtime inventory changed during qualification");
  for (const [name, digest] of Object.entries(toolchain.runtime_files)) readInput(runtime, name, digest);
  const result = {
    schema: "acyclic.sdk.dart.installed-qualification.v1", scope: "installed-dart-transport-bindings",
    source_revision: approved.manifest.source_revision, authority_manifest_sha256: sha256(approved.bytes), generation_receipt_sha256: sha256(receiptBytes),
    dart_version: versionText.trim(), runtime_source: toolchain.dart_sdk_archive, runtime_files_sha256: toolchain.runtime_files,
    archive_sha256: sha256(archive), archiver_sha256: archiverPin.sha256, archive_options: archiveOptions,
    package_payload_sha256: Object.fromEntries([...payload].map(([name, bytes]) => [name, sha256(bytes)])),
    dependency_archive_sha256: Object.fromEntries([...archives].map(([name, bytes]) => [name, sha256(bytes)])),
    installed_dependency_file_sha256: Object.fromEntries(files(env.PUB_CACHE).map(name => [name, sha256(readFileSync(join(env.PUB_CACHE, name)))])),
    qualifier_sha256: sha256(readFileSync(fileURLToPath(import.meta.url))), authority_reader_sha256: sha256(read("../../../shared/authority.mjs")),
    archive_reader_sha256: sha256(read("../../../../../scripts/archive-utils.mjs")), toolchain_sha256: sha256(JSON.stringify(toolchain)),
    project_sha256: Object.fromEntries(files(project).map(name => [name, sha256(readFileSync(join(project, name)))])),
    log_sha256: Object.fromEntries(logs.map(name => [name, sha256(readFileSync(join(output, name)))])),
    positive_controls_passed: true, independent_negative_static_type_controls_rejected: 3, complete_file_descriptor_comparison: false,
    descriptor_scope: "message/enum descriptors including nested contents, and gRPC method/message/streaming shapes",
    rust_backed_rpc_qualified: false, embedded_runtime_qualified: false,
  };
  put("qualification.json", JSON.stringify(result, null, 2) + "\n"); return result;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const options = Object.fromEntries(["package", "authority", "dart-home", "archiver", "cache", "output"].map(name => [name, { type: "string" }]));
  const { values } = parseArgs({ options });
  if (Object.keys(options).some(name => !values[name])) throw new Error("--package, --authority, --dart-home, --archiver, --cache and --output are required");
  const result = qualify(values); console.log(JSON.stringify({ scope: result.scope, archive_sha256: result.archive_sha256, qualification: resolve(values.output, "qualification.json") }, null, 2));
}
