import { spawnSync } from "node:child_process";
import { lstatSync, mkdirSync, readFileSync, realpathSync, writeFileSync } from "node:fs";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { parseArgs } from "node:util";
import { readInput, sha256, within } from "../../../shared/authority.mjs";
import { readPackage, targets } from "./package.mjs";
import { tree, verifyRuntime, verifySource } from "./provenance.mjs";

const directory = dirname(fileURLToPath(import.meta.url));
const read = name => readFileSync(join(directory, name));
const pinned = JSON.parse(read("../toolchains/qualification.json"));
const markers = [
  "PASS Swift bytes, unsigned bounds, optional zero, populated oneofs, gRPC codecs and bounded RPC metadata",
  "PASS Swift 25 generated client/server calls, Rust descriptor message/field/path/stream checks, populated native payloads",
];
const negatives = { InvalidActorBytes: "Data", InvalidWorkerBytes: "Data", InvalidOptionalInteger: "UInt64" };

// Command/pin injection supports offline controls. The CLI always uses real tools.
export function qualify(args, { command = spawnSync, toolchain = pinned, onProgress = () => {} } = {}) {
  if (command === spawnSync && (process.platform !== "linux" || process.arch !== "x64")) throw new Error("Swift installed qualification requires the admitted Linux x86_64 runtime");
  const inputs = Object.fromEntries(["package", "receipt", "authority", "swift-home", "sdk-inventory", "dependencies", "git"].map(name => [name, realpathSync(args[name])]));
  const output = join(realpathSync(dirname(resolve(args.output))), basename(resolve(args.output)));
  try { lstatSync(output); throw new Error("qualification output must be absent"); }
  catch (error) { if (error.code !== "ENOENT") throw error; }
  for (const input of [...Object.values(inputs), realpathSync(join(directory, "../../.."))]) {
    if (within(input, output) || within(output, input)) throw new Error("qualification output overlaps an input");
  }
  onProgress("Verify archive and producer inputs");
  const admitted = readPackage({ ...args, ...inputs });
  const sourceNames = ["qualify.mjs", "package.mjs", "provenance.mjs", "generate.mjs",
    "../toolchains/qualification.json", "../toolchains/toolchain.json", "../../../shared/protoc.json",
    "../../../shared/authority.mjs", "../../../../../scripts/archive-utils.mjs",
    "../templates/qualification/Package.swift", ...["Consumer.swift", "RPCConsumer.swift", ...Object.keys(negatives).map(name => name + ".swift")]
      .map(name => "../tests/fixtures/consumer/" + name)];
  const sourceBytes = new Map(sourceNames.map(name => [name, read(name)]));
  const inventoryBytes = readFileSync(inputs["sdk-inventory"]);
  const gitPin = toolchain.git?.["linux-x64"];
  if (!gitPin || sha256(readFileSync(inputs.git)) !== gitPin.sha256) throw new Error("Git executable differs from admitted pin");
  const env = Object.fromEntries(Object.entries(process.env).filter(([key]) =>
    !/^(?:GIT_|LD_|DYLD_|SWIFT_|SDKROOT$|TOOLCHAINS$|DEVELOPER_DIR$|XDG_|HOME$)/.test(key)));
  Object.assign(env, {
    HOME: join(output, "home"), XDG_CACHE_HOME: join(output, "cache"), XDG_CONFIG_HOME: join(output, "config"),
    PATH: [join(inputs["swift-home"], "usr/bin"), dirname(inputs.git), "/usr/bin", "/bin"].join(":"),
    LD_LIBRARY_PATH: join(inputs["swift-home"], "usr/lib/swift/linux"), LC_ALL: "C",
    GIT_NO_REPLACE_OBJECTS: "1", GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null", GIT_TERMINAL_PROMPT: "0",
    GIT_CONFIG_COUNT: "3", GIT_CONFIG_KEY_0: "protocol.file.allow", GIT_CONFIG_VALUE_0: "always",
    GIT_CONFIG_KEY_1: "protocol.http.allow", GIT_CONFIG_VALUE_1: "never",
    GIT_CONFIG_KEY_2: "protocol.https.allow", GIT_CONFIG_VALUE_2: "never",
  });
  const execute = (exe, argv, cwd) => command(exe, argv, { cwd, env, encoding: "utf8", timeout: 45 * 60_000, maxBuffer: 16 * 1024 * 1024 });
  const git = (root, argv) => {
    const result = execute(inputs.git, ["-C", root, ...argv], directory);
    if (result.error || result.status !== 0) throw new Error("Git source verification failed: " + (result.stderr ?? ""));
    return result.stdout;
  };
  const swift = join(inputs["swift-home"], "usr/bin/swift");
  for (const [exe, expected] of [[inputs.git, gitPin.version]]) {
    const result = execute(exe, ["--version"], directory);
    if (result.error || result.status !== 0 || result.stdout.trim() !== expected) throw new Error("qualification tool version differs");
  }
  onProgress("Verify exact dependency source inputs");
  for (const pin of toolchain.dependencies) {
    const root = realpathSync(join(inputs.dependencies, pin.name));
    if (!within(inputs.dependencies, root)) throw new Error("dependency checkout escapes source root");
    verifySource(root, pin, git, { cloneInput: true });
    if (git(root, ["rev-parse", `refs/tags/${pin.version}^{commit}`]).trim() !== pin.revision) throw new Error("dependency version tag differs");
  }
  onProgress("Verify complete Swift SDK inventory");
  const inventory = verifyRuntime(inputs["swift-home"], inventoryBytes, toolchain);
  const swiftVersion = execute(swift, ["--version"], directory);
  if (swiftVersion.error || swiftVersion.status !== 0 || swiftVersion.stdout.trim() !== inventory.swift_version) throw new Error("qualification tool version differs");
  mkdirSync(output);
  const put = (name, bytes) => {
    const path = join(output, name); mkdirSync(dirname(path), { recursive: true }); writeFileSync(path, bytes, { flag: "wx" }); return path;
  };
  const logs = [];
  const run = (name, exe, argv, cwd = output, negative = false) => {
    onProgress("Run " + name);
    const result = execute(exe, argv, cwd), text = (result.stdout ?? "") + (result.stderr ?? "");
    put(`logs/${name}.log`, text); logs.push(`logs/${name}.log`);
    if (result.error || (negative ? result.status === 0 || result.status === null : result.status !== 0)) throw new Error(`${name} failed: ${text}`);
    return text;
  };
  const installed = join(output, "sdk"), project = join(output, "consumer"), scratch = join(output, "build");
  for (const [name, bytes] of admitted.payload) put("sdk/" + name, bytes);
  put("package.tar.gz", admitted.archiveBytes); put("generation-receipt.json", admitted.receiptBytes);
  const controls = new Map(["Consumer.swift", "RPCConsumer.swift", ...Object.keys(negatives).map(name => name + ".swift")]
    .map(name => [name, sourceBytes.get("../tests/fixtures/consumer/" + name)]));
  const template = sourceBytes.get("../templates/qualification/Package.swift");
  const stagedControls = new Map([["consumer/Package.swift", template],
    ["consumer/Sources/Consumer/main.swift", Buffer.concat([controls.get("Consumer.swift"), Buffer.from("\n"), controls.get("RPCConsumer.swift")])],
    ...Object.keys(negatives).map(name => ["negative/" + name + ".swift", controls.get(name + ".swift")])]);
  for (const [name, bytes] of stagedControls) put(name, bytes);
  mkdirSync(env.HOME, { recursive: true }); mkdirSync(join(output, "feed"));
  const options = ["--package-path", project, "--scratch-path", scratch, "--cache-path", join(output, "cache"),
    "--config-path", join(output, "config"), "--security-path", join(output, "security")];
  for (const pin of toolchain.dependencies) {
    const mirror = join(output, "feed", pin.name);
    run("clone-" + pin.name, inputs.git, ["clone", "--local", "--no-hardlinks", "--quiet", "--branch", pin.version, join(inputs.dependencies, pin.name), mirror]);
    verifySource(realpathSync(mirror), pin, git);
    const owner = pin.name.startsWith("swift-") ? "apple" : "grpc";
    run("mirror-" + pin.name, swift, ["package", ...options, "config", "set-mirror", "--original", `https://github.com/${owner}/${pin.name}.git`, "--mirror", pathToFileURL(mirror).href]);
  }
  stagedControls.set("consumer/.swiftpm/configuration/mirrors.json", readFileSync(join(project, ".swiftpm/configuration/mirrors.json")));
  run("build-positive", swift, ["build", ...options, "--configuration", "release", "--product", "Consumer", "--jobs", "1"]);
  const lockBytes = readFileSync(join(project, "Package.resolved")), lock = JSON.parse(lockBytes);
  if (!Array.isArray(lock.pins) || lock.pins.length !== toolchain.dependencies.length) throw new Error("resolved dependency closure differs");
  const checked = new Set(), compiled = {};
  for (const entry of lock.pins) {
    const pin = toolchain.dependencies.find(pin => pin.name === entry.identity);
    if (!pin || checked.has(pin.name) || entry.state.revision !== pin.revision || entry.state.version !== pin.version) throw new Error("resolved dependency pin differs");
    checked.add(pin.name);
    compiled[pin.name] = verifySource(realpathSync(join(scratch, "checkouts", pin.name)), pin, git);
  }
  const products = join(scratch, "out/Products/Release-linux-x86_64");
  const descriptors = targets.map((source, index) => put(`authority/${index}.bin`, admitted.approved.inputs.get(admitted.approved.descriptors.get(source))));
  const positive = run("positive", join(products, "Consumer"), descriptors, project);
  if (markers.some(marker => !positive.split(/\r?\n/).includes(marker))) throw new Error("positive consumer completion marker missing");
  const compiler = join(inputs["swift-home"], "usr/bin/swiftc");
  run("type-positive", compiler, ["-typecheck", "-swift-version", "6", "-I", products, join(project, "Sources/Consumer/main.swift")], project);
  for (const [name, type] of Object.entries(negatives)) {
    const text = run("negative-" + name, compiler, ["-typecheck", "-swift-version", "6", "-I", products, join(output, "negative", name + ".swift")], project, true);
    if (!text.includes(`${name}.swift:3:`) || !text.includes("cannot assign value of type 'String'") || !text.includes(type)
      || text.includes("no such module") || text.includes("missing required module")) throw new Error("negative control rejected for an unrelated reason: " + name);
  }
  if (JSON.stringify(tree(installed).files) !== JSON.stringify([...admitted.payload.keys()].sort()) || Object.keys(tree(installed).links).length) throw new Error("installed SDK inventory changed");
  for (const [name, bytes] of admitted.payload) readInput(installed, name, sha256(bytes));
  onProgress("Verify SDK and source inputs after native controls");
  const consumerSources = tree(project), negativeSources = tree(join(output, "negative"));
  if (JSON.stringify(consumerSources.files) !== JSON.stringify([".swiftpm/configuration/mirrors.json", "Package.resolved", "Package.swift", "Sources/Consumer/main.swift"]) || Object.keys(consumerSources.links).length
    || JSON.stringify(negativeSources.files) !== JSON.stringify(Object.keys(negatives).map(name => name + ".swift").sort())
    || Object.keys(negativeSources.links).length) throw new Error("staged consumer inventory changed");
  for (const [name, bytes] of stagedControls) if (!readFileSync(join(output, name)).equals(bytes)) throw new Error("staged consumer source changed: " + name);
  if (!readFileSync(join(project, "Package.resolved")).equals(lockBytes)) throw new Error("consumer dependency lock changed");
  verifyRuntime(inputs["swift-home"], inventoryBytes, toolchain);
  for (const pin of toolchain.dependencies) {
    verifySource(realpathSync(join(inputs.dependencies, pin.name)), pin, git, { cloneInput: true });
    verifySource(realpathSync(join(scratch, "checkouts", pin.name)), pin, git);
  }
  for (const [name, bytes] of sourceBytes) if (!read(name).equals(bytes)) throw new Error("qualification source changed during execution: " + name);
  const result = {
    schema: "acyclic.sdk.swift.installed-qualification.v1", scope: "archive-installed-swift-transport-bindings",
    source_revision: admitted.receipt.source_revision, authority_manifest_sha256: sha256(admitted.approved.bytes),
    archive_sha256: sha256(admitted.archiveBytes), generation_receipt_sha256: sha256(admitted.receiptBytes),
    sdk_inventory_sha256: sha256(inventoryBytes), sdk_archive_sha256: toolchain.sdk_archive_sha256,
    swift_version: inventory.swift_version, git_sha256: gitPin.sha256, dependencies: compiled,
    consumer_template_sha256: sha256(template), control_sha256: Object.fromEntries([...controls].map(([name, bytes]) => [name, sha256(bytes)])),
    staged_control_sha256: Object.fromEntries([...stagedControls].map(([name, bytes]) => [name, sha256(bytes)])),
    qualifier_sha256: sha256(sourceBytes.get("qualify.mjs")),
    package_admission_sha256: sha256(sourceBytes.get("package.mjs")), provenance_reader_sha256: sha256(sourceBytes.get("provenance.mjs")),
    source_sha256: Object.fromEntries([...sourceBytes].map(([name, bytes]) => [name, sha256(bytes)])),
    toolchain_sha256: sha256(JSON.stringify(toolchain)), installed_payload_sha256: admitted.receipt.output_sha256,
    executable_sha256: sha256(readFileSync(join(products, "Consumer"))), lock_sha256: sha256(lockBytes),
    negative_type_controls: Object.keys(negatives), native_in_process_rpc_methods: 25,
    network_transport_qualified: false, rust_backed_rpc_qualified: false, embedded_runtime_qualified: false,
    logs_sha256: Object.fromEntries(logs.map(name => [name, sha256(readFileSync(join(output, name)))])),
  };
  put("qualification.json", JSON.stringify(result, null, 2) + "\n");
  return result;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const options = Object.fromEntries(["package", "sha256", "receipt", "authority", "swift-home", "sdk-inventory", "dependencies", "git", "output"].map(name => [name, { type: "string" }]));
  const { values } = parseArgs({ options });
  if (Object.keys(options).some(name => !values[name])) throw new Error("all installed Swift qualification arguments are required");
  console.log(JSON.stringify(qualify(values, { onProgress: message => console.error(message) }), null, 2));
}
