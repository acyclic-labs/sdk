import { spawnSync } from "node:child_process";
import { lstatSync, mkdirSync, readFileSync, readdirSync, realpathSync, writeFileSync } from "node:fs";
import { basename, dirname, join, posix, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { sha256, within } from "../../../shared/authority.mjs";
import { packageTemplates, pins, producerSources } from "./generate.mjs";
import { expectedModules, readPackage } from "./package.mjs";
import { verifyRuntime, verifyTools, verifyTree } from "./inventory.mjs";

const directory = dirname(fileURLToPath(import.meta.url));
const read = name => readFileSync(join(directory, name));
const qualificationPins = JSON.parse(read("../toolchains/qualification.json"));
export const targets = ["actors/v1/actors.proto", "workers/v1/workers.proto", "stream/v1/stream.proto"];
export const controls = ["rpc_route_control.erl", "compare_descriptor.erl", "installed_provenance.erl"];
const fixtures = controls.map(name => "../tests/fixtures/consumer/" + name);
const sourceNames = [...new Set(["qualify.mjs", "package.mjs", "inventory.mjs", ...producerSources,
  "../../../../../scripts/archive-utils.mjs", "../toolchains/qualification.json", "../toolchains/dependency-files.json", ...fixtures,
  ...[...packageTemplates.values()].map(name => "../templates/package/" + name)])];

export function verifyDependencies(root, bytes, pin = qualificationPins) {
  if (sha256(bytes) !== pin.dependency_inventory_sha256) throw new Error("dependency inventory differs from pin");
  const index = JSON.parse(bytes);
  if (index.schema !== "acyclic.sdk.erlang-runtime-dependencies.v1" || index.upstream_lock_sha256 !== pin.upstream_lock_sha256
    || JSON.stringify(index.packages) !== JSON.stringify(pin.packages) || Object.keys(index.files_sha256 ?? {}).length !== pin.dependency_files) throw new Error("dependency metadata differs");
  verifyTree(root, index);
  return index;
}

function files(root, prefix = "") {
  return readdirSync(join(root, prefix), { withFileTypes: true }).flatMap(entry => {
    const name = posix.join(prefix, entry.name);
    if (entry.isSymbolicLink()) throw new Error("installed tree contains a link");
    if (entry.isDirectory()) return files(root, name);
    if (!entry.isFile()) throw new Error("installed tree contains a non-file");
    return [name];
  }).sort();
}

// Injected commands support offline lifecycle controls; CLI qualification is native.
export function qualify(args, { command = spawnSync, onProgress = () => {}, toolchain = pins,
  runtimePin = toolchain.runtime["linux-x64"], dependencyPin = qualificationPins,
  toolIndexBytes = read("../toolchains/generator-files.json"), dependencyIndexBytes = read("../toolchains/dependency-files.json") } = {}) {
  if (command === spawnSync && (process.platform !== "linux" || process.arch !== "x64")) throw new Error("installed Erlang qualification requires Linux x86_64");
  const admitted = readPackage(args);
  if (targets.length !== admitted.approved.manifest.families.length || targets.some(name => !admitted.approved.descriptors.has(name))) throw new Error("installed controls require Actors v1, Workers v1 and Stream v1 exports");
  const inputs = Object.fromEntries(["package", "receipt", "authority", "runtime-root", "runtime-inventory", "tool-home", "dependencies"].map(name => [name, realpathSync(args[name])]));
  const output = join(realpathSync(dirname(resolve(args.output))), basename(resolve(args.output)));
  try { lstatSync(output); throw new Error("qualification output must be absent"); }
  catch (error) { if (error.code !== "ENOENT") throw error; }
  for (const input of [...Object.values(inputs), dirname(directory)]) {
    if (within(input, output) || within(output, input)) throw new Error("qualification output overlaps protected input");
  }
  const runtimeBytes = readFileSync(inputs["runtime-inventory"]);
  const toolBytes = toolIndexBytes, dependencyBytes = dependencyIndexBytes;
  const sourceBytes = new Map(sourceNames.map(name => [name, read(name)]));
  const guard = () => {
    if (!readFileSync(inputs["runtime-inventory"]).equals(runtimeBytes) || !readFileSync(inputs.receipt).equals(admitted.receiptBytes)) throw new Error("qualification input bytes changed");
    verifyRuntime(inputs["runtime-root"], runtimeBytes, runtimePin);
    verifyTools(inputs["tool-home"], toolBytes, toolchain);
    verifyDependencies(inputs.dependencies, dependencyBytes, dependencyPin);
    for (const [name, bytes] of sourceBytes) if (!read(name).equals(bytes)) throw new Error("qualifier source changed");
    readPackage(args);
  };
  onProgress("Admit package, full OTP runtime, generator tools and locked native dependencies"); guard();
  mkdirSync(output);
  const put = (name, bytes) => { const target = join(output, name); mkdirSync(dirname(target), { recursive: true }); writeFileSync(target, bytes, { flag: "wx" }); return target; };
  const sdk = join(output, "libs/acyclic_sdk_transport-0.1.0"), libs = join(output, "libs"), consumer = join(output, "consumer");
  const dependencies = JSON.parse(dependencyBytes);
  for (const [name, bytes] of admitted.payload) put("libs/acyclic_sdk_transport-0.1.0/" + name, bytes);
  for (const [name, digest] of Object.entries(dependencies.files_sha256)) {
    const bytes = readFileSync(join(inputs.dependencies, name));
    if (sha256(bytes) !== digest) throw new Error("dependency changed while staging");
    put("libs/" + name, bytes);
  }
  const dependencyRoots = Object.values(dependencies.packages).map(value => value.directory);
  const privateDependencyGuard = () => {
    const entries = readdirSync(libs, { withFileTypes: true });
    if (entries.some(entry => !entry.isDirectory() || entry.isSymbolicLink())
      || JSON.stringify(entries.map(entry => entry.name).sort()) !== JSON.stringify([...dependencyRoots, "acyclic_sdk_transport-0.1.0"].sort())) throw new Error("private application inventory differs");
    verifyTree(libs, dependencies, dependencyRoots);
  };
  privateDependencyGuard();
  for (const name of controls) put("consumer/" + name, sourceBytes.get("../tests/fixtures/consumer/" + name));
  mkdirSync(join(sdk, "ebin")); mkdirSync(join(consumer, "ebin")); mkdirSync(join(output, "home"));
  const otp = join(inputs["runtime-root"], runtimePin.roots[0]), gpb = join(inputs["tool-home"], "gpb-4.21.7");
  const env = { HOME: join(output, "home"), PATH: [join(otp, "bin"), "/usr/bin", "/bin"].join(":"),
    ERL_FLAGS: "+S 1:1 +fnu", ERL_LIBS: libs, LC_ALL: "C.UTF-8" };
  const logNames = [];
  const run = (name, executable, argv) => {
    onProgress("Run " + name);
    const result = command(executable, argv, { cwd: consumer, env, encoding: "utf8", timeout: 180_000, maxBuffer: 16 * 1024 * 1024 });
    const text = (result.stdout ?? "") + (result.stderr ?? "");
    const log = "logs/" + name + ".log"; put(log, text); logNames.push(log);
    if (result.error || result.status !== 0) throw new Error("Erlang qualification command failed: " + name + "\n" + text);
    return text;
  };
  const modules = expectedModules(admitted.approved).map(name => basename(name, ".erl"));
  run("compile-installed-package", join(otp, "bin/erlc"), ["-Werror", "+debug_info", "-o", join(sdk, "ebin"), ...modules.map(name => join(sdk, "src", name + ".erl"))]);
  put("libs/acyclic_sdk_transport-0.1.0/ebin/acyclic_sdk_transport.app", admitted.payload.get("src/acyclic_sdk_transport.app.src"));
  if (JSON.stringify(readdirSync(join(sdk, "ebin")).sort()) !== JSON.stringify([...modules.map(name => name + ".beam"), "acyclic_sdk_transport.app"].sort())) throw new Error("compiled SDK inventory differs");
  const compiled = Object.fromEntries(readdirSync(join(sdk, "ebin")).sort().map(name => [name, sha256(readFileSync(join(sdk, "ebin", name)))]));
  run("compile-consumer-controls", join(otp, "bin/erlc"), ["-Werror", "+debug_info", "-I", join(gpb, "descr_src"), "-I", join(gpb, "include"), "-o", join(consumer, "ebin"), ...controls.map(name => join(consumer, name))]);
  const expectedConsumerBeams = controls.map(name => name.slice(0, -4) + ".beam").sort();
  if (JSON.stringify(readdirSync(join(consumer, "ebin")).sort()) !== JSON.stringify(expectedConsumerBeams)) throw new Error("compiled consumer inventory differs");
  const consumerBeams = Object.fromEntries(expectedConsumerBeams.map(name => [name, sha256(readFileSync(join(consumer, "ebin", name)))]));
  const literal = value => JSON.stringify(value);
  const provenance = `ok=installed_provenance:run(${[sdk, libs, inputs["tool-home"], otp, consumer].map(literal).join(",")})`;
  const expression = `{ok,_}=application:ensure_all_started(acyclic_sdk_transport),${provenance},ok=compare_descriptor:run(init:get_plain_arguments()),ok=rpc_route_control:run(),${provenance},halt(0).`;
  const native = run("installed-native-consumer", join(otp, "bin/erl"), ["-pa", join(gpb, "ebin"), "-pa", join(sdk, "ebin"), "-pa", join(consumer, "ebin"), "-noshell", "-noinput", "-eval", expression, "-extra",
    ...targets.map(name => join(sdk, "authority", admitted.approved.descriptors.get(name)))]);
  for (const marker of ["PASS archive-installed Erlang clients: 25 populated native TCP RPC pairs", "actors_pb differing modeled file descriptor fields: []", "workers_pb differing modeled file descriptor fields: []", "stream_pb differing modeled file descriptor fields: []"]) {
    if (!native.includes(marker)) throw new Error("installed consumer completion marker missing: " + marker);
  }
  if (native.split("PASS installed SDK and loaded-module provenance").length !== 3) throw new Error("loaded-module provenance controls incomplete");
  onProgress("Verify post-execution package, dependency, compiler and consumer bytes");
  guard(); privateDependencyGuard();
  for (const [name, bytes] of admitted.payload) if (!readFileSync(join(sdk, name)).equals(bytes)) throw new Error("installed package payload changed");
  for (const [name, digest] of Object.entries(compiled)) if (sha256(readFileSync(join(sdk, "ebin", name))) !== digest) throw new Error("compiled SDK changed");
  if (JSON.stringify(files(sdk)) !== JSON.stringify([...admitted.payload.keys(), ...Object.keys(compiled).map(name => "ebin/" + name)].sort())) throw new Error("installed SDK tree differs");
  for (const name of controls) if (!readFileSync(join(consumer, name)).equals(sourceBytes.get("../tests/fixtures/consumer/" + name))) throw new Error("staged consumer source changed");
  for (const [name, digest] of Object.entries(consumerBeams)) if (sha256(readFileSync(join(consumer, "ebin", name))) !== digest) throw new Error("compiled consumer changed");
  const consumerFiles = [...controls, ...expectedConsumerBeams.map(name => "ebin/" + name), "actors_pb_route_fixture.erl", "workers_pb_route_fixture.erl", "stream_pb_route_fixture.erl"].sort();
  if (JSON.stringify(files(consumer)) !== JSON.stringify(consumerFiles)) throw new Error("consumer tree differs");
  const receipt = { schema: "acyclic.sdk.erlang-installed-qualification.v1", authority: "rust", target: "erlang",
    source_revision: admitted.receipt.source_revision, authority_manifest_sha256: sha256(admitted.approved.bytes), archive_sha256: sha256(admitted.archiveBytes),
    generation_receipt_sha256: sha256(admitted.receiptBytes), runtime_inventory_sha256: sha256(runtimeBytes),
    generator_inventory_sha256: sha256(toolBytes), dependency_inventory_sha256: sha256(dependencyBytes),
    qualifier_source_sha256: Object.fromEntries([...sourceBytes].map(([name, bytes]) => [name, sha256(bytes)])),
    payload_sha256: Object.fromEntries([...admitted.payload].map(([name, bytes]) => [name, sha256(bytes)])),
    compiled_sdk_sha256: compiled, compiled_consumer_sha256: consumerBeams,
    consumer_source_sha256: Object.fromEntries(files(consumer).filter(name => name.endsWith(".erl")).map(name => [name, sha256(readFileSync(join(consumer, name)))])),
    log_sha256: Object.fromEntries(logNames.map(name => [name, sha256(readFileSync(join(output, name)))])),
    native_rpc_calls_executed: 25, unary_rpc_calls: 22, server_streaming_rpc_calls: 3, populated_rpc_message_pairs: 25, runtime_type_rejections: 3,
    compiled_sdk_provenance: true, modeled_file_descriptor_semantics: true,
    descriptor_normalization: ["source locations omitted", "source filename basename", "declared protobuf defaults", "default JSON names", "message and enum declaration order", "single-field proto3-optional synthetic oneof names"],
    unknown_descriptor_extensions_qualified: false, rust_backed_rpc_qualified: false, embedded_runtime_qualified: false };
  put("qualification.json", Buffer.from(JSON.stringify(receipt, null, 2) + "\n"));
  return receipt;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const options = Object.fromEntries(["package", "sha256", "receipt", "authority", "runtime-root", "runtime-inventory", "tool-home", "dependencies", "output"].map(name => [name, { type: "string" }]));
  const { values } = parseArgs({ options });
  if (Object.keys(options).some(name => !values[name])) throw new Error("all installed qualification arguments are required");
  console.log(JSON.stringify(qualify(values), null, 2));
}
