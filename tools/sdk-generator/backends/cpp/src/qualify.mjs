import { spawnSync } from "node:child_process";
import { lstatSync, mkdirSync, readFileSync, realpathSync, writeFileSync } from "node:fs";
import { basename, delimiter, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { sha256, within } from "../../../shared/authority.mjs";
import { readPackage, targets } from "./package.mjs";
import { files, verifyCompiledSources, verifyHost, verifyPrefixes, verifyRuntime } from "./provenance.mjs";

const directory = dirname(fileURLToPath(import.meta.url));
const read = name => readFileSync(join(directory, name));
const pinned = JSON.parse(read("../toolchains/qualification.json"));
const negatives = { InvalidActorBytes: ["C2665", "set_code_sha256"], InvalidWorkerBytes: ["C2665", "set_javascript_module"], InvalidOptionalInteger: ["C2664", "set_if_tail"] };
const controls = ["CMakeLists.txt", "message_consumer.cc", "rpc_consumer.cc", "PositiveTypes.cc", ...Object.keys(negatives).map(name => name + ".cc")];
const markers = ["PASS C++ complete file descriptors, 25 RPC message pairs, bytes, unsigned bounds, optional zero and oneof controls",
  "PASS C++ 25 generated client/server loopback calls, Rust file descriptors, populated payloads and actual method paths/stream types"];

// Native command and pin injection is limited to offline runner controls.
export function qualify(args, { command = spawnSync, toolchain = pinned, onProgress = () => {} } = {}) {
  if (command === spawnSync && (process.platform !== "win32" || process.arch !== "x64")) throw new Error("C++ installed qualification requires Windows x64");
  const inputs = Object.fromEntries(["package", "receipt", "authority", "runtime-root", "runtime-inventory", "host-inventory"].map(name => [name, realpathSync(args[name])]));
  const output = join(realpathSync(dirname(resolve(args.output))), basename(resolve(args.output)));
  try { lstatSync(output); throw new Error("qualification output must be absent"); }
  catch (error) { if (error.code !== "ENOENT") throw error; }
  for (const input of [...Object.values(inputs), realpathSync(join(directory, "../../.."))]) {
    if (within(input, output) || within(output, input)) throw new Error("qualification output overlaps an input");
  }
  onProgress("Verify archive and producer evidence");
  const admitted = readPackage({ ...args, ...inputs });
  const sourceNames = ["qualify.mjs", "package.mjs", "provenance.mjs", "generate.mjs", "../toolchains/qualification.json", "../toolchains/toolchain.json", "../toolchains/build-runtime.cmake",
    "../../../shared/authority.mjs", "../../../../../scripts/archive-utils.mjs", "../templates/package/CMakeLists.txt", "../templates/package/AcyclicTransportConfig.cmake.in", ...controls.map(name => "../tests/fixtures/consumer/" + name)];
  const sourceBytes = new Map(sourceNames.map(name => [name, read(name)]));
  const hostBytes = readFileSync(inputs["host-inventory"]), runtimeBytes = readFileSync(inputs["runtime-inventory"]);
  onProgress("Verify complete admitted compiler/SDK and runtime inventories");
  const host = verifyHost(hostBytes, toolchain), runtime = verifyRuntime(inputs["runtime-root"], runtimeBytes, toolchain);
  for (const root of host.roots) if (within(root.root, output) || within(output, root.root)) throw new Error("qualification output overlaps host SDK");
  const tools = Object.fromEntries(Object.entries(host.tools).map(([name, pin]) => [name, pin.path]));
  const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !/^(?:CMAKE_|VCPKG_|CONAN_|VS|VC|WindowsSdk|UCRT|CL$|_CL_$|LINK$|_LINK_$|CC$|CXX$|CFLAGS$|CXXFLAGS$|CPPFLAGS$|LDFLAGS$|INCLUDE$|LIB$|LIBPATH$|PATH$|COMSPEC$|SystemRoot$|HOME$|XDG_|USERPROFILE$|APPDATA$|LOCALAPPDATA$|TEMP$|TMP$)/i.test(key)));
  const present = paths => paths.filter(path => { try { return lstatSync(path).isDirectory(); } catch (error) { if (error.code !== "ENOENT") throw error; return false; } });
  Object.assign(env, { SystemRoot: toolchain.system_root, COMSPEC: join(toolchain.system_root, "System32/cmd.exe"), VSLANG: "1033",
    INCLUDE: present(host.environment.include).join(";"), LIB: present(host.environment.lib).join(";"), LIBPATH: present(host.environment.lib).join(";"),
    PATH: [...new Set([...Object.values(tools).map(dirname), join(toolchain.system_root, "System32"), toolchain.system_root])].join(delimiter),
    USERPROFILE: join(output, "home"), APPDATA: join(output, "home/AppData/Roaming"), LOCALAPPDATA: join(output, "home/AppData/Local"), TEMP: join(output, "tmp"), TMP: join(output, "tmp") });
  const execute = (exe, argv, cwd) => command(exe, argv, { cwd, env, encoding: "utf8", timeout: 20 * 60_000, maxBuffer: 16 * 1024 * 1024, windowsHide: true });
  mkdirSync(output);
  const put = (name, bytes) => { const target = join(output, name); mkdirSync(dirname(target), { recursive: true }); writeFileSync(target, bytes, { flag: "wx" }); return target; };
  for (const name of ["home", "home/AppData/Roaming", "home/AppData/Local", "tmp"]) mkdirSync(join(output, name), { recursive: true });
  put("package.tar.gz", admitted.archiveBytes); put("generation-receipt.json", admitted.receiptBytes); put("host-inventory.json", hostBytes); put("runtime-inventory.json", runtimeBytes);
  for (const [name, bytes] of admitted.payload) put("sdk-source/" + name, bytes);
  for (const [name, bytes] of admitted.approved.inputs) put("authority/" + name, bytes); put("authority/rust-authority.json", admitted.approved.bytes);
  for (const name of Object.keys(runtime.files)) put("runtime/" + name, readFileSync(join(inputs["runtime-root"], name)));
  for (const name of controls) put("consumer/" + name, sourceBytes.get("../tests/fixtures/consumer/" + name));
  const logs = [];
  const run = (name, exe, argv, negative) => {
    onProgress("Run " + name);
    const result = execute(exe, argv, output), text = (result.stdout ?? "") + (result.stderr ?? "");
    put("logs/" + name + ".log", text); logs.push("logs/" + name + ".log");
    if (result.error || (negative ? result.status === 0 || result.status === null : result.status !== 0)) throw new Error(name + " failed: " + text);
    if (negative && (!text.includes("error " + negative[0] + ":") || !text.includes(negative[1]) || /fatal error/i.test(text))) throw new Error("unintended negative compile failure: " + name);
    return text;
  };
  if (run("cmake-version", tools["cmake.exe"], ["--version"]).split(/\r?\n/)[0].trim() !== toolchain.versions.cmake
    || run("ninja-version", tools["ninja.exe"], ["--version"]).trim() !== toolchain.versions.ninja) throw new Error("build tool version differs");
  const compiler = execute(tools["cl.exe"], ["/Bv"], output), compilerText = (compiler.stdout ?? "") + (compiler.stderr ?? "");
  put("logs/compiler-version.log", compilerText); logs.push("logs/compiler-version.log");
  if (compiler.error || ![0, 2].includes(compiler.status) || !compilerText.includes("Version " + toolchain.versions.cl)) throw new Error("MSVC compiler version differs");
  const runtimeRoot = join(output, "runtime"), sdk = join(output, "sdk-source"), sdkBuild = join(output, "sdk-build"), installed = join(output, "sdk-install"), consumer = join(output, "consumer"), build = join(output, "consumer-build");
  const cmake = tools["cmake.exe"], common = ["-G", "Ninja", "-DCMAKE_BUILD_TYPE=Release", "-DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreadedDLL", "-DCMAKE_EXPORT_COMPILE_COMMANDS=ON", "-DCMAKE_FIND_USE_PACKAGE_REGISTRY=OFF", "-DCMAKE_FIND_USE_SYSTEM_PACKAGE_REGISTRY=OFF",
    ...[["CMAKE_CXX_COMPILER", "cl.exe"], ["CMAKE_LINKER", "link.exe"], ["CMAKE_AR", "lib.exe"], ["CMAKE_RC_COMPILER", "rc.exe"], ["CMAKE_MT", "mt.exe"], ["CMAKE_MAKE_PROGRAM", "ninja.exe"]].map(([key, name]) => "-D" + key + "=" + tools[name].replaceAll("\\", "/"))];
  run("configure-sdk", cmake, ["-S", sdk, "-B", sdkBuild, ...common, "-DCMAKE_PREFIX_PATH=" + runtimeRoot.replaceAll("\\", "/"), "-DCMAKE_INSTALL_PREFIX=" + installed.replaceAll("\\", "/")]);
  run("build-sdk", cmake, ["--build", sdkBuild, "--parallel", "1"]);
  run("install-sdk", cmake, ["--install", sdkBuild]);
  run("configure-consumer", cmake, ["-S", consumer, "-B", build, ...common, "-DCMAKE_PREFIX_PATH=" + (installed + ";" + runtimeRoot).replaceAll("\\", "/")]);
  run("build-consumer", cmake, ["--build", build, "--parallel", "1", "--target", "RpcConsumer", "PositiveTypes"]);
  const descriptors = targets.map(name => join(output, "authority", admitted.approved.manifest.families.find(family => family.source === name).descriptor));
  const positive = run("installed-native-consumer", join(build, "RpcConsumer.exe"), descriptors);
  if (markers.some(marker => !positive.split(/\r?\n/).includes(marker))) throw new Error("installed consumer completion marker missing");
  for (const [name, failure] of Object.entries(negatives)) run("negative-" + name, cmake, ["--build", build, "--parallel", "1", "--target", name], failure);
  onProgress("Verify compiled source inventories and post-execution provenance");
  verifyCompiledSources(sdkBuild, [...admitted.payload.keys()].filter(name => name.startsWith("src/")).map(name => join(sdk, name)));
  verifyCompiledSources(build, ["rpc_consumer.cc", "PositiveTypes.cc", ...Object.keys(negatives).map(name => name + ".cc")].map(name => join(consumer, name)));
  verifyPrefixes(sdkBuild, runtimeRoot); verifyPrefixes(build, runtimeRoot, installed);
  if (JSON.stringify(files(sdk)) !== JSON.stringify([...admitted.payload.keys()].sort()) || JSON.stringify(files(consumer)) !== JSON.stringify(controls.toSorted())) throw new Error("compiled source file inventory differs");
  for (const [name, bytes] of admitted.payload) {
    if (!readFileSync(join(sdk, name)).equals(bytes)) throw new Error("SDK source changed");
    const destination = name.startsWith("include/") ? join(installed, name) : ["LICENSE", "NOTICE"].includes(name) || name.startsWith("authority/") ? join(installed, "share/acyclic-sdk-transport", name) : undefined;
    if (destination && !readFileSync(destination).equals(bytes)) throw new Error("installed SDK payload differs");
  }
  for (const name of controls) if (!readFileSync(join(consumer, name)).equals(sourceBytes.get("../tests/fixtures/consumer/" + name))) throw new Error("consumer source changed");
  for (const [name, bytes] of sourceBytes) if (!read(name).equals(bytes)) throw new Error("qualifier source changed");
  if (!readFileSync(inputs["host-inventory"]).equals(hostBytes) || !readFileSync(inputs["runtime-inventory"]).equals(runtimeBytes)) throw new Error("external inventory changed");
  verifyHost(hostBytes, toolchain); verifyRuntime(inputs["runtime-root"], runtimeBytes, toolchain); verifyRuntime(runtimeRoot, runtimeBytes, toolchain);
  const after = readPackage({ ...args, ...inputs });
  if (!after.archiveBytes.equals(admitted.archiveBytes) || !after.receiptBytes.equals(admitted.receiptBytes)) throw new Error("archive evidence changed");
  const receipt = { schema: "acyclic.sdk.cpp-installed-qualification.v1", authority: "rust", target: "cpp", source_revision: admitted.receipt.source_revision,
    authority_manifest_sha256: admitted.receipt.authority_manifest_sha256, archive_sha256: sha256(admitted.archiveBytes), generation_receipt_sha256: sha256(admitted.receiptBytes),
    host_inventory_sha256: sha256(hostBytes), runtime_inventory_sha256: sha256(runtimeBytes), host_sdk_files: toolchain.host_inventory_files, runtime_files: toolchain.runtime_files,
    qualifier_source_sha256: Object.fromEntries([...sourceBytes].map(([name, bytes]) => [name, sha256(bytes)])), payload_sha256: admitted.receipt.output_sha256,
    compiled_sdk_inventory_sha256: sha256(readFileSync(join(sdkBuild, "compile_commands.json"))), compiled_consumer_inventory_sha256: sha256(readFileSync(join(build, "compile_commands.json"))),
    installed_library_sha256: sha256(readFileSync(join(installed, "lib/acyclic_sdk_transport.lib"))), consumer_executable_sha256: sha256(readFileSync(join(build, "RpcConsumer.exe"))),
    log_sha256: Object.fromEntries(logs.map(name => [name, sha256(readFileSync(join(output, name)))])),
    populated_rpc_message_pairs: 25, native_rpc_method_calls: 25, independent_negative_compiles: 3,
    rust_backed_rpc_qualified: false, embedded_runtime_qualified: false };
  put("qualification.json", JSON.stringify(receipt, null, 2) + "\n"); return receipt;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const options = Object.fromEntries(["package", "sha256", "receipt", "authority", "runtime-root", "runtime-inventory", "host-inventory", "output"].map(name => [name, { type: "string" }]));
  const { values } = parseArgs({ options }); if (Object.keys(options).some(name => !values[name])) throw new Error("all installed qualification arguments are required");
  const receipt = qualify(values, { onProgress: message => process.stderr.write(message + "\n") }); process.stdout.write(JSON.stringify(receipt, null, 2) + "\n");
}
