import { spawnSync } from "node:child_process";
import { chmodSync, lstatSync, mkdirSync, readFileSync, realpathSync, writeFileSync } from "node:fs";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { sha256, within } from "../../../shared/authority.mjs";
import { pins as producerPins, readPackage, targets } from "./package.mjs";
import { verifyRuntime } from "./runtime.mjs";
import { files, readDependencies, readTools, verifyInstalledDependencies, verifyLock } from "./provenance.mjs";

const directory = dirname(fileURLToPath(import.meta.url));
const read = name => readFileSync(join(directory, name));
const pinned = JSON.parse(read("../toolchains/qualification.json"));
const marker = "PASS Elixir Rust message/enum/service descriptors, 25 populated RPC message pairs, bytes, unsigned bounds, optional presence, oneofs and 3 runtime type rejections";
const provenanceMarker = "PASS Elixir exact compiled SDK module inventory, source paths and loaded BEAM paths";

// Injection supports offline runner controls; the CLI uses admitted native tools.
export function qualify(args, { command = spawnSync, toolchain = pinned,
  runtimePin = producerPins.runtime["linux-x64"], lockBytes = read("../templates/qualification/mix.lock"), onProgress = () => {} } = {}) {
  if (command === spawnSync && (process.platform !== "linux" || process.arch !== "x64")) throw new Error("installed Elixir qualification requires Linux x86_64");
  const inputs = Object.fromEntries(["package", "receipt", "authority", "runtime-root", "runtime-inventory", "tool-home", "tool-inventory", "cache"].map(name => [name, realpathSync(args[name])]));
  const output = join(realpathSync(dirname(resolve(args.output))), basename(resolve(args.output)));
  try { lstatSync(output); throw new Error("qualification output must be absent"); }
  catch (error) { if (error.code !== "ENOENT") throw error; }
  for (const input of [...Object.values(inputs), realpathSync(join(directory, "../../.."))]) {
    if (within(input, output) || within(output, input)) throw new Error("qualification output overlaps an input");
  }
  onProgress("Verify archive and producer evidence");
  const admitted = readPackage({ ...args, ...inputs });
  const sourceNames = ["qualify.mjs", "package.mjs", "provenance.mjs", "runtime.mjs", "generate.mjs",
    "../toolchains/qualification.json", "../toolchains/toolchain.json", "../../../shared/protoc.json",
    "../../../shared/authority.mjs", "../../../../../scripts/archive-utils.mjs",
    "../templates/qualification/mix.exs", "../templates/qualification/mix.lock",
    "../tests/fixtures/consumer/Consumer.exs", "../tests/fixtures/consumer/Provenance.exs"];
  const sourceBytes = new Map(sourceNames.map(name => [name, read(name)]));
  const indexBytes = readFileSync(inputs["runtime-inventory"]), toolIndexBytes = readFileSync(inputs["tool-inventory"]);
  onProgress("Verify complete OTP/Elixir runtime and qualification tools");
  verifyRuntime(inputs["runtime-root"], indexBytes, runtimePin);
  const tools = readTools(inputs["tool-home"], toolIndexBytes, toolchain);
  const dependencies = readDependencies(inputs.cache, toolchain);
  const lock = lockBytes;
  verifyLock(lock, dependencies, toolchain);
  const home = join(output, "home");
  const env = Object.fromEntries(Object.entries(process.env).filter(([key]) =>
    !/^(?:ERL_|ELIXIR_|ESCRIPT_|MIX_|HEX_|GIT_|LD_|DYLD_|XDG_|HOME$|PATH$|ROOTDIR$|BINDIR$|EMU$|PROGNAME$)/.test(key)));
  const elixirBin = join(inputs["runtime-root"], runtimePin.roots[1], "bin"), otpBin = join(inputs["runtime-root"], runtimePin.roots[0], "bin");
  Object.assign(env, { HOME: home, MIX_HOME: join(home, ".mix"), HEX_HOME: join(home, ".hex"),
    MIX_ENV: "prod", MIX_BUILD_PATH: join(output, "build"), MIX_DEPS_PATH: join(output, "consumer/deps"),
    MIX_REBAR3: join(home, ".mix/elixir/1-20-otp-29/rebar3"),
    HEX_OFFLINE: "1",
    PATH: [elixirBin, otpBin, "/usr/bin", "/bin"].join(":"),
    ERL_FLAGS: "+S 1:1", ELIXIR_ERL_OPTIONS: "+fnu", LC_ALL: "C.UTF-8" });
  const execute = (exe, argv, cwd) => command(exe, argv, { cwd, env, encoding: "utf8", timeout: 20 * 60_000, maxBuffer: 16 * 1024 * 1024 });
  mkdirSync(output);
  const put = (name, bytes) => { const target = join(output, name); mkdirSync(dirname(target), { recursive: true }); writeFileSync(target, bytes, { flag: "wx" }); return target; };
  for (const [name, bytes] of admitted.payload) put("sdk/" + name, bytes);
  put("package.tar.gz", admitted.archiveBytes); put("generation-receipt.json", admitted.receiptBytes);
  put("runtime-inventory.json", indexBytes); put("tool-inventory.json", toolIndexBytes);
  for (const [name, bytes] of tools) put("home/" + name, bytes);
  chmodSync(env.MIX_REBAR3, 0o755);
  for (const dependency of dependencies.values()) put("home/.hex/packages/hexpm/" + dependency.filename, dependency.bytes);
  put("consumer/mix.exs", sourceBytes.get("../templates/qualification/mix.exs")); put("consumer/mix.lock", lock);
  for (const name of ["Consumer.exs", "Provenance.exs"]) put("consumer/" + name, sourceBytes.get("../tests/fixtures/consumer/" + name));
  const logs = [];
  const run = (name, executable, argv) => {
    onProgress("Run " + name);
    const result = execute(executable, argv, join(output, "consumer"));
    const text = (result.stdout ?? "") + (result.stderr ?? "");
    put("logs/" + name + ".log", text); logs.push("logs/" + name + ".log");
    if (result.error || result.status !== 0) throw new Error(name + " failed: " + text);
    return result.stdout ?? "";
  };
  const version = run("runtime-version", join(elixirBin, "elixir"), ["-e", 'IO.puts(System.version()); IO.puts(System.otp_release())']);
  if (version.trim() !== `${runtimePin.elixir_version}\n${runtimePin.otp_release}`) throw new Error("qualification runtime version differs");
  const mix = join(elixirBin, "mix");
  const hex = run("hex-version", mix, ["hex.info"]);
  if (!hex.split(/\r?\n/).some(line => line.trim().replace(/\s+/g, " ") === "Hex: " + toolchain.hex_version)) throw new Error("qualification Hex version differs");
  run("resolve-offline-dependencies", mix, ["deps.get", "--only", "prod", "--check-locked"]);
  if (!readFileSync(join(output, "consumer/mix.lock")).equals(lock)) throw new Error("consumer lock changed");
  onProgress("Verify dependency source bytes before compilation");
  verifyInstalledDependencies(join(output, "consumer/deps"), dependencies);
  run("compile-installed-package", mix, ["compile"]);
  const provenance = run("loaded-sdk-provenance", mix, ["run", "--no-compile", "--no-start", "Provenance.exs", join(output, "sdk"), join(output, "build/lib/acyclic_sdk_transport/ebin")]);
  if (!provenance.split(/\r?\n/).includes(provenanceMarker)) throw new Error("loaded SDK provenance marker missing");
  const descriptors = targets.map(name => join(output, "sdk/authority", admitted.approved.manifest.families.find(family => family.source === name).descriptor));
  const positive = run("installed-consumer", mix, ["run", "--no-compile", "--no-start", "Consumer.exs", ...descriptors]);
  if (!positive.split(/\r?\n/).includes(marker)) throw new Error("installed consumer completion marker missing");
  onProgress("Verify post-execution source and runtime provenance");
  const installedDependencies = verifyInstalledDependencies(join(output, "consumer/deps"), dependencies);
  verifyLock(readFileSync(join(output, "consumer/mix.lock")), dependencies, toolchain);
  if (JSON.stringify(files(join(output, "sdk"))) !== JSON.stringify([...admitted.payload.keys()].sort())) throw new Error("installed SDK file inventory differs");
  for (const [name, bytes] of admitted.payload) if (!readFileSync(join(output, "sdk", name)).equals(bytes)) throw new Error("installed SDK payload changed");
  for (const name of ["mix.exs", "Consumer.exs", "Provenance.exs"]) if (!readFileSync(join(output, "consumer", name)).equals(sourceBytes.get(name === "mix.exs" ? "../templates/qualification/mix.exs" : "../tests/fixtures/consumer/" + name))) throw new Error("consumer source changed");
  const installedHex = files(join(home, ".mix/archives")).map(name => ".mix/archives/" + name);
  if (JSON.stringify(installedHex) !== JSON.stringify([...tools.keys()].filter(name => name.startsWith(".mix/archives/")).sort())) throw new Error("installed Hex code inventory changed");
  for (const [name, bytes] of tools) if (name !== ".hex/cache.ets" && !readFileSync(join(home, name)).equals(bytes)) throw new Error("installed qualification tool changed");
  for (const [name, bytes] of sourceBytes) if (!read(name).equals(bytes)) throw new Error("qualifier source changed");
  if (!readFileSync(inputs["runtime-inventory"]).equals(indexBytes) || !readFileSync(inputs["tool-inventory"]).equals(toolIndexBytes)) throw new Error("external inventory changed");
  verifyRuntime(inputs["runtime-root"], indexBytes, runtimePin); readTools(inputs["tool-home"], toolIndexBytes, toolchain);
  const after = readPackage({ ...args, ...inputs });
  if (!after.archiveBytes.equals(admitted.archiveBytes) || !after.receiptBytes.equals(admitted.receiptBytes)) throw new Error("archive evidence changed");
  for (const item of readDependencies(inputs.cache, toolchain).values()) if (!item.bytes.equals(dependencies.get(item.pin.name).bytes)) throw new Error("input dependency archive changed");
  const receipt = { schema: "acyclic.sdk.elixir-installed-qualification.v1", authority: "rust", target: "elixir",
    source_revision: admitted.receipt.source_revision, authority_manifest_sha256: admitted.receipt.authority_manifest_sha256,
    archive_sha256: sha256(admitted.archiveBytes), generation_receipt_sha256: sha256(admitted.receiptBytes),
    runtime_inventory_sha256: sha256(indexBytes), tool_inventory_sha256: sha256(toolIndexBytes),
    qualifier_source_sha256: Object.fromEntries([...sourceBytes].map(([name, bytes]) => [name, sha256(bytes)])),
    lock_sha256: sha256(lock), installed_dependencies: installedDependencies, payload_sha256: admitted.receipt.output_sha256,
    compiled_sdk_sha256: Object.fromEntries(files(join(output, "build/lib/acyclic_sdk_transport/ebin")).map(name => [name, sha256(readFileSync(join(output, "build/lib/acyclic_sdk_transport/ebin", name)))])),
    log_sha256: Object.fromEntries(logs.map(name => [name, sha256(readFileSync(join(output, name)))])),
    populated_rpc_message_pairs: 25, runtime_type_rejections: 3, compiled_sdk_provenance: true,
    native_rpc_calls_executed: false, rust_backed_rpc_qualified: false, embedded_runtime_qualified: false };
  put("qualification.json", JSON.stringify(receipt, null, 2) + "\n");
  return receipt;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const options = Object.fromEntries(["package", "sha256", "receipt", "authority", "runtime-root", "runtime-inventory", "tool-home", "tool-inventory", "cache", "output"].map(name => [name, { type: "string" }]));
  const { values } = parseArgs({ options });
  if (Object.keys(options).some(name => !values[name])) throw new Error("all installed qualification arguments are required");
  const result = qualify(values, { onProgress: message => process.stderr.write(message + "\n") });
  process.stdout.write(JSON.stringify(result, null, 2) + "\n");
}
