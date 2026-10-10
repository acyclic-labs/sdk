import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { copyFileSync, lstatSync, mkdirSync, readFileSync, readdirSync, realpathSync, writeFileSync } from "node:fs";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { canonical, loadAuthority, readInput, sha256, within } from "../../../shared/authority.mjs";

const directory = dirname(fileURLToPath(import.meta.url));
const pinned = JSON.parse(readFileSync(join(directory, "../toolchains/toolchain.json"))).qualification;
const coordinate = "acyclic.sdk.transport";
const version = "0.2.0-alpha.1";
const packageName = `Acyclic.Sdk.Transport.${version}.nupkg`;
const projectName = "Acyclic.Sdk.Transport.csproj";
const targets = ["actors/v1/actors.proto", "workers/v1/workers.proto", "stream/v1/stream.proto"];
const negatives = { InvalidActorBytes: "Google.Protobuf.ByteString", InvalidWorkerBytes: "Google.Protobuf.ByteString", InvalidOptionalInteger: "ulong" };
const xml = value => value.replaceAll("&", "&amp;").replaceAll('"', "&quot;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");

function files(root, prefix = "") {
  return readdirSync(join(root, prefix), { withFileTypes: true }).flatMap(entry => {
    const name = prefix ? `${prefix}/${entry.name}` : entry.name;
    if (entry.isSymbolicLink()) throw new Error("tool inventory contains a link");
    if (entry.isDirectory()) return files(root, name);
    if (!entry.isFile()) throw new Error("tool inventory contains a non-file");
    return [name];
  }).sort();
}

// Injected commands/pins support offline runner tests. The CLI uses the real tools.
export function qualify(args, { command = spawnSync, toolchain = pinned } = {}) {
  if (process.platform !== "win32" && command === spawnSync) throw new Error("installed qualification currently requires Windows NuGet.exe");
  const packageRoot = realpathSync(args.package);
  const authority = realpathSync(args.authority);
  const dotnet = realpathSync(args.dotnet);
  const dotnetHome = dirname(dotnet);
  const nuget = realpathSync(args.nuget);
  const cache = realpathSync(args.cache);
  const output = join(realpathSync(dirname(resolve(args.output))), basename(resolve(args.output)));
  try { lstatSync(output); throw new Error("qualification output must be absent"); }
  catch (error) { if (error.code !== "ENOENT") throw error; }
  for (const input of [packageRoot, authority, dotnetHome, nuget, cache, dirname(directory)]) {
    if (within(input, output) || within(output, input)) throw new Error("qualification output overlaps an input");
  }
  const receiptBytes = readFileSync(join(packageRoot, "generation-receipt.json"));
  const receipt = JSON.parse(receiptBytes);
  if (receipt.schema !== "acyclic.sdk.dotnet-producer-receipt.v1" || receipt.authority !== "rust"
    || receipt.target !== "dotnet" || !Array.isArray(receipt.outputs)) throw new Error("unsupported generation receipt");
  const approved = loadAuthority(authority);
  if (sha256(approved.bytes) !== receipt.authority_manifest_sha256
    || approved.manifest.source_revision !== receipt.source_revision) throw new Error("generation authority differs");
  if (targets.some(name => !approved.descriptors.has(name))) throw new Error("tested family lacks an attested descriptor");
  const payload = new Map();
  for (const name of receipt.outputs) {
    if (!canonical(name) || payload.has(name)) throw new Error("unsafe or duplicate package output");
    payload.set(name, readInput(packageRoot, name, receipt.output_sha256[name]));
  }
  for (const name of [projectName, "packages.lock.json", "global.json", "README.md"]) {
    if (!payload.has(name) || sha256(payload.get(name)) !== sha256(readFileSync(join(directory, "../templates/package", name)))) {
      throw new Error("package build metadata differs from the pinned template");
    }
  }
  if (!payload.has("rust-authority.json") || sha256(payload.get("rust-authority.json")) !== sha256(approved.bytes)) {
    throw new Error("package lacks matching authority metadata");
  }
  if (sha256(readFileSync(nuget)) !== toolchain.nuget_exe_sha256) throw new Error("NuGet executable differs from the published pin");
  const dependencies = JSON.parse(payload.get("packages.lock.json")).dependencies["net8.0"];
  const archives = new Map();
  for (const [id, dependency] of Object.entries(dependencies)) {
    const name = id.toLowerCase();
    if (!/^[a-z0-9.-]+$/.test(name) || !/^[0-9.]+$/.test(dependency.resolved) || name === coordinate) throw new Error("unsafe dependency coordinate");
    const filename = `${name}.${dependency.resolved}.nupkg`;
    const path = realpathSync(join(cache, name, dependency.resolved, filename));
    if (!within(cache, path)) throw new Error("dependency archive escapes cache");
    const bytes = readFileSync(path);
    // NuGet lock hashes describe unsigned package content, rather than the raw
    // repository-signed ZIP. Pin raw bytes separately to the official catalog;
    // the subsequent locked restore also verifies NuGet's content hash.
    if (createHash("sha512").update(bytes).digest("base64") !== toolchain.dependency_archives?.[filename]?.sha512) {
      throw new Error("dependency archive differs from the official catalog pin");
    }
    archives.set(filename, bytes);
  }
  const env = { ...process.env };
  for (const name of Object.keys(env)) {
    if (/^(DOTNET_|MSBUILD|NUGET_|COREHOST_|COMPLUS_|COR_|STARTUP_HOOKS)/i.test(name)) delete env[name];
  }
  Object.assign(env, { DOTNET_ROOT: dotnetHome, DOTNET_MULTILEVEL_LOOKUP: "0", DOTNET_CLI_HOME: join(output, "home"),
    DOTNET_CLI_TELEMETRY_OPTOUT: "1", DOTNET_NOLOGO: "1", DOTNET_CLI_USE_MSBUILD_SERVER: "0", MSBUILDDISABLENODEREUSE: "1",
    NUGET_PACKAGES: join(output, "cache"), NUGET_HTTP_CACHE_PATH: join(output, "http-cache"), DOTNET_PROCESSOR_COUNT: "1" });
  Object.assign(env, { DOTNET_CLI_UI_LANGUAGE: "en-US", VSLANG: "1033", NUGET_CERT_REVOCATION_MODE: "offline" });
  const execute = (exe, argv, cwd) => command(exe, argv, { cwd, env, encoding: "utf8", timeout: 180_000, maxBuffer: 8 * 1024 * 1024 });
  // Check version in a directory carrying the package's exact global.json.
  const sdk = execute(dotnet, ["--list-sdks"], directory);
  if (sdk.error || sdk.status !== 0 || !(sdk.stdout ?? "").split(/\r?\n/).some(line => line.startsWith(`${toolchain.dotnet_sdk} [`))) {
    throw new Error("installed controls require the pinned .NET SDK");
  }
  mkdirSync(output);
  const project = join(output, "project");
  for (const [name, bytes] of payload) {
    const destination = join(project, name);
    mkdirSync(dirname(destination), { recursive: true });
    writeFileSync(destination, bytes, { flag: "wx" });
  }
  writeFileSync(join(output, "global.json"), payload.get("global.json"), { flag: "wx" });
  const feed = join(output, "feed");
  mkdirSync(feed);
  for (const [name, bytes] of archives) writeFileSync(join(feed, name), bytes, { flag: "wx" });
  const config = join(output, "NuGet.Config");
  writeFileSync(config, `<configuration><packageSources><clear/><add key="owned-local-feed" value="${xml(feed)}"/></packageSources><fallbackPackageFolders><clear/></fallbackPackageFolders></configuration>\n`, { flag: "wx" });
  const logs = [];
  const run = (name, exe, argv, cwd, failure = false) => {
    const result = execute(exe, argv, cwd);
    const text = (result.stdout ?? "") + (result.stderr ?? "");
    const log = `${name}.log`;
    writeFileSync(join(output, log), text, { flag: "wx" }); logs.push(log);
    if (result.error || result.status === null || (result.status !== 0) !== failure) throw new Error(`${name} failed: ${text}`);
    return text;
  };
  const buildArguments = ["--no-restore", "-c", "Release", "-m:1", "-p:BuildInParallel=false", "-p:UseSharedCompilation=false",
    "-p:ImportDirectoryBuildProps=false", "-p:ImportDirectoryBuildTargets=false"];
  const restore = (name, projectFile, cwd, locked) => run(name, dotnet,
    ["restore", projectFile, "--configfile", config, "--disable-parallel", "-p:ImportDirectoryBuildProps=false",
      "-p:ImportDirectoryBuildTargets=false", ...(locked ? ["--locked-mode"] : [])], cwd);
  restore("restore-package", join(project, projectName), project, true);
  const raw = join(output, "raw-pack");
  run("build-package", dotnet, ["pack", join(project, projectName), ...buildArguments, "--output", raw], project);
  run("deterministic-pack", nuget, ["pack", join(project, "obj", "Release", `Acyclic.Sdk.Transport.${version}.nuspec`),
    "-OutputDirectory", feed, "-Deterministic", "-DeterministicTimestamp", toolchain.deterministic_timestamp,
    "-ConfigFile", config, "-NonInteractive", "-ForceEnglishOutput"], project);
  const archive = join(feed, packageName);
  const assembly = join(project, "bin", "Release", "net8.0", "Acyclic.Sdk.Transport.dll");
  const assemblyHash = sha256(readFileSync(assembly));
  const controls = join(directory, "../tests/fixtures", "consumer");
  const snapshots = join(output, "authority"); mkdirSync(snapshots);
  const descriptors = targets.map((name, index) => {
    const file = join(snapshots, `${index}.bin`); writeFileSync(file, approved.inputs.get(approved.descriptors.get(name)), { flag: "wx" }); return file;
  });
  const controlNames = ["Consumer.csproj", "InstalledConsumer.cs", ...Object.keys(negatives).map(name => `${name}.cs`)];
  for (const [name, expected] of [["positive", null], ...Object.entries(negatives)]) {
    const consumer = join(output, name); mkdirSync(consumer);
    copyFileSync(join(controls, "Consumer.csproj"), join(consumer, "Consumer.csproj"));
    copyFileSync(join(controls, expected ? `${name}.cs` : "InstalledConsumer.cs"), join(consumer, "Program.cs"));
    restore(`restore-${name}`, join(consumer, "Consumer.csproj"), consumer, false);
    const log = run(`compile-${name}`, dotnet, ["build", join(consumer, "Consumer.csproj"), ...buildArguments], consumer, Boolean(expected));
    if (expected) {
      const errors = new Set([...log.matchAll(/error (CS\d+): ([^\r\n]+)/g)].map(match => match[0]));
      if (errors.size !== 1 || ![...errors][0].includes(`error CS0029: Cannot implicitly convert type 'string' to '${expected}'`)) {
        throw new Error(`${name} failed for an unrelated reason`);
      }
    } else {
      const installed = join(env.NUGET_PACKAGES, coordinate, version);
      if (sha256(readFileSync(join(installed, `${coordinate}.${version}.nupkg`))) !== sha256(readFileSync(archive))
        || sha256(readFileSync(join(installed, "lib", "net8.0", "Acyclic.Sdk.Transport.dll"))) !== assemblyHash) {
        throw new Error("NuGet installation differs from built package");
      }
      run("positive", dotnet, [join(consumer, "bin", "Release", "net8.0", "Consumer.dll"), ...descriptors, assemblyHash], consumer);
    }
  }
  const result = {
    schema: "acyclic.sdk.dotnet.installed-qualification.v1", scope: "installed-dotnet-transport-bindings",
    source_revision: receipt.source_revision, authority_manifest_sha256: sha256(approved.bytes), generation_receipt_sha256: sha256(receiptBytes),
    nuget_package_sha256: sha256(readFileSync(archive)), assembly_sha256: assemblyHash,
    dotnet_sdk: toolchain.dotnet_sdk, nuget_version: toolchain.nuget_version, deterministic_timestamp: toolchain.deterministic_timestamp,
    tool_sha256: { nuget: sha256(readFileSync(nuget)), ...Object.fromEntries(files(dotnetHome).map(name => [`dotnet/${name}`, sha256(readFileSync(join(dotnetHome, name)))])) },
    dependency_archive_sha256: Object.fromEntries([...archives].map(([name, bytes]) => [name, sha256(bytes)])),
    qualifier_sha256: sha256(readFileSync(fileURLToPath(import.meta.url))), authority_reader_sha256: sha256(readFileSync(join(directory, "../../../shared/authority.mjs"))),
    toolchain_sha256: sha256(JSON.stringify(toolchain)),
    control_sha256: Object.fromEntries(controlNames.map(name => [name, sha256(readFileSync(join(controls, name)))])),
    log_sha256: Object.fromEntries(logs.map(name => [name, sha256(readFileSync(join(output, name)))])),
    positive_controls_passed: true, negative_type_controls_rejected: 3, rust_backed_rpc_qualified: false, embedded_runtime_qualified: false,
  };
  writeFileSync(join(output, "qualification.json"), JSON.stringify(result, null, 2) + "\n", { flag: "wx" });
  return result;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const options = Object.fromEntries(["package", "authority", "dotnet", "nuget", "cache", "output"].map(name => [name, { type: "string" }]));
  const { values } = parseArgs({ options });
  if (Object.keys(options).some(name => !values[name])) throw new Error("--package, --authority, --dotnet, --nuget, --cache and --output are required");
  const result = qualify(values);
  console.log(JSON.stringify({ scope: result.scope, source_revision: result.source_revision, nuget_package_sha256: result.nuget_package_sha256,
    qualification: resolve(values.output, "qualification.json") }, null, 2));
}
