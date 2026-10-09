import { spawnSync } from "node:child_process";
import { lstatSync, mkdirSync, readFileSync, realpathSync, writeFileSync } from "node:fs";
import { basename, delimiter, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { canonical, loadAuthority, readInput, sha256, within } from "../../../shared/authority.mjs";

const directory = dirname(fileURLToPath(import.meta.url));
const artifact = "sdk-java-transport-0.2.0-alpha.1";
const targets = ["actors/v1/actors.proto", "workers/v1/workers.proto", "stream/v1/stream.proto"];

// Commands and manifests are injectable only for offline runner controls.
export function qualifyJvm(args, { command = spawnSync, toolchains } = {}) {
  if (!["kotlin", "scala"].includes(args.language)) throw new Error("language must be kotlin or scala");
  const installation = realpathSync(args.installation);
  const authority = realpathSync(args.authority);
  const cache = realpathSync(args.cache);
  const javaHome = realpathSync(args["java-home"]);
  const output = join(realpathSync(dirname(resolve(args.output))), basename(resolve(args.output)));
  try { lstatSync(output); throw new Error("qualification output must be absent"); }
  catch (error) { if (error.code !== "ENOENT") throw error; }
  for (const input of [installation, authority, cache, javaHome, dirname(directory)]) {
    if (within(input, output) || within(output, input)) throw new Error("qualification output overlaps an input");
  }
  const proofBytes = readFileSync(join(installation, "qualification.json"));
  const proof = JSON.parse(proofBytes);
  const approved = loadAuthority(authority);
  if (proof.schema !== "acyclic.sdk.java.installed-qualification.v1" || proof.scope !== "installed-java-transport-bindings"
    || proof.source_revision !== approved.manifest.source_revision || proof.authority_manifest_sha256 !== sha256(approved.bytes)
    || proof.positive_controls_passed !== true || proof.negative_type_controls_rejected !== 3) throw new Error("installation lacks matching Java qualification");
  if (targets.some(name => !approved.descriptors.has(name))) throw new Error("tested family lacks an attested descriptor");
  const jarBytes = readInput(installation, `installed/${artifact}.jar`, proof.installed_jar_sha256);
  const pomBytes = readInput(installation, `installed/${artifact}.pom`, proof.installed_pom_sha256);
  const tools = toolchains ?? JSON.parse(readFileSync(join(directory, "../toolchains/jvm-toolchains.json")));
  const toolchain = tools[args.language];
  if (!toolchain || !Array.isArray(toolchain.artifacts) || toolchain.artifacts.length === 0
    || !Array.isArray(toolchain.runtime) || toolchain.runtime.length === 0) throw new Error("compiler toolchain lacks pinned artifacts");
  const compiler = new Map();
  for (const entry of toolchain.artifacts) {
    if (!canonical(entry.path) || !entry.path.endsWith(".jar") || compiler.has(entry.path)) throw new Error("unsafe or duplicate compiler artifact");
    compiler.set(entry.path, readInput(cache, entry.path, entry.sha256));
  }
  if (toolchain.runtime.some(name => !compiler.has(name))) throw new Error("runtime is not in the pinned compiler closure");
  const dependencies = new Map();
  for (const [name, digest] of Object.entries(proof.dependency_sha256 ?? {})) {
    const path = realpathSync(name);
    if (!within(cache, path) || !path.endsWith(".jar") || dependencies.has(path)) throw new Error("dependency escapes prepared cache");
    const bytes = readFileSync(path);
    if (sha256(bytes) !== digest) throw new Error("SDK dependency digest mismatch");
    dependencies.set(path, bytes);
  }
  if (dependencies.size === 0) throw new Error("SDK dependency closure is empty");
  const extension = process.platform === "win32" ? ".exe" : "";
  const java = realpathSync(join(javaHome, "bin", `java${extension}`));
  const javac = realpathSync(join(javaHome, "bin", `javac${extension}`));
  const env = { ...process.env, JAVA_HOME: javaHome };
  for (const key of Object.keys(env)) {
    if (/^(JAVA_TOOL_OPTIONS|JDK_JAVA_OPTIONS|_JAVA_OPTIONS|CLASSPATH)$/i.test(key)) delete env[key];
  }
  const options = { env, encoding: "utf8", timeout: 180_000, maxBuffer: 8 * 1024 * 1024 };
  const execute = (exe, argv, cwd) => command(exe, argv, { ...options, cwd });
  const versionText = result => (result.stdout ?? "") + (result.stderr ?? "");
  const javaVersion = execute(java, ["-version"], directory);
  const javacVersion = execute(javac, ["-version"], directory);
  if (javaVersion.error || javaVersion.status !== 0 || !versionText(javaVersion).split(/\r?\n/)[0].includes('"17.0.14"')
    || javacVersion.error || javacVersion.status !== 0 || versionText(javacVersion).trim() !== "javac 17.0.14") throw new Error("JVM controls require JDK 17.0.14");
  mkdirSync(output);
  const put = (name, bytes) => {
    const path = join(output, name); mkdirSync(dirname(path), { recursive: true }); writeFileSync(path, bytes, { flag: "wx" }); return path;
  };
  const jar = put(`installed/${artifact}.jar`, jarBytes);
  put(`installed/${artifact}.pom`, pomBytes);
  const compilerPaths = new Map([...compiler].map(([name, bytes]) => [name, put(`compiler/${name}`, bytes)]));
  const dependencyPaths = [...dependencies].map(([, bytes], index) => put(`dependencies/${index}.jar`, bytes));
  const logs = [];
  const run = (name, exe, argv, failure = false) => {
    const result = execute(exe, argv, output); const text = versionText(result); const log = `${name}.log`;
    put(log, text); logs.push(log);
    if (result.error || result.status === null || (result.status !== 0) !== failure) throw new Error(`${name} failed: ${text}`);
    return text;
  };
  const javaOptions = ["-Xmx512m", "-XX:ActiveProcessorCount=1", "-Duser.language=en", "-Duser.country=US"];
  const compilerArguments = [...javaOptions, "-cp", [...compilerPaths.values()].join(delimiter), toolchain.main_class];
  const compilerVersion = run("compiler-version", java, [...compilerArguments, "-version"]);
  const expectedVersion = args.language === "kotlin" ? `kotlinc-jvm ${toolchain.version}` : `version ${toolchain.version}`;
  if (!compilerVersion.includes(expectedVersion)) throw new Error("compiler reports an unexpected pinned version");
  const helper = join(output, "helper"); mkdirSync(helper);
  const classpath = [jar, ...dependencyPaths, ...toolchain.runtime.map(name => compilerPaths.get(name)), helper].join(delimiter);
  const helperSource = put("controls/InstalledConsumer.java", readFileSync(join(directory, "../tests/fixtures", "consumer", "InstalledConsumer.java")));
  run("compile-helper", javac, ["-J-Xmx256m", "-J-XX:ActiveProcessorCount=1", "--release", "17", "-cp", classpath, "-d", helper, helperSource]);
  const entry = args.language === "kotlin" ? "InstalledKotlinConsumer" : "InstalledScalaConsumer";
  const sourceExtension = args.language === "kotlin" ? "kt" : "scala";
  const controls = ["InstalledConsumer.java"];
  const compile = (name, input, negative = false) => {
    const destination = join(output, name); mkdirSync(destination);
    const file = `${input}.${sourceExtension}`;
    const source = put(`controls/${file}`, readFileSync(join(directory, "../tests/fixtures", args.language, file))); controls.push(file);
    const arguments_ = args.language === "kotlin" ? ["-no-stdlib", "-no-reflect", "-jvm-target", "17"] : ["-release", "17", "-color:never"];
    return { destination, log: run(`compile-${name}`, java, [...compilerArguments, ...arguments_, "-classpath", classpath, "-d", destination, source], negative) };
  };
  const positive = compile("positive", entry);
  const descriptors = targets.map((name, index) => put(`authority/${index}.bin`, approved.inputs.get(approved.descriptors.get(name))));
  const positiveLog = run("positive", java, [...javaOptions, `-Dsdk.qualified.jar=${jar}`, "-cp", [classpath, positive.destination].join(delimiter), entry, ...descriptors]);
  const languageName = args.language === "kotlin" ? "Kotlin" : "Scala";
  if (!positiveLog.includes("PASS: installed Java descriptors, bytes, unsigned bounds, optional presence, oneof and gRPC shapes")
    || !positiveLog.includes(`PASS: installed ${languageName} Java-binding interoperability, bytes, integer bits, presence and oneof`)) {
    throw new Error("positive consumer did not report completed Java and language controls");
  }
  for (const name of ["ActorBytes", "WorkerBytes", "OptionalInteger"]) {
    const { log } = compile(`Invalid${name}`, `Invalid${name}`, true);
    const expected = name === "OptionalInteger" ? "Long" : "ByteString";
    const valid = args.language === "kotlin"
      ? (log.match(/error:/g) ?? []).length === 1 && /argument type mismatch/i.test(log) && log.includes("String") && log.includes(expected)
      : /1 error found/.test(log) && log.includes("Type Mismatch Error") && log.includes("Found:") && log.includes("String") && log.includes("Required:") && log.includes(expected);
    if (!valid) throw new Error(`${name} failed for an unrelated reason`);
  }
  const result = {
    schema: "acyclic.sdk.jvm-consumer.installed-qualification.v1", scope: `installed-${args.language}-java-binding-interoperability`,
    source_revision: approved.manifest.source_revision, authority_manifest_sha256: sha256(approved.bytes),
    java_qualification_receipt_sha256: sha256(proofBytes), installed_jar_sha256: sha256(jarBytes), installed_pom_sha256: sha256(pomBytes),
    compiler_version: compilerVersion.trim(), java_version: versionText(javaVersion).trim(), javac_version: versionText(javacVersion).trim(),
    compiler_sha256: Object.fromEntries([...compiler].map(([name, bytes]) => [name, sha256(bytes)])),
    dependency_sha256: Object.fromEntries([...dependencies].map(([name, bytes]) => [name, sha256(bytes)])),
    tool_sha256: Object.fromEntries([java, javac, join(javaHome, "lib", "modules"), join(javaHome, "release")].map(name => [name, sha256(readFileSync(name))])),
    qualifier_sha256: sha256(readFileSync(fileURLToPath(import.meta.url))), authority_reader_sha256: sha256(readFileSync(join(directory, "../../../shared/authority.mjs"))),
    toolchain_sha256: sha256(JSON.stringify(toolchain)), control_sha256: Object.fromEntries(controls.map(name => [name, sha256(readFileSync(join(output, "controls", name)))])),
    log_sha256: Object.fromEntries(logs.map(name => [name, sha256(readFileSync(join(output, name)))])),
    positive_controls_passed: true, independent_negative_type_controls_rejected: 3, rust_backed_rpc_qualified: false, embedded_runtime_qualified: false,
  };
  put("qualification.json", JSON.stringify(result, null, 2) + "\n"); return result;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const options = Object.fromEntries(["language", "installation", "authority", "cache", "java-home", "output"].map(name => [name, { type: "string" }]));
  const { values } = parseArgs({ options });
  if (Object.keys(options).some(name => !values[name])) throw new Error("--language, --installation, --authority, --cache, --java-home and --output are required");
  const result = qualifyJvm(values);
  console.log(JSON.stringify({ scope: result.scope, installed_jar_sha256: result.installed_jar_sha256, qualification: resolve(values.output, "qualification.json") }, null, 2));
}
