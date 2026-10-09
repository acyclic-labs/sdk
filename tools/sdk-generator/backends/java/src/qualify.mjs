import { spawnSync } from "node:child_process";
import { copyFileSync, lstatSync, mkdirSync, readFileSync, readdirSync, realpathSync, writeFileSync } from "node:fs";
import { basename, delimiter, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { canonical, loadAuthority, readInput, sha256, within } from "../../../shared/authority.mjs";

const directory = dirname(fileURLToPath(import.meta.url));
const targets = ["actors/v1/actors.proto", "workers/v1/workers.proto", "stream/v2/stream.proto"];
const negatives = { "InvalidActorBytes.java": "ByteString", "InvalidWorkerBytes.java": "ByteString", "InvalidOptionalInteger.java": "long" };
const artifact = "sdk-java-transport-0.2.0-alpha.1";

function files(root, prefix = "") {
  return readdirSync(join(root, prefix), { withFileTypes: true }).flatMap(entry => {
    const name = prefix ? `${prefix}/${entry.name}` : entry.name;
    if (entry.isSymbolicLink()) throw new Error("tool/cache inventory contains a link");
    if (entry.isDirectory()) return files(root, name);
    if (!entry.isFile()) throw new Error("tool/cache inventory contains a non-file");
    return [name];
  }).sort();
}

export function qualify(args, command = spawnSync) {
  const packageRoot = realpathSync(args.package);
  const authority = realpathSync(args.authority);
  const javaHome = realpathSync(args["java-home"]);
  const mavenHome = realpathSync(args["maven-home"]);
  const cache = realpathSync(args.cache);
  const output = join(realpathSync(dirname(resolve(args.output))), basename(resolve(args.output)));
  try { lstatSync(output); throw new Error("qualification output must be absent"); }
  catch (error) { if (error.code !== "ENOENT") throw error; }
  for (const input of [packageRoot, authority, javaHome, mavenHome, cache, dirname(directory)]) {
    if (within(input, output) || within(output, input)) throw new Error("qualification output overlaps an input");
  }
  const receiptBytes = readFileSync(join(packageRoot, "generation-receipt.json"));
  const receipt = JSON.parse(receiptBytes);
  if (receipt.schema !== "acyclic.sdk.java-producer-receipt.v1" || receipt.authority !== "rust"
    || receipt.target !== "java" || !Array.isArray(receipt.outputs)) throw new Error("unsupported generation receipt");
  const approved = loadAuthority(authority);
  if (sha256(approved.bytes) !== receipt.authority_manifest_sha256
    || approved.manifest.source_revision !== receipt.source_revision) throw new Error("generation authority differs");
  if (targets.some(name => !approved.descriptors.has(name))) throw new Error("tested family lacks an attested descriptor");
  const payload = new Map();
  for (const name of receipt.outputs) {
    if (!canonical(name) || payload.has(name)) throw new Error("unsafe or duplicate package output");
    payload.set(name, readInput(packageRoot, name, receipt.output_sha256[name]));
  }
  if (!payload.has("pom.xml") || !payload.has("src/main/resources/META-INF/rust-authority.json")
    || sha256(payload.get("src/main/resources/META-INF/rust-authority.json")) !== sha256(approved.bytes)) {
    throw new Error("package lacks matching authority metadata");
  }
  const extension = process.platform === "win32" ? ".exe" : "";
  const java = realpathSync(join(javaHome, "bin", `java${extension}`));
  const javac = realpathSync(join(javaHome, "bin", `javac${extension}`));
  const boot = readdirSync(join(mavenHome, "boot")).filter(name => /^plexus-classworlds-.*\.jar$/.test(name));
  if (boot.length !== 1) throw new Error("Maven must have one classworlds launcher");
  const bootJar = join(mavenHome, "boot", boot[0]);
  const env = { ...process.env, JAVA_HOME: javaHome, MAVEN_HOME: mavenHome, LC_ALL: "C" };
  for (const name of ["MAVEN_ARGS", "MAVEN_OPTS", "MAVEN_EXT_CLASS_PATH", "CLASSPATH",
    "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS"]) delete env[name];
  const options = { env, encoding: "utf8", timeout: 180_000, maxBuffer: 8 * 1024 * 1024 };
  const execute = (executable, argv, cwd) => command(executable, argv, { ...options, cwd });
  const javaVersion = execute(java, ["-Xmx256m", "-XX:ActiveProcessorCount=1", "-version"], directory);
  const javacVersion = execute(javac, ["-J-Xmx256m", "-J-XX:ActiveProcessorCount=1", "-version"], directory);
  const versionText = result => (result.stdout ?? "") + (result.stderr ?? "");
  if (javaVersion.error || javaVersion.status !== 0 || !versionText(javaVersion).split(/\r?\n/)[0].includes('"17.0.14"')
    || javacVersion.error || javacVersion.status !== 0 || versionText(javacVersion).trim() !== "javac 17.0.14") {
    throw new Error("installed controls require JDK 17.0.14");
  }
  const mavenArguments = project => ["-Xmx512m", "-XX:ActiveProcessorCount=1", "-Duser.language=en", "-Duser.country=US",
    "-classpath", bootJar, `-Dclassworlds.conf=${join(mavenHome, "bin", "m2.conf")}`, `-Dmaven.home=${mavenHome}`,
    `-Dmaven.multiModuleProjectDirectory=${project}`, "org.codehaus.plexus.classworlds.launcher.Launcher"];
  const mavenVersion = execute(java, [...mavenArguments(directory), "--version", "-Dstyle.color=never"], directory);
  if (mavenVersion.error || mavenVersion.status !== 0 || !versionText(mavenVersion).includes("Apache Maven 3.9.9")) {
    throw new Error("installed controls require Maven 3.9.9");
  }
  // The cache is caller-owned and must remain exclusive throughout the run.
  const cacheHashes = Object.fromEntries(files(cache).filter(name => /\.(jar|pom)$/.test(name)
    && !name.startsWith("dev/acyclic/sdk-java-transport/")).map(name => [name, sha256(readFileSync(join(cache, name)))]));
  mkdirSync(output);
  const project = join(output, "project");
  for (const [name, bytes] of payload) {
    const path = join(project, name);
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, bytes, { flag: "wx" });
  }
  const settings = join(output, "settings.xml");
  writeFileSync(settings, '<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"/>\n', { flag: "wx" });
  const logs = [];
  const run = (name, executable, argv, cwd, failure = false) => {
    const result = execute(executable, argv, cwd);
    const text = versionText(result);
    const log = `${name}.log`;
    writeFileSync(join(output, log), text, { flag: "wx" });
    logs.push(log);
    if (result.error || result.status === null || (result.status !== 0) !== failure) throw new Error(`${name} failed: ${text}`);
    return text;
  };
  run("maven-install", java, [...mavenArguments(project), "-B", "-ntp", "-o", "-T", "1", "-s", settings, "-gs", settings,
    `-Dmaven.repo.local=${cache}`, "-Dstyle.color=never", "-f", join(project, "pom.xml"), "-DskipTests",
    "install", "dependency:build-classpath", "-Dmdep.outputFile=runtime-classpath.txt"], project);
  const installedDirectory = join(output, "installed");
  mkdirSync(installedDirectory);
  const cachedArtifact = join(cache, "dev", "acyclic", "sdk-java-transport", "0.2.0-alpha.1", artifact);
  if (sha256(readFileSync(`${cachedArtifact}.jar`)) !== sha256(readFileSync(join(project, "target", `${artifact}.jar`)))
    || sha256(readFileSync(`${cachedArtifact}.pom`)) !== sha256(payload.get("pom.xml"))) throw new Error("Maven installation differs from built package");
  const jar = join(installedDirectory, `${artifact}.jar`);
  copyFileSync(`${cachedArtifact}.jar`, jar);
  copyFileSync(`${cachedArtifact}.pom`, join(installedDirectory, `${artifact}.pom`));
  const dependencies = readFileSync(join(project, "runtime-classpath.txt"), "utf8").trim().split(delimiter).map(name => {
    const path = realpathSync(name);
    if (!within(cache, path) || !path.endsWith(".jar") || path === `${cachedArtifact}.jar`) throw new Error("dependency classpath escapes prepared cache");
    return path;
  });
  const classpath = [jar, ...dependencies].join(delimiter);
  const consumer = join(output, "consumer");
  mkdirSync(consumer);
  const controls = join(directory, "../tests/fixtures", "consumer");
  const controlNames = ["InstalledConsumer.java", ...Object.keys(negatives)];
  for (const name of controlNames) copyFileSync(join(controls, name), join(consumer, name));
  const descriptors = targets.map(name => {
    const descriptor = approved.descriptors.get(name);
    const path = join(output, "authority", descriptor);
    if (!lstatExists(path)) {
      mkdirSync(dirname(path), { recursive: true });
      writeFileSync(path, approved.inputs.get(descriptor), { flag: "wx" });
    }
    return path;
  });
  const compileArguments = ["-J-Xmx256m", "-J-XX:ActiveProcessorCount=1", "-J-Duser.language=en", "-J-Duser.country=US",
    "--release", "17", "-cp", classpath];
  run("compile-positive", javac, [...compileArguments, "-d", consumer, join(consumer, "InstalledConsumer.java")], consumer);
  const positive = run("positive", java, ["-Xmx256m", "-XX:ActiveProcessorCount=1", `-Dsdk.qualified.jar=${jar}`,
    "-ea", "-cp", [consumer, classpath].join(delimiter), "InstalledConsumer", ...descriptors], consumer);
  if (!positive.includes("PASS: installed Java descriptors")) throw new Error("installed consumer did not report success");
  for (const [name, expected] of Object.entries(negatives)) {
    const destination = join(consumer, name.replace(".java", ""));
    mkdirSync(destination);
    const negative = run(name.replace(".java", ""), javac, [...compileArguments, "-d", destination, join(consumer, name)], consumer, true);
    if ((negative.match(/error:/g) ?? []).length !== 1 || !negative.includes(`String cannot be converted to ${expected}`)) {
      throw new Error(`${name} failed for an unrelated reason`);
    }
  }
  const result = {
    schema: "acyclic.sdk.java.installed-qualification.v1", scope: "installed-java-transport-bindings",
    source_revision: receipt.source_revision, authority_manifest_sha256: sha256(approved.bytes),
    generation_receipt_sha256: sha256(receiptBytes), installed_jar_sha256: sha256(readFileSync(jar)),
    installed_pom_sha256: sha256(readFileSync(join(installedDirectory, `${artifact}.pom`))),
    java_version: versionText(javaVersion).trim(), javac_version: versionText(javacVersion).trim(), maven_version: versionText(mavenVersion).trim(),
    tool_sha256: { java: sha256(readFileSync(java)), javac: sha256(readFileSync(javac)),
      java_modules: sha256(readFileSync(join(javaHome, "lib", "modules"))), java_release: sha256(readFileSync(join(javaHome, "release"))),
      ...Object.fromEntries(files(mavenHome).filter(name => /\.(jar|conf)$/.test(name)).map(name => [`maven/${name}`, sha256(readFileSync(join(mavenHome, name)))])) },
    build_cache_sha256: cacheHashes, dependency_sha256: Object.fromEntries(dependencies.map(path => [path, sha256(readFileSync(path))])),
    qualifier_sha256: sha256(readFileSync(fileURLToPath(import.meta.url))), authority_reader_sha256: sha256(readFileSync(join(directory, "../../../shared/authority.mjs"))),
    control_sha256: Object.fromEntries(controlNames.map(name => [name, sha256(readFileSync(join(controls, name)))])),
    log_sha256: Object.fromEntries(logs.map(name => [name, sha256(readFileSync(join(output, name)))])),
    positive_controls_passed: true, negative_type_controls_rejected: 3,
    rust_backed_rpc_qualified: false, embedded_runtime_qualified: false,
  };
  writeFileSync(join(output, "qualification.json"), JSON.stringify(result, null, 2) + "\n", { flag: "wx" });
  return result;
}

function lstatExists(path) {
  try { lstatSync(path); return true; }
  catch (error) { if (error.code === "ENOENT") return false; throw error; }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const options = Object.fromEntries(["package", "authority", "java-home", "maven-home", "cache", "output"].map(name => [name, { type: "string" }]));
  const { values } = parseArgs({ options });
  if (Object.keys(options).some(name => !values[name])) throw new Error("--package, --authority, --java-home, --maven-home, --cache and --output are required");
  const result = qualify(values);
  console.log(JSON.stringify({ scope: result.scope, source_revision: result.source_revision,
    installed_jar_sha256: result.installed_jar_sha256, qualification: resolve(values.output, "qualification.json") }, null, 2));
}
