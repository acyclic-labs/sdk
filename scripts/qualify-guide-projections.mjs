#!/usr/bin/env node

import { createHash } from "node:crypto";
import { cpSync, existsSync, mkdirSync, readFileSync, readdirSync, realpathSync, statSync, unlinkSync, writeFileSync } from "node:fs";
import { extname, join, relative, resolve } from "node:path";
import { spawn, spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

// Resolve from the script directory so this remains correct when invoked from a docs checkout, a release archive, or a clean worktree.
const repo = resolve(fileURLToPath(new URL(".", import.meta.url)), "..");
const cargo = process.env.CARGO_BIN ?? (process.platform === "win32" ? join(process.env.USERPROFILE ?? "C:\\Users\\varun", ".cargo", "bin", "cargo.exe") : "cargo");
// Release qualification supplies pinned toolchain paths. Keep every local
// fallback overridable so a clean checkout can use the same resolver without
// depending on a machine's PATH layout.
const binaries = {
  rustfmt: process.env.SDK_RUSTFMT_BIN ?? process.env.RUSTFMT_BIN ?? "rustfmt",
  python: process.env.SDK_PYTHON_BIN ?? process.env.PYTHON_BIN ?? "python",
  bun: process.env.SDK_BUN_BIN ?? process.env.BUN_BIN ?? "bun",
  node: process.env.SDK_NODE_BIN ?? process.env.NODE_BIN ?? "node",
  tsc: process.env.SDK_TSC_BIN ?? process.env.TSC_BIN ?? "tsc",
  go: process.env.SDK_GO_BIN ?? process.env.GO_BIN ?? "go",
  maven: process.env.SDK_MAVEN_BIN ?? process.env.MAVEN_BIN ?? "mvn",
  dotnet: process.env.SDK_DOTNET_BIN ?? process.env.DOTNET_BIN ?? "dotnet",
  ruby: process.env.SDK_RUBY_BIN ?? process.env.RUBY_BIN ?? "ruby",
  gem: process.env.SDK_GEM_BIN ?? process.env.GEM_BIN ?? "gem",
  dart: process.env.SDK_DART_BIN ?? process.env.DART_BIN ?? "dart",
  php: process.env.SDK_PHP_BIN ?? process.env.PHP_BIN ?? "php",
  composer: process.env.SDK_COMPOSER_BIN ?? process.env.COMPOSER_BIN ?? "composer",
  java: process.env.SDK_JAVA_BIN ?? process.env.JAVA_BIN ?? "java",
};
const args = new Map();
for (let i = 2; i < process.argv.length; i += 1) {
  const value = process.argv[i];
  if (value === "--execute" || value === "--strict") args.set(value, true);
  else if (value.startsWith("--")) args.set(value, process.argv[++i]);
}

const output = resolve(args.get("--output") ?? join(repo, "work", "guide-projection-qualification"));
const snippets = join(output, "snippets");
mkdirSync(snippets, { recursive: true });

function command(name, commandArgs, cwd = repo, extraEnv = {}) {
  const shell = /\.(?:cmd|bat)$/i.test(name);
  const result = spawnSync(name, commandArgs, {
    cwd,
    encoding: "utf8",
    windowsHide: true,
    shell,
    env: { ...process.env, ...extraEnv },
    timeout: Number(process.env.QUALIFY_COMMAND_TIMEOUT_MS ?? 120000),
    killSignal: "SIGTERM",
  });
  return {
    command: [name, ...commandArgs].join(" "),
    exitCode: result.status ?? 127,
    stdout: result.stdout ?? "",
    stderr: result.stderr ?? (result.error?.message ?? ""),
  };
}

async function startFixture() {
  const binary = join(repo, "rust", "crates", "sdk-examples", "target", "debug", process.platform === "win32" ? "fixture-server.exe" : "fixture-server");
  if (!existsSync(binary)) return null;
  const child = spawn(binary, ["--port", "0", "--grpc-port", "0", "--max-requests", "512"], {
    cwd: repo,
    stdio: ["ignore", "pipe", "pipe"],
    windowsHide: true,
  });
  const address = await new Promise((resolveAddress, reject) => {
    let buffer = "";
    child.stdout.setEncoding("utf8");
    child.stdout.on("data", (chunk) => {
      buffer += chunk;
      for (const line of buffer.split(/\r?\n/).slice(0, -1)) {
        try {
          const record = JSON.parse(line);
          if (record.grpc_address) {
            resolveAddress(record.grpc_address);
            return;
          }
        } catch {}
      }
      buffer = buffer.split(/\r?\n/).at(-1) ?? "";
    });
    child.once("error", reject);
    child.once("exit", (code) => reject(new Error(`fixture server exited before readiness (${code})`)));
  });
  child.stdout.unref();
  child.stderr.unref();
  child.unref();
  return { child, address };
}

function fixtureEnvironment(language, address) {
  const normalized = address.replace(/^https?:\/\//, "");
  const urlLanguages = new Set(["typescript", "csharp"]);
  return { FIXTURE_GRPC_ADDRESS: urlLanguages.has(language) ? address : normalized };
}

function allFiles(root, seen = new Set()) {
  const files = [];
  if (!existsSync(root)) return files;
  const pending = [root];
  while (pending.length) {
    const directory = pending.pop();
    const canonical = realpathSync(directory);
    if (seen.has(canonical)) continue;
    seen.add(canonical);
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const path = join(directory, entry.name);
      if (entry.isDirectory() && !entry.isSymbolicLink()) pending.push(path);
      else files.push(path);
    }
  }
  return files;
}

function artifact(root, pattern) {
  const normalized = pattern.replaceAll("\\", "/");
  if (!normalized.includes("*")) {
    const path = resolve(root, normalized);
    return existsSync(path) && statSync(path).isFile() ? path : null;
  }
  const expression = new RegExp(`^${normalized.split("*").map((part) => part.replace(/[.+?^${}()|[\\]\\]/g, "\\$&")).join(".*")}$`, "i");
  const staticPrefix = normalized.slice(0, normalized.indexOf("*"));
  const prefixSlash = staticPrefix.lastIndexOf("/");
  const searchRoot = resolve(root, prefixSlash >= 0 ? staticPrefix.slice(0, prefixSlash) : ".");
  return allFiles(searchRoot).find((path) => expression.test(relative(root, path).replaceAll("\\", "/"))) ?? null;
}

function extension(language) {
  return ({ rust: ".rs", python: ".py", typescript: ".ts", go: ".go", java: ".java", csharp: ".cs", ruby: ".rb", dart: ".dart", php: ".php" })[language];
}

function compile(language, file, cwd, packageArtifact, environment = {}) {
  switch (language) {
    case "rust": {
      // Compile the snippet as a consumer crate against the installed package
      // artifact so API drift is caught.
      const source = readFileSync(file, "utf8");
      mkdirSync(join(cwd, "src"), { recursive: true });
      writeFileSync(join(cwd, "src", "main.rs"), `#[tokio::main]\nasync fn main() -> Result<(), Box<dyn std::error::Error>> {\n${source}\nOk(())\n}\n`);
      return command(cargo, ["check", "--offline", "--manifest-path", join(cwd, "Cargo.toml")], cwd, environment);
    }
    case "python": return command(environment.PYTHON_BIN ?? binaries.python, ["-m", "py_compile", file], cwd, environment);
    case "typescript": return command(binaries.tsc, ["--noEmit", "--strict", "--module", "NodeNext", "--moduleResolution", "NodeNext", "--target", "ES2022", file], cwd, environment);
    case "go": return command(binaries.go, ["test", "."], cwd, environment);
    case "java": return command(binaries.maven, ["--offline", "--batch-mode", "-q", "-DskipTests", "package"], cwd, environment);
    case "csharp": return command(binaries.dotnet, ["build", join(cwd, "GuideSnippet.csproj"), "--nologo", "--verbosity", "quiet"], cwd, environment);
    case "ruby": return command(binaries.ruby, ["-c", file], cwd, environment);
    case "dart": return command(binaries.dart, ["analyze", file], cwd, environment);
    case "php": return command(binaries.php, ["-l", file], cwd, environment);
    default: return { command: "", exitCode: 127, stdout: "", stderr: `unsupported language ${language}` };
  }
}

function execute(language, file, cwd, environment = {}) {
  switch (language) {
    case "python": return command(environment.PYTHON_BIN ?? binaries.python, [file], cwd, environment);
    case "typescript": return command(binaries.bun, [file], cwd, environment);
    case "go": return command(binaries.go, ["run", "."], cwd, environment);
    case "ruby": return command(binaries.ruby, [file], cwd, environment);
    case "dart": return command(binaries.dart, ["run", file], cwd, environment);
    case "php": return command(binaries.php, [file], cwd, environment);
    case "java": {
      const classpathFile = join(cwd, "runtime-classpath.txt");
      const classpath = existsSync(classpathFile) ? readFileSync(classpathFile, "utf8").trim() : "";
      const targetClasses = join(cwd, "target", "classes");
      return command(binaries.java, ["-cp", [targetClasses, classpath].filter(Boolean).join(";"), "GuideSnippet"], cwd, environment);
    }
    case "csharp": return command(binaries.dotnet, ["run", "--project", join(cwd, "GuideSnippet.csproj"), "--no-build"], cwd, environment);
    case "rust": return command(cargo, ["run", "--offline", "--manifest-path", join(cwd, "Cargo.toml")], cwd, environment);
    default: return { command: "", exitCode: 125, stdout: "", stderr: `execution is release-only for ${language}` };
  }
}

function prepare(language, packageArtifact, directory) {
  if (!packageArtifact) return { status: "artifact-missing", install: null, environment: {} };

  if (language === "rust" && packageArtifact.endsWith("Cargo.toml")) {
    const packageRoot = resolve(packageArtifact, "..");
    const packageToml = readFileSync(packageArtifact, "utf8");
    const packageName = packageToml.match(/^name\s*=\s*"([^"]+)"/m)?.[1];
    if (!packageName) return { status: "install-failed", install: null, environment: {}, error: "Cargo.toml has no package name" };
    writeFileSync(join(directory, "Cargo.toml"), `[package]\nname = "guide_snippet"\nversion = "0.0.0"\nedition = "2024"\n\n[workspace]\n\n[dependencies]\n${packageName} = { package = "${packageName}", path = "${packageRoot.replaceAll("\\", "/")}" }\nbytes = "1.10.1"\nsha2 = "0.10.9"\ntokio = { version = "1.48.0", features = ["macros", "rt", "rt-multi-thread", "time", "sync"] }\n`);
    return { status: "installed", install: { command: "cargo consumer package", exitCode: 0, stdout: "", stderr: "" }, environment: { CARGO_TARGET_DIR: join(output, "cargo-target") } };
  }

  if (language === "python" && packageArtifact.endsWith(".whl")) {
    const site = join(directory, "site");
    mkdirSync(site, { recursive: true });
    const install = command(binaries.python, ["-m", "pip", "install", "--no-deps", "--no-cache-dir", "--target", site, packageArtifact], directory);
    return {
      status: install.exitCode === 0 ? "installed" : "install-failed",
      install,
      environment: { PYTHON_BIN: binaries.python, PYTHONPATH: [site, process.env.PYTHONPATH].filter(Boolean).join(";") },
    };
  }

  if (language === "go") {
    const moduleRoot = resolve(packageArtifact, "..");
    const module = readFileSync(packageArtifact, "utf8").match(/^module\s+([^\r\n]+)/m)?.[1]?.trim();
    if (!module) return { status: "install-failed", install: null, environment: {}, error: "go.mod has no module declaration" };
    const packageGoMod = readFileSync(packageArtifact, "utf8").replace(/^module\s+[^\r\n]+\r?\n?/m, "");
    writeFileSync(join(directory, "go.mod"), `module guide-snippet\n\n${packageGoMod}\nrequire ${module} v0.0.0\n\nreplace ${module} => ${moduleRoot.replaceAll("\\", "/")}\n`);
    const packageGoSum = join(moduleRoot, "go.sum");
    if (existsSync(packageGoSum)) writeFileSync(join(directory, "go.sum"), readFileSync(packageGoSum));
    return { status: "installed", install: { command: "go mod replace", exitCode: 0, stdout: "", stderr: "" }, environment: { GOPROXY: "off", GOTOOLCHAIN: "local" } };
  }

  if (language === "java") {
    mkdirSync(join(directory, "src", "main", "java"), { recursive: true });
    cpSync(join(directory, "GuideSnippet.java"), join(directory, "src", "main", "java", "GuideSnippet.java"));
    const jar = packageArtifact.replaceAll("\\", "/");
    writeFileSync(join(directory, "pom.xml"), `<?xml version="1.0" encoding="UTF-8"?><project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion><groupId>guide</groupId><artifactId>guide-snippet</artifactId><version>0.0.0</version><properties><maven.compiler.release>17</maven.compiler.release><project.build.sourceEncoding>UTF-8</project.build.sourceEncoding></properties><dependencies><dependency><groupId>dev.acyclic</groupId><artifactId>acyclic-sdk-jvm-transport</artifactId><version>0.2.0</version><scope>system</scope><systemPath>${jar}</systemPath></dependency><dependency><groupId>io.grpc</groupId><artifactId>grpc-stub</artifactId><version>1.75.0</version></dependency><dependency><groupId>io.grpc</groupId><artifactId>grpc-protobuf</artifactId><version>1.75.0</version></dependency><dependency><groupId>io.grpc</groupId><artifactId>grpc-netty-shaded</artifactId><version>1.75.0</version></dependency><dependency><groupId>com.google.protobuf</groupId><artifactId>protobuf-java</artifactId><version>4.31.1</version></dependency></dependencies><build><plugins><plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-dependency-plugin</artifactId><version>3.7.0</version></plugin></plugins></build></project>`);
    const mavenArgs = ["--offline", "--batch-mode", "-q", "org.apache.maven.plugins:maven-dependency-plugin:3.7.0:build-classpath", "-Dmdep.outputFile=runtime-classpath.txt"];
    if (process.env.SDK_MAVEN_REPO) mavenArgs.push(`-Dmaven.repo.local=${process.env.SDK_MAVEN_REPO}`);
    const classpath = command(binaries.maven, mavenArgs, directory);
    if (classpath.exitCode !== 0) return { status: "install-failed", install: classpath, environment: {} };
    return { status: "installed", install: { command: "Maven consumer project", exitCode: 0, stdout: "", stderr: "" }, environment: {} };
  }

  if (language === "typescript") {
    const packageRoot = resolve(packageArtifact, "..");
    const packageJson = JSON.parse(readFileSync(packageArtifact, "utf8"));
    writeFileSync(join(directory, "package.json"), JSON.stringify({
      name: "guide-snippet",
      private: true,
      type: "module",
      dependencies: {
        [packageJson.name]: `file:${packageRoot.replaceAll("\\", "/")}`,
        "@connectrpc/connect": packageJson.dependencies?.["@connectrpc/connect"] ?? "2.1.1",
        "@connectrpc/connect-node": "2.1.1",
      },
    }, null, 2));
    const install = command(binaries.bun, ["install", "--offline", "--no-progress"], directory);
    const nodePath = [join(directory, "node_modules"), join(repo, "typescript", "node_modules"), join(packageRoot, "node_modules")].join(";");
    return { status: install.exitCode === 0 ? "installed" : "install-failed", install, environment: { NODE_PATH: nodePath, PATH: process.env.PATH } };
  }

  if (language === "csharp") {
    const hint = packageArtifact.replaceAll("\\", "/");
    const dependencyRoot = join(repo, "dotnet", "consumer", "bin", "Debug", "net8.0");
    const dependencyReferences = ["Google.Protobuf", "Grpc.Core.Api", "Grpc.Net.Client", "Grpc.Net.Common"]
      .map((name) => join(dependencyRoot, `${name}.dll`))
      .filter((path) => existsSync(path))
      .map((path) => `<Reference Include="${path.split(/[\\/]/).at(-1).replace(/\.dll$/i, "")}"><HintPath>${path.replaceAll("\\", "/")}</HintPath></Reference>`)
      .join("");
    writeFileSync(join(directory, "GuideSnippet.csproj"), `<Project Sdk="Microsoft.NET.Sdk"><PropertyGroup><OutputType>Exe</OutputType><TargetFramework>net8.0</TargetFramework><ImplicitUsings>enable</ImplicitUsings><Nullable>enable</Nullable></PropertyGroup><ItemGroup><Reference Include="Acyclic.Sdk.Transport"><HintPath>${hint}</HintPath></Reference>${dependencyReferences}</ItemGroup></Project>\n`);
    return { status: "installed", install: { command: "local assembly reference", exitCode: 0, stdout: "", stderr: "" }, environment: {} };
  }

  if (language === "dart") {
    const packageRoot = resolve(packageArtifact, "..");
    writeFileSync(join(directory, "pubspec.yaml"), `name: guide_snippet\nenvironment:\n  sdk: ">=3.8.0 <4.0.0"\ndependencies:\n  acyclic_sdk:\n    path: ${packageRoot.replaceAll("\\", "/")}\n`);
    const install = command(binaries.dart, ["pub", "get", "--offline"], directory);
    return { status: install.exitCode === 0 ? "installed" : "install-failed", install, environment: {} };
  }

  if (language === "php") {
    const packageRoot = resolve(packageArtifact, "..");
    writeFileSync(join(directory, "composer.json"), JSON.stringify({
      require: { "acyclic/sdk": "*" },
      "minimum-stability": "dev",
      "prefer-stable": true,
      repositories: [{ type: "path", url: packageRoot.replaceAll("\\", "/"), options: { symlink: false } }],
    }, null, 2));
    const composerArgs = process.env.SDK_COMPOSER_PHAR
      ? [process.env.SDK_COMPOSER_PHAR, "install", "--no-interaction", "--no-progress"]
      : ["install", "--no-interaction", "--no-progress"];
    const install = command(binaries.composer, composerArgs, directory);
    const ini = join(directory, "runtime.ini");
    const grpcExtension = process.env.SDK_PHP_GRPC_EXTENSION ?? "";
    const protobufExtension = process.env.SDK_PHP_PROTOBUF_EXTENSION ?? "";
    const environment = existsSync(grpcExtension) && existsSync(protobufExtension)
      ? { PHPRC: ini }
      : {};
    if (environment.PHPRC) writeFileSync(ini, `extension=${grpcExtension}\nextension=${protobufExtension}\n`);
    return { status: install.exitCode === 0 ? "installed" : "install-failed", install, environment };
  }

  if (language === "ruby" && packageArtifact.endsWith(".gemspec")) {
    const packageRoot = resolve(packageArtifact, "..");
    const gemPath = join(directory, "acyclic-sdk.gem");
    const gemHome = join(directory, "vendor", "bundle");
    const built = command(binaries.gem, ["build", packageArtifact, "--output", gemPath], directory);
    if (built.exitCode !== 0) return { status: "install-failed", install: built, environment: {} };
    const rubyGemPath = [process.env.SDK_RUBY_GEM_PATH, gemHome, process.env.GEM_PATH].filter(Boolean).join(";");
    const rubyEnv = { GEM_HOME: gemHome, ...(rubyGemPath ? { GEM_PATH: rubyGemPath } : {}) };
    const dependencyCache = process.env.SDK_RUBY_GEM_PATH ? join(process.env.SDK_RUBY_GEM_PATH, "cache") : null;
    const dependencyNames = ["google-protobuf-4.33.0", "grpc-1.82.0", "googleapis-common-protos-types-1.23.0"];
    const dependencyInstalls = [];
    if (dependencyCache && existsSync(dependencyCache)) {
      for (const dependencyName of dependencyNames) {
        const dependencyGem = readdirSync(dependencyCache).find((entry) => entry.startsWith(dependencyName) && entry.endsWith(".gem"));
        if (!dependencyGem) continue;
        const dependencyInstall = command(binaries.gem, ["install", "--local", join(dependencyCache, dependencyGem), "--install-dir", gemHome, "--no-document", "--ignore-dependencies"], directory, rubyEnv);
        dependencyInstalls.push(dependencyInstall);
        if (dependencyInstall.exitCode !== 0) return { status: "install-failed", install: dependencyInstall, environment: {} };
      }
    }
    const install = command(binaries.gem, ["install", "--local", gemPath, "--install-dir", gemHome, "--no-document", "--ignore-dependencies"], directory, rubyEnv);
    return {
      status: install.exitCode === 0 ? "installed" : "install-failed",
      install: { ...install, command: [built.command, ...dependencyInstalls.map((dependency) => dependency.command), install.command].join(" && ") },
      environment: { ...rubyEnv, RUBYLIB: join(packageRoot, "lib") },
    };
  }

  return {
    status: "install-failed",
    install: { command: "unsupported package installation", exitCode: 127, stdout: "", stderr: `no package installer for ${language}` },
    environment: {},
    error: `no package installer for ${language}`,
  };
}

const projectionInput = args.get("--projections");
const manifestCommand = projectionInput
  ? { exitCode: 0, stdout: readFileSync(resolve(repo, projectionInput), "utf8"), stderr: "", command: `file:${projectionInput}` }
  : command(cargo, ["run", "--locked", "--offline", "--manifest-path", join(repo, "rust", "crates", "sdk-examples", "Cargo.toml"), "--example", "guide-projections"]);
if (manifestCommand.exitCode !== 0) {
  console.error(manifestCommand.stderr);
  process.exit(manifestCommand.exitCode);
}
const parsedProjections = JSON.parse(manifestCommand.stdout);
const projections = Array.isArray(parsedProjections) ? parsedProjections : [parsedProjections];
const sourceDigests = [...new Set(projections.map((projection) => projection.source_sha256).filter(Boolean))];
const sourceSha256 = sourceDigests.length === 1 ? sourceDigests[0] : null;
const sourceGitRevisions = [...new Set(projections.map((projection) => projection.source_git_revision).filter(Boolean))];
const sourceGitRevision = sourceGitRevisions.length === 1 ? sourceGitRevisions[0] : null;
// Rust emits the source closure digest in every projection. It is the
// authoritative identity for an archive or dirty checkout; Git HEAD is not
// sufficient because it can describe a different tree than the producer.
const sourceRevision = sourceSha256 ? `source-sha256:${sourceSha256}` : null;
const receipts = [];
const fixture = args.has("--execute") && !process.env.FIXTURE_GRPC_ADDRESS ? await startFixture() : null;
if (args.has("--execute") && !process.env.FIXTURE_GRPC_ADDRESS && !fixture) {
  console.error("--execute requires FIXTURE_GRPC_ADDRESS or a built sdk-examples fixture-server binary");
}
if (fixture) process.on("exit", () => fixture.child.kill());
for (const projection of projections) {
  const directory = join(snippets, projection.scenario_id, projection.language);
  mkdirSync(directory, { recursive: true });
  const fileName = ["java", "csharp"].includes(projection.language)
    ? `GuideSnippet${extension(projection.language)}`
    : `${projection.scenario_id}${extension(projection.language)}`;
  const file = join(directory, fileName);
  writeFileSync(file, projection.code);
  const packageArtifact = artifact(repo, projection.artifact_path);
  const receipt = {
    scenario_id: projection.scenario_id,
    family: projection.family,
    operation: projection.operation,
    language: projection.language,
    mode: projection.mode,
    source: projection.source,
    source_revision: sourceRevision,
    source_git_revision: sourceGitRevision,
    source_sha256: projection.source_sha256 ?? null,
    package_manager: projection.package_manager,
    package_name: projection.package_name,
    artifact_path: projection.artifact_path,
    package_artifact: packageArtifact ? relative(repo, packageArtifact).replaceAll("\\", "/") : null,
    package_sha256: packageArtifact ? createHash("sha256").update(readFileSync(packageArtifact)).digest("hex") : null,
    snippet_path: relative(repo, file).replaceAll("\\", "/"),
    qualification: projection.qualification ?? null,
  };
  const recipe = projection.qualification;
  if (!recipe || !recipe.install || !recipe.compile || !recipe.execute) {
    receipt.status = "install-failed";
    receipt.install = { command: "Rust qualification recipe missing", exitCode: 127, stdout: "", stderr: "projection did not carry a Rust-owned install/compile/execute recipe" };
  } else if (!packageArtifact) {
    receipt.status = "artifact-missing";
  } else {
    const prepared = prepare(projection.language, packageArtifact, directory);
    receipt.install = prepared.install;
    receipt.install_status = prepared.status;
    if (prepared.status !== "installed") {
      receipt.status = prepared.status;
    } else {
      const checked = compile(projection.language, file, directory, packageArtifact, prepared.environment);
      receipt.compile = checked;
      receipt.status = checked.exitCode === 0 ? "compiled" : "compile-failed";
    }
    if (receipt.status === "compiled" && args.has("--execute")) {
      const executionEnvironment = {
        ...(fixture ? fixtureEnvironment(projection.language, fixture.address) : {}),
        ...prepared.environment,
      };
      const ran = execute(projection.language, file, directory, executionEnvironment);
      receipt.execution = ran;
      receipt.status = ran.exitCode === 0 ? "executed" : "execution-failed";
    }
  }
  writeFileSync(join(directory, "receipt.json"), `${JSON.stringify(receipt, null, 2)}\n`);
  receipts.push(receipt);
}

const summary = {
  schema: "acyclic.sdk.guide-projection-qualification.v1",
  source_revision: sourceRevision,
  source_git_revision: sourceGitRevision,
  source_sha256: sourceSha256,
  source: "rust/crates/sdk-examples/src/guide_projections.rs",
  projection_count: receipts.length,
  compiled: receipts.filter((receipt) => receipt.status === "compiled" || receipt.status === "executed").length,
  executed: receipts.filter((receipt) => receipt.status === "executed").length,
  installed: receipts.filter((receipt) => receipt.install_status === "installed").length,
  artifact_missing: receipts.filter((receipt) => receipt.status === "artifact-missing").length,
  install_failed: receipts.filter((receipt) => receipt.status === "install-failed").length,
  failed: receipts.filter((receipt) => receipt.status.endsWith("failed")).length,
  receipts,
};
const hasSuccessfulCommand = (value) => value && value.exitCode === 0 && typeof value.command === "string" && value.command.length > 0 && !/artifact present/i.test(value.command);
const everyReceiptHasEvidence = receipts.every((receipt) =>
  receipt.install_status === "installed" &&
  hasSuccessfulCommand(receipt.install) &&
  hasSuccessfulCommand(receipt.compile) &&
  (!args.has("--execute") || (receipt.status === "executed" && hasSuccessfulCommand(receipt.execution))),
);
summary.evidence_complete = everyReceiptHasEvidence;
writeFileSync(join(output, "qualification.json"), `${JSON.stringify(summary, null, 2)}\n`);
console.log(JSON.stringify({ ...summary, receipts: undefined }, null, 2));
if (args.has("--strict") && (summary.artifact_missing > 0 || summary.failed > 0 || summary.projection_count !== 54 || !sourceRevision || !sourceSha256 || !everyReceiptHasEvidence || projections.some((projection) => projection.source_sha256 !== sourceSha256))) process.exit(1);
