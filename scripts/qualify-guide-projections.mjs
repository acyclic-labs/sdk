#!/usr/bin/env node

import { createHash } from "node:crypto";
import { cpSync, existsSync, mkdirSync, readFileSync, readdirSync, statSync, writeFileSync } from "node:fs";
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
  go: process.env.SDK_GO_BIN ?? process.env.GO_BIN ?? "go",
  maven: process.env.SDK_MAVEN_BIN ?? process.env.MAVEN_BIN ?? "mvn",
  dotnet: process.env.SDK_DOTNET_BIN ?? process.env.DOTNET_BIN ?? "dotnet",
  ruby: process.env.SDK_RUBY_BIN ?? process.env.RUBY_BIN ?? "ruby",
  gem: process.env.SDK_GEM_BIN ?? process.env.GEM_BIN ?? "gem",
  dart: process.env.SDK_DART_BIN ?? process.env.DART_BIN ?? "dart",
  php: process.env.SDK_PHP_BIN ?? process.env.PHP_BIN ?? "php",
  composer: process.env.SDK_COMPOSER_BIN ?? process.env.COMPOSER_BIN ?? "composer",
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
  const result = spawnSync(name, commandArgs, {
    cwd,
    encoding: "utf8",
    windowsHide: true,
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
  const child = spawn(binary, ["--port", "0", "--grpc-port", "0", "--max-requests", "128"], {
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
  child.unref();
  return { child, address };
}

function fixtureEnvironment(language, address) {
  const normalized = address.replace(/^https?:\/\//, "");
  const urlLanguages = new Set(["typescript", "csharp"]);
  return { FIXTURE_GRPC_ADDRESS: urlLanguages.has(language) ? address : normalized };
}

function allFiles(root) {
  const files = [];
  if (!existsSync(root)) return files;
  for (const entry of readdirSync(root, { withFileTypes: true })) {
    const path = join(root, entry.name);
    if (entry.isDirectory()) files.push(...allFiles(path));
    else files.push(path);
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
  return allFiles(root).find((path) => expression.test(relative(root, path).replaceAll("\\", "/"))) ?? null;
}

function extension(language) {
  return ({ rust: ".rs", python: ".py", typescript: ".ts", go: ".go", java: ".java", csharp: ".cs", ruby: ".rb", dart: ".dart", php: ".php" })[language];
}

function compile(language, file, cwd, packageArtifact, environment = {}) {
  switch (language) {
    case "rust": return command(binaries.rustfmt, ["--check", file], cwd, environment);
    case "python": return command(environment.PYTHON_BIN ?? binaries.python, ["-m", "py_compile", file], cwd, environment);
    case "typescript": return command(binaries.bun, ["build", file, "--no-bundle", "--target=node"], cwd, environment);
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
    case "java": return command(binaries.maven, ["--offline", "--batch-mode", "-q", "exec:java", "-Dexec.mainClass=GuideSnippet"], cwd, environment);
    case "csharp": return command(binaries.dotnet, ["run", "--project", join(cwd, "GuideSnippet.csproj"), "--no-build"], cwd, environment);
    default: return { command: "", exitCode: 125, stdout: "", stderr: `execution is release-only for ${language}` };
  }
}

function prepare(language, packageArtifact, directory) {
  if (!packageArtifact) return { status: "artifact-missing", install: null, environment: {} };

  if (language === "python" && packageArtifact.endsWith(".whl")) {
    const venv = join(directory, ".venv");
    const venvPython = join(venv, "Scripts", "python.exe");
    const created = command(binaries.python, ["-m", "venv", venv], directory);
    if (created.exitCode !== 0) return { status: "install-failed", install: created, environment: {} };
    const site = join(directory, "site");
    mkdirSync(site, { recursive: true });
    const install = command(venvPython, ["-m", "pip", "install", "--target", site, packageArtifact], directory);
    return {
      status: install.exitCode === 0 ? "installed" : "install-failed",
      install: { ...install, command: `${created.command} && ${install.command}` },
      environment: { PYTHON_BIN: venvPython, PYTHONPATH: [site, process.env.PYTHONPATH].filter(Boolean).join(";") },
    };
  }

  if (language === "go") {
    const moduleRoot = resolve(packageArtifact, "..");
    const module = readFileSync(packageArtifact, "utf8").match(/^module\s+([^\r\n]+)/m)?.[1]?.trim();
    if (!module) return { status: "install-failed", install: null, environment: {}, error: "go.mod has no module declaration" };
    writeFileSync(join(directory, "go.mod"), `module guide-snippet\n\ngo 1.27\n\nrequire ${module} v0.0.0\n\nreplace ${module} => ${moduleRoot.replaceAll("\\", "/")}\n`);
    return { status: "installed", install: { command: "go mod replace", exitCode: 0, stdout: "", stderr: "" }, environment: {} };
  }

  if (language === "java") {
    mkdirSync(join(directory, "src", "main", "java"), { recursive: true });
    cpSync(join(directory, "GuideSnippet.java"), join(directory, "src", "main", "java", "GuideSnippet.java"));
    const jar = packageArtifact.replaceAll("\\", "/");
    writeFileSync(join(directory, "pom.xml"), `<?xml version="1.0" encoding="UTF-8"?><project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion><groupId>guide</groupId><artifactId>guide-snippet</artifactId><version>0.0.0</version><properties><maven.compiler.release>17</maven.compiler.release><project.build.sourceEncoding>UTF-8</project.build.sourceEncoding></properties><dependencies><dependency><groupId>dev.acyclic</groupId><artifactId>acyclic-sdk-jvm-transport</artifactId><version>0.2.0</version><scope>system</scope><systemPath>${jar}</systemPath></dependency><dependency><groupId>io.grpc</groupId><artifactId>grpc-stub</artifactId><version>1.75.0</version></dependency><dependency><groupId>io.grpc</groupId><artifactId>grpc-protobuf</artifactId><version>1.75.0</version></dependency><dependency><groupId>com.google.protobuf</groupId><artifactId>protobuf-java</artifactId><version>4.31.1</version></dependency></dependencies><build><plugins><plugin><groupId>org.codehaus.mojo</groupId><artifactId>exec-maven-plugin</artifactId><version>3.5.0</version></plugin></plugins></build></project>`);
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
    const packageDirectory = join(directory, "node_modules", "@acyclic-labs", packageJson.name.split("/")[1]);
    mkdirSync(resolve(packageDirectory, ".."), { recursive: true });
    // A package directory is the installable local artifact in manual and
    // release qualification. Copy it into node_modules to exercise package
    // exports exactly as a consumer does.
    if (!existsSync(packageDirectory)) {
      cpSync(packageRoot, packageDirectory, { recursive: true });
    }
    const install = command(binaries.bun, ["install", "--offline", "--no-progress"], directory);
    const nodePath = [join(directory, "node_modules"), join(repo, "typescript", "node_modules"), join(packageRoot, "node_modules")].join(";");
    return { status: install.exitCode === 0 ? "installed" : "install-failed", install, environment: { NODE_PATH: nodePath } };
  }

  if (language === "csharp") {
    const hint = packageArtifact.replaceAll("\\", "/");
    writeFileSync(join(directory, "GuideSnippet.csproj"), `<Project Sdk="Microsoft.NET.Sdk"><PropertyGroup><OutputType>Exe</OutputType><TargetFramework>net8.0</TargetFramework><ImplicitUsings>enable</ImplicitUsings><Nullable>enable</Nullable></PropertyGroup><ItemGroup><Reference Include="Acyclic.Sdk.Transport"><HintPath>${hint}</HintPath></Reference></ItemGroup></Project>\n`);
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
      repositories: [{ type: "path", url: packageRoot.replaceAll("\\", "/"), options: { symlink: false } }],
    }, null, 2));
    const install = command(binaries.composer, ["install", "--no-interaction", "--no-progress"], directory);
    return { status: install.exitCode === 0 ? "installed" : "install-failed", install, environment: {} };
  }

  if (language === "ruby" && packageArtifact.endsWith(".gemspec")) {
    const packageRoot = resolve(packageArtifact, "..");
    const gemPath = join(directory, "acyclic-sdk.gem");
    const gemHome = join(directory, "vendor", "bundle");
    const built = command(binaries.gem, ["build", packageArtifact, "--output", gemPath], directory);
    if (built.exitCode !== 0) return { status: "install-failed", install: built, environment: {} };
    const install = command(binaries.gem, ["install", "--local", gemPath, "--install-dir", gemHome, "--no-document"], directory, { GEM_HOME: gemHome });
    return {
      status: install.exitCode === 0 ? "installed" : "install-failed",
      install: { ...install, command: `${built.command} && ${install.command}` },
      environment: { GEM_HOME: gemHome, RUBYLIB: join(packageRoot, "lib") },
    };
  }

  return { status: "installed", install: { command: "artifact present", exitCode: 0, stdout: "", stderr: "" }, environment: {} };
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
const revisionCommand = command("git", ["rev-parse", "HEAD"]);
const sourceRevision = revisionCommand.exitCode === 0
  ? revisionCommand.stdout.trim()
  : "working-tree";
const sourceDigests = [...new Set(projections.map((projection) => projection.source_sha256).filter(Boolean))];
const sourceSha256 = sourceDigests.length === 1 ? sourceDigests[0] : null;
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
    source_sha256: projection.source_sha256 ?? null,
    package_manager: projection.package_manager,
    package_name: projection.package_name,
    artifact_path: projection.artifact_path,
    package_artifact: packageArtifact ? relative(repo, packageArtifact).replaceAll("\\", "/") : null,
    package_sha256: packageArtifact ? createHash("sha256").update(readFileSync(packageArtifact)).digest("hex") : null,
    snippet_path: relative(repo, file).replaceAll("\\", "/"),
  };
  if (!packageArtifact) {
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
writeFileSync(join(output, "qualification.json"), `${JSON.stringify(summary, null, 2)}\n`);
console.log(JSON.stringify({ ...summary, receipts: undefined }, null, 2));
if (args.has("--strict") && (summary.artifact_missing > 0 || summary.failed > 0 || summary.projection_count !== 54 || sourceRevision === "working-tree" || !sourceSha256 || projections.some((projection) => projection.source_sha256 !== sourceSha256))) process.exit(1);
