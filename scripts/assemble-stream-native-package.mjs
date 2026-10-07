import { createHash } from "node:crypto";
import { copyFileSync, existsSync, readFileSync } from "node:fs";
import { mkdir, mkdtemp, readdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

function fail(message) {
  throw new Error(message);
}

function run(command, args, options = {}) {
  const npmCli = join(dirname(process.execPath), "node_modules/npm/bin/npm-cli.js");
  const executable = process.platform === "win32" && command === "npm" && existsSync(npmCli) ? process.execPath : command;
  const commandArgs = executable === process.execPath && command === "npm" ? [npmCli, ...args] : args;
  const result = spawnSync(executable, commandArgs, {
    cwd: options.cwd ?? root,
    encoding: "utf8",
    stdio: options.stdio ?? ["ignore", "pipe", "pipe"],
    windowsHide: true,
  });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(" ")} failed: ${(result.stderr || result.stdout || "").trim()}`);
  }
  return result.stdout ?? "";
}

async function findMetadata(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  for (const entry of entries) {
    const path = join(directory, entry.name);
    if (entry.isFile() && entry.name === "native-targets.json") return path;
    if (entry.isDirectory()) {
      const found = await findMetadata(path);
      if (found) return found;
    }
  }
  return undefined;
}

function digest(path) {
  return createHash("sha256").update(readFileSync(path)).digest("hex");
}

async function main() {
  const [typescriptOutput, bundleArgument, sourceSha] = process.argv.slice(2);
  if (!typescriptOutput || !bundleArgument || !/^[0-9a-f]{40,64}$/.test(sourceSha ?? "")) {
    fail("usage: assemble-stream-native-package.mjs TYPESCRIPT_OUTPUT NATIVE_BUNDLE SOURCE_SHA");
  }
  const expectedSource = run("git", ["rev-parse", "HEAD"]).trim();
  if (expectedSource !== sourceSha) fail(`native package source ${sourceSha} differs from checkout ${expectedSource}`);

  const output = resolve(typescriptOutput);
  const bundleArgumentPath = resolve(bundleArgument);
  const metadataPath = await findMetadata(bundleArgumentPath);
  if (!metadataPath) fail(`native bundle has no native-targets.json: ${bundleArgumentPath}`);
  const bundle = dirname(metadataPath);
  const metadata = JSON.parse((await readFile(metadataPath)).toString("utf8"));
  if (metadata.source_revision !== sourceSha) fail("native bundle source revision differs from release source");

  const packageManifest = JSON.parse(await readFile(join(root, "typescript/packages/stream/package.json"), "utf8"));
  const archive = join(output, `acyclic-labs-stream-${packageManifest.version}.tgz`);
  await stat(archive);
  const temporary = await mkdtemp(join(tmpdir(), "acyclic-stream-native-package-"));
  try {
    const unpacked = join(temporary, "unpacked");
    const repacked = join(temporary, "repacked");
    await mkdir(unpacked, { recursive: true });
    await mkdir(repacked, { recursive: true });
    run("tar", ["-xzf", archive, "-C", unpacked]);
    const packageRoot = join(unpacked, "package");
    run(process.execPath, ["scripts/build-stream-native.mjs", "stage", "--bundle", bundle, "--output", join(packageRoot, "generated/native")]);
    const packedName = run("npm", ["pack", "--ignore-scripts", "--pack-destination", repacked, "--silent"], { cwd: packageRoot }).trim();
    if (packedName !== `acyclic-labs-stream-${packageManifest.version}.tgz`) fail(`repacked Stream archive has unexpected name ${packedName}`);
    copyFileSync(join(repacked, packedName), archive);
  } finally {
    await rm(temporary, { recursive: true, force: true });
  }

  const receipt = join(output, "QUALIFICATION.json");
  await rm(receipt, { force: true });
  run(process.execPath, ["scripts/typescript-qualification.mjs", "create", output, sourceSha], { stdio: "inherit" });
  const archives = (await readdir(output)).filter(name => name.endsWith(".tgz")).sort();
  const checksums = archives.map(name => `${digest(join(output, name))}  ${name}`).join("\n") + "\n";
  await writeFile(join(output, "SHA256SUMS"), checksums);
  console.log(JSON.stringify({
    schema: "acyclic.stream.native-package-assembly.v1",
    source_commit: sourceSha,
    archive: `acyclic-labs-stream-${packageManifest.version}.tgz`,
    native_bundle: bundle,
  }));
}

await main();
