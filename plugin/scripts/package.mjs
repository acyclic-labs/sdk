#!/usr/bin/env node

import { createRequire } from "node:module";
import { homedir } from "node:os";
import {
  copyFileSync,
  cpSync,
  existsSync,
  mkdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { dirname, isAbsolute, join, parse, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

function fail(message) {
  throw new Error(message);
}

function contains(parent, child) {
  const remainder = relative(parent, child);
  return remainder === "" || (!remainder.startsWith("..") && !isAbsolute(remainder));
}

const require = createRequire(import.meta.url);
const { hostTarget, sha256File } = require("../bin/verify.js");

function targetPath(value) {
  const separator = value.indexOf("=");
  return separator === -1
    ? [hostTarget(), resolve(value)]
    : [value.slice(0, separator), resolve(value.slice(separator + 1))];
}

function parseArguments(argv) {
  const result = { binary: [] };
  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (!["--binary", "--out"].includes(argument)) {
      fail(`unknown argument: ${argument}`);
    }
    const value = argv[index + 1];
    if (!value) fail(`${argument} requires a value`);
    index += 1;
    if (argument === "--binary") result.binary.push(value);
    else result.out = value;
  }
  if (result.binary.length === 0) fail("--binary is required");
  if (!result.out) fail("--out is required");
  return result;
}

const args = parseArguments(process.argv.slice(2));
const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const repository = resolve(root, "..");
const out = resolve(args.out);
const release = join(repository, "release");
if (
  [parse(out).root, repository, root, resolve(process.cwd()), resolve(homedir())]
    .some(path => contains(out, path))
  || (contains(repository, out) && (out === release || !contains(release, out)))
) fail(`refusing unsafe package output: ${out}`);
if (existsSync(out)) {
  try {
    const marketplace = JSON.parse(readFileSync(join(out, "marketplace.json"), "utf8"));
    const packageManifest = JSON.parse(
      readFileSync(join(out, "plugin", "package.json"), "utf8"),
    );
    if (marketplace.name !== "acyclic" || packageManifest.name !== "@acyclic-labs/plugin") {
      fail(`refusing to replace unowned package output: ${out}`);
    }
  } catch (error) {
    if (String(error).includes("refusing to replace")) throw error;
    fail(`refusing to replace unowned package output: ${out}`);
  }
}
rmSync(out, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
const plugin = join(out, "plugin");
mkdirSync(join(plugin, "bin"), { recursive: true });
for (const script of ["acyclic.js", "install.js", "verify.js", "targets.json"]) {
  copyFileSync(join(root, "bin", script), join(plugin, "bin", script));
}
for (const name of ["plugin.json", "package.json", "README.md", "CHANGELOG.md"]) {
  copyFileSync(join(root, name), join(plugin, name));
}
cpSync(join(root, ".codex-plugin"), join(plugin, ".codex-plugin"), { recursive: true });
cpSync(join(root, ".agents"), join(plugin, ".agents"), { recursive: true });
cpSync(join(root, "hooks"), join(plugin, "hooks"), { recursive: true });

const targetSchema = JSON.parse(readFileSync(join(root, "bin", "targets.json"), "utf8"));
if (targetSchema.version !== 1 || !targetSchema.targets || Array.isArray(targetSchema.targets)) {
  fail("unsupported Acyclic target schema");
}
const manifest = { version: 1, targets: {} };
for (const [platformTarget, binary] of args.binary.map(targetPath)) {
  const executable = targetSchema.targets[platformTarget];
  if (!executable) fail(`unsupported binary target: ${platformTarget}`);
  if (manifest.targets[platformTarget]) fail(`duplicate binary target: ${platformTarget}`);
  if (!existsSync(binary)) fail(`binary does not exist: ${binary}`);
  const target = join(plugin, "bin", platformTarget, executable);
  mkdirSync(dirname(target), { recursive: true });
  copyFileSync(binary, target);
  const entry = {
    path: `${platformTarget}/${executable}`,
    sha256: sha256File(target),
  };
  manifest.targets[platformTarget] = entry;
}
writeFileSync(
  join(plugin, "bin", "platform-binaries.json"),
  `${JSON.stringify(manifest, null, 2)}\n`,
);

const marketplace = {
  name: "acyclic",
  interface: { displayName: "Acyclic" },
  plugins: [{
    name: "acyclic",
    source: { source: "local", path: "./plugin" },
    policy: { installation: "AVAILABLE", authentication: "ON_INSTALL" },
    category: "Developer Tools",
  }],
};
writeFileSync(join(out, "marketplace.json"), `${JSON.stringify(marketplace, null, 2)}\n`);
