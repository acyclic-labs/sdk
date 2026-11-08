#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { copyFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, renameSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const recovery = `node ${JSON.stringify(fileURLToPath(import.meta.url))} ${process.argv.slice(2).map(JSON.stringify).join(" ")}`;
let step = "arguments";
let temporary;

function run(label, command, args, options = {}) {
  step = label;
  const result = spawnSync(command, args, { cwd: root, encoding: "utf8", stdio: "inherit", windowsHide: true, ...options });
  if (result.error || result.status !== 0) {
    throw new Error(`${result.error?.message ?? `exit ${result.status}: ${result.stderr ?? ""}`}; command argv: ${JSON.stringify([command, ...args])}`);
  }
  return result.stdout;
}

try {
  const binaries = [];
  let output;
  let certification;
  let universal = false;
  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i += 1) {
    if (argv[i] === "--require-universal") universal = true;
    else if (["--binary", "--out", "--certification"].includes(argv[i]) && argv[i + 1]) {
      const name = argv[i++];
      if (name === "--binary") binaries.push(argv[i]);
      else if (name === "--out") output = resolve(argv[i]);
      else certification = resolve(argv[i]);
    } else throw new Error(`unknown or incomplete argument: ${argv[i]}`);
  }
  if (!output || !binaries.length) throw new Error("required: --out <release-directory> --binary <target>=<executable> [--binary ...] [--certification <receipts-directory>] [--require-universal]");
  const staging = join(output, "package");
  const plugin = join(staging, "plugin");
  const node = (label, script, args = [], options) => run(label, process.execPath, [join(root, script), ...args], options);
  node("stage plugin", "plugin/scripts/package.mjs", [...binaries.flatMap(value => ["--binary", value]), "--out", staging]);
  if (certification) {
    const receipts = [["linux-fuse", "linux-x64-gnu"], ["macos-nfs", "darwin-arm64"], ["windows-projfs", "win32-x64"]];
    mkdirSync(join(plugin, "certification"), { recursive: true });
    step = "copy native certification";
    for (const [receipt, target] of receipts) {
      copyFileSync(join(certification, `${receipt}.json`), join(plugin, "certification", `native-mount-${target}.json`));
    }
  }
  node("validate plugin", "plugin/scripts/validate-package.mjs", [staging, ...(universal ? ["--require-universal"] : [])]);
  temporary = mkdtempSync(join(tmpdir(), "acyclic-release-"));
  const prefix = join(temporary, "prefix");
  const state = join(temporary, "state");
  const home = join(temporary, "home");
  mkdirSync(home);
  const env = { ...process.env, HOME: home, USERPROFILE: home, XDG_STATE_HOME: state, XDG_CONFIG_HOME: join(home, ".config"), XDG_DATA_HOME: join(home, ".local/share"), XDG_CACHE_HOME: join(home, ".cache"), NODE_ENV: "production", npm_config_cache: join(temporary, "cache"), npm_config_offline: "true" };
  const npm = (label, args, options) => process.platform === "win32"
    ? run(label, process.execPath, [join(dirname(process.execPath), "node_modules/npm/bin/npm-cli.js"), ...args], options)
    : run(label, "npm", args, options);
  const packed = JSON.parse(npm("pack plugin offline", ["pack", "--offline", "--json", "--pack-destination", temporary], { cwd: plugin, env, stdio: ["ignore", "pipe", "inherit"] }));
  if (packed.length !== 1 || packed[0].name !== "@acyclic-labs/plugin") throw new Error("npm pack did not produce the plugin");
  const archive = join(temporary, packed[0].filename);
  npm("install plugin offline into empty prefix", ["install", "--offline", "--global", "--prefix", prefix, "--no-audit", "--no-fund", archive], { env });
  const installed = process.platform === "win32" ? join(prefix, "node_modules", "@acyclic-labs/plugin") : join(prefix, "lib", "node_modules", "@acyclic-labs/plugin");
  const launcher = process.platform === "win32" ? join(installed, "bin/acyclic.exe") : join(prefix, "bin/acyclic-plugin");
  run("smoke installed launcher", launcher, ["--help"], { env });
  step = "verify installed assets";
  for (const name of ["plugin.json", ".agents/plugins/marketplace.json"]) {
    if (!existsSync(join(installed, name))) throw new Error(`missing installed asset: ${name}`);
  }
  if (existsSync(join(installed, "src"))) throw new Error("installed package contains native sources");
  const version = JSON.parse(readFileSync(join(plugin, "package.json"), "utf8")).version;
  const installedVersion = run("verify installed native version", launcher, ["--version"], { env, stdio: ["ignore", "pipe", "inherit"] }).trim();
  if (installedVersion !== `acyclic ${version}`) throw new Error(`installed native version differs from package ${version}: ${installedVersion}`);
  step = "assemble release assets";
  const manifest = JSON.parse(readFileSync(join(plugin, "bin/platform-binaries.json"), "utf8"));
  copyFileSync(join(plugin, "bin/platform-binaries.json"), join(output, "platform-binaries.json"));
  copyFileSync(join(plugin, "bin/verify.js"), join(output, "verify.js"));
  for (const [target, entry] of Object.entries(manifest.targets)) {
    copyFileSync(join(plugin, "bin", entry.path), join(output, `acyclic-${target}${target.startsWith("win32-") ? ".exe" : ""}`));
  }
  run("archive marketplace", "tar", ["--sort=name", "--mtime=UTC 1970-01-01", "--owner=0", "--group=0", "--numeric-owner", "--format=ustar", "-C", staging, "-czf", join(temporary, "acyclic-plugin.tar.gz"), "."]);
  // Publish distributable archives only after the installed executable passes.
  for (const [source, name] of [[archive, "acyclic-plugin-npm.tgz"], [join(temporary, "acyclic-plugin.tar.gz"), "acyclic-plugin.tar.gz"]]) {
    const pending = join(output, `.${name}.tmp`);
    copyFileSync(source, pending);
    renameSync(pending, join(output, name));
  }
  console.log(`Plugin release assembled and installed offline: ${output}`);
} catch (error) {
  console.error(`step=${step}: ${error.message}; recovery: ${recovery}`);
  process.exitCode = 1;
} finally {
  if (temporary) {
    try { rmSync(temporary, { recursive: true, force: true }); }
    catch (error) {
      console.error(`step=clean release smoke prefix: ${error.message}; recovery argv: ${JSON.stringify([process.execPath, "-e", `require('node:fs').rmSync(${JSON.stringify(temporary)}, {recursive:true, force:true})`])}`);
      process.exitCode = 1;
    }
  }
}
