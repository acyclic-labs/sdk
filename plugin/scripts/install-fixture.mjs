// Shared package-installer fixtures use the actual admitted shipping executable.
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { chmodSync, copyFileSync, cpSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { basename, dirname, isAbsolute, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Processes } from "../../scripts/graphcoder-e2e/process.mjs";
import { environment } from "../../scripts/graphcoder-e2e/scratch.mjs";
const plugin = dirname(dirname(fileURLToPath(import.meta.url)));
const require = createRequire(import.meta.url);
const { hostTarget, sha256File } = require("../bin/verify.js");
const target = hostTarget();
const binary = process.env.ACYCLIC_TEST_BINARY;
const recovery = "with ACYCLIC_TEST_BINARY set to the source-qualified shipping executable, run node --test --test-reporter=tap --test-concurrency=1 plugin/scripts/install.test.mjs";
assert.ok(binary && isAbsolute(binary), `step=admit shipping installer fixture; recovery=${recovery}`);
const packageVersion = JSON.parse(readFileSync(join(plugin, "package.json"), "utf8")).version;

const executableName = process.platform === "win32" ? "acyclic.exe" : "acyclic";
const scratch = process.env.ACYCLIC_TEST_TEMP_ROOT
  || join(dirname(plugin), "target", "plugin-installer-tests");
mkdirSync(scratch, { recursive: true });

function scope(prefix) {
  const root = mkdtempSync(join(scratch, prefix));
  const processes = new Processes();
  const inherited = Object.fromEntries(["PATH", "SystemRoot", "ComSpec", "PATHEXT", "E2E_PROCESS_HOST"]
    .filter(key => process.env[key]).map(key => [key, process.env[key]]));
  const env = { ...environment(join(root, "profile"), inherited), NODE_ENV: "test",
    npm_config_cache: join(root, "profile", "npm-cache"), npm_config_update_notifier: "false",
    npm_config_audit: "false", npm_config_fund: "false", npm_config_offline: "true",
    BUN_INSTALL_CACHE_DIR: join(root, "profile", "bun-cache") };
  return { root, env,
    run: (program, args, extraEnvironment = {}, cwd = root) =>
      processes.runOwned(program, args, { cwd, env: { ...env, ...extraEnvironment } }),
    async close() {
      try {
        await processes.close();
        rmSync(root, { recursive: true, force: true });
      } catch (cause) {
        throw new Error(`step=drain installer fixture and retire its files; recovery=${recovery}`, { cause });
      }
    },
  };
}

const admission = scope("admit-");
let nativeVersion;
try {
  const result = await admission.run(binary, ["--version"]);
  assert.equal(result.code, 0, result.stderr);
  nativeVersion = result.stdout.trim();
  assert.equal(nativeVersion, `acyclic ${packageVersion}`);
} finally { await admission.close(); }
const binaryDigest = digest(binary);

function digest(path) {
  return sha256File(path);
}

function fixture() {
  const owner = scope("install-");
  const { root } = owner;
  const bin = join(root, "bin");
  cpSync(join(plugin, "bin"), bin, {
    recursive: true,
    filter: source => !basename(source).startsWith("acyclic."),
  });
  writeFileSync(join(root, "package.json"), JSON.stringify({ version: packageVersion }));
  const source = join(bin, target, executableName);
  mkdirSync(dirname(source), { recursive: true });
  copyFileSync(binary, source);
  chmodSync(source, 0o755);
  assert.equal(digest(source), binaryDigest, `step=admit relocated installer bytes; recovery=${recovery}`);
  writeFileSync(join(bin, "platform-binaries.json"), JSON.stringify({
    version: 1,
    targets: { [target]: { path: `${target}/${executableName}`, sha256: digest(source) } },
  }));
  return { ...owner, bin, source, installed: join(bin, executableName),
    install: (crash, failure, extraEnvironment = {}) => owner.run(process.execPath, [join(bin, "install.js")], {
      ...extraEnvironment,
      ...(crash ? { ACYCLIC_INSTALL_TEST_CRASH: crash } : {}),
      ...(failure ? { ACYCLIC_INSTALL_TEST_FAIL: failure } : {}),
    }),
  };
}

export { plugin, target, binary, packageVersion, nativeVersion, executableName, scratch, digest, fixture, scope };
