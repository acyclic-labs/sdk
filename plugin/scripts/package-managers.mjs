// Pinned external tools are installed by npm ci before qualification. These
// paths select the native payloads, never their install/download launchers.
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
const directory = dirname(fileURLToPath(import.meta.url));
const { linuxLibc, sha256File } = createRequire(import.meta.url)("../bin/verify.js");
const fixture = join(directory, "fixtures");
const expected = JSON.parse(readFileSync(join(fixture, "package.json"), "utf8")).dependencies;
const recovery = "npm ci --prefix plugin/scripts/fixtures --ignore-scripts --no-audit --no-fund, then with the admitted ACYCLIC_TEST_BINARY run node --test --test-reporter=tap --test-concurrency=1 plugin/scripts/install.test.mjs";

export async function admitManagers(owner, modules = join(fixture, "node_modules")) {
  try {
    const musl = process.platform === "linux" && linuxLibc() === "musl" ? "-musl" : "";
    const bunOS = process.platform === "win32" ? "windows" : process.platform;
    const bunArch = process.arch === "arm64" ? "aarch64" : process.arch;
    const packages = {
      npm: { name: "npm", path: "bin/npm-cli.js", javascript: true },
      pnpm: { name: `@pnpm/exe.${process.platform}-${process.arch}${musl}`, path: process.platform === "win32" ? "pnpm.exe" : "pnpm" },
      bun: { name: `@oven/bun-${bunOS}-${bunArch}${musl}`, path: process.platform === "win32" ? "bin/bun.exe" : "bin/bun" },
    };
    const result = {};
    for (const [name, selected] of Object.entries(packages)) {
      const root = join(modules, selected.name);
      const manifest = JSON.parse(readFileSync(join(root, "package.json"), "utf8"));
      assert.equal(manifest.version, expected[name], `${name}: pinned version mismatch`);
      const path = join(root, selected.path);
      assert.ok(existsSync(path), `${name}: pinned payload is missing`);
      const program = selected.javascript ? process.execPath : path;
      const prefix = selected.javascript ? [path] : [];
      const run = async (scope, args, cwd = scope.root) => {
        const argv = [...prefix, ...args];
        const completed = await scope.run(program, argv, {}, cwd);
        assert.equal(completed.code, 0,
          `step=run pinned ${name}: ${completed.stderr}; recovery argv: ${JSON.stringify([program, ...argv])}`);
        return completed.stdout;
      };
      const version = (await run(owner, ["--version"])).trim();
      assert.equal(version, expected[name], `${name}: actual version output disagrees with the pinned fixture`);
      console.log(`installer-manager ${JSON.stringify({ name, version, path, sha256: sha256File(path) })}`);
      result[name] = { run };
    }
    return result;
  } catch (cause) {
    throw new Error(`step=admit pinned installer package managers: ${cause.message}; recovery=${recovery}`, { cause });
  }
}
