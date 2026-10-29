import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdir, readFile, rm } from "node:fs/promises";
import { dirname, delimiter, isAbsolute, join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { Processes } from "../../../scripts/graphcoder-e2e/process.mjs";
import { environment } from "../../../scripts/graphcoder-e2e/scratch.mjs";
import { hostFixtureDirectory } from "../../../scripts/graphcoder-e2e/fixture-directory.mjs";
import { withOwnedCase } from "../../../scripts/graphcoder-e2e/suite.mjs";
import { stagePlugin } from "./package.mjs";

const recovery = "ACYCLIC_TEST_BINARY=/absolute/acyclic E2E_CODEX=/absolute/codex node --test --test-concurrency=1 plugin/tests/hosts/native-installation.test.mjs";
const source = fileURLToPath(new URL("../../", import.meta.url));
const digest = bytes => createHash("sha256").update(bytes).digest("hex");

test("actual Codex installs and removes the exact native plugin in its fixture profile", async t => {
  const binary = process.env.ACYCLIC_TEST_BINARY, codex = process.env.E2E_CODEX;
  const processes = new Processes();
  let root, step = "admit prebuilt plugin and actual Codex";
  try {
    assert.ok(isAbsolute(binary ?? "") && isAbsolute(codex ?? ""));
    step = "create isolated native installation fixture";
    root = hostFixtureDirectory("native-plugin-installation-");
    const env = environment(join(root, "profile"));
    env.PATH = dirname(codex) + delimiter + env.PATH;
    const packageRoot = join(root, "package");
    step = "stage production plugin package and admitted binary";
    const executable = await stagePlugin(source, binary, packageRoot);
    const run = async (program, args) => {
      const result = await processes.runOwned(program, args, { cwd: root, env, scopeMountRoot: root });
      t.diagnostic(JSON.stringify({ program, args, ...result }));
      assert.equal(result.signal, null);
      return result;
    };
    const receipt = await withOwnedCase({ processes, run: async () => {
      step = "reject a missing marketplace before native plugin registration";
      const missing = join(root, "missing"); await mkdir(missing);
      const rejected = await run(executable, ["install", "codex", "--plugin-root", missing]);
      assert.notEqual(rejected.code, 0);
      assert.match(rejected.stderr, /plugin marketplace manifest is missing/);
      const list = async () => {
        const result = await run(codex, ["plugin", "list", "--json"]);
        assert.equal(result.code, 0);
        return JSON.parse(result.stdout).installed;
      };
      assert.deepEqual(await list(), []);
      step = "install through shipping CLI and actual native plugin manager";
      assert.equal((await run(executable, ["install", "codex", "--plugin-root", packageRoot])).code, 0);
      const installed = await list();
      assert.equal(installed.length, 1);
      assert.equal(installed[0].pluginId, "acyclic@acyclic");
      assert.equal(installed[0].installed, true);
      assert.equal(installed[0].enabled, true);
      step = "verify native plugin cache contains the admitted executable and shipped manifests";
      const cached = join(env.CODEX_HOME, "plugins/cache/acyclic/acyclic", installed[0].version);
      for (const member of [".mcp.json", "hooks/hooks.json", ".codex-plugin/plugin.json", "bin/" + (process.platform === "win32" ? "acyclic.exe" : "acyclic")])
        assert.equal(digest(await readFile(join(cached, member))), digest(await readFile(join(packageRoot, member))), member);
      step = "remove through shipping CLI and verify native manager state";
      assert.equal((await run(executable, ["uninstall", "codex", "--plugin-root", packageRoot])).code, 0);
      assert.deepEqual(await list(), []);
    } });
    assert.equal(receipt.expired, false);
    assert.ok(receipt.seconds < 10);
  } catch (cause) {
    throw new Error(`step=${step}: ${cause.message}; recovery=${recovery}`, { cause });
  } finally {
    try {
      await processes.close();
      if (root) await rm(root, { recursive: true });
    } catch (cause) {
      throw new Error(`step=drain native installation fixture: ${cause.message}; recovery=${recovery}`, { cause });
    }
  }
});
