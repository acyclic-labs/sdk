import assert from "node:assert/strict";
import { chmodSync, copyFileSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { isAbsolute, join, delimiter } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { Processes } from "../../../scripts/graphcoder-e2e/process.mjs";
import { hostFixtureDirectory } from "../../../scripts/graphcoder-e2e/fixture-directory.mjs";
import { environment } from "../../../scripts/graphcoder-e2e/scratch.mjs";

const recovery = "Set ACYCLIC_TEST_BINARY, ACYCLIC_TEST_CODEX_OLD and ACYCLIC_TEST_CODEX_CURRENT to admitted native executables; run node --test --test-reporter=tap --test-concurrency=1 plugin/tests/hosts/version-admission.test.mjs";
function required(name) {
  const value = process.env[name];
  assert.ok(value && isAbsolute(value), `step=admit ${name}; recovery=${recovery}`);
  return value;
}
const binary = required("ACYCLIC_TEST_BINARY");
const hosts = { old: required("ACYCLIC_TEST_CODEX_OLD"), current: required("ACYCLIC_TEST_CODEX_CURRENT") };
const captures = fileURLToPath(new URL("../../../rust/crates/harness/fixtures/hosts/", import.meta.url));

for (const [command, version] of [["install", "old"], ["doctor", "old"], ["doctor", "current"]]) {
  test(`${command} codex ${version === "old" ? "refuses obsolete" : "admits supported"} host before configuration changes`, async t => {
    let fixture;
    const processes = new Processes();
    let step = "create isolated host profile";
    try {
      fixture = hostFixtureDirectory("version-admission-");
      const home = join(fixture, "home"), bin = join(fixture, "bin");
      mkdirSync(bin);
      const env = environment(home, {
        PATH: `${bin}${delimiter}${process.env.PATH ?? ""}`, LANG: "C.UTF-8",
      });
      const codexHome = env.CODEX_HOME;
      const codex = join(bin, process.platform === "win32" ? "codex.exe" : "codex");
      copyFileSync(hosts[version], codex);
      chmodSync(codex, 0o700);
      const config = join(codexHome, "config.toml");
      const original = '# Version admission fixture; contains no login.\nmodel = "fixture-unused"\n';
      writeFileSync(config, original, { flag: "wx" });
      for (const key of ["SystemRoot", "ComSpec", "PATHEXT", "E2E_PROCESS_HOST"]) {
        if (process.env[key]) env[key] = process.env[key];
      }
      const run = (program, args) => processes.runOwned(program, args, { cwd: fixture, env });
      step = "identify native host against captured output";
      const native = await run(codex, ["--version"]);
      const capture = version === "old" ? "codex-0.146.0" : "codex-0.164.0";
      assert.equal(native.code, 0, `step=identify official native host; recovery=${recovery}`);
      assert.equal(native.stdout, readFileSync(join(captures, `${capture}.stdout`), "utf8"));
      assert.equal(native.stderr, readFileSync(join(captures, `${capture}.stderr`), "utf8"));
      // A missing package also proves admission precedes installer path/config work.
      step = `${command} codex version admission`;
      const result = await run(binary, [command, "codex", "--plugin-root", join(fixture, "missing-package")]);
      t.diagnostic(JSON.stringify({ argv: [command, "codex"], version: native.stdout.trim(), ...result }));
      assert.equal(result.signal, null);
      assert.equal(readFileSync(config, "utf8"), original, `step=preserve configuration; recovery=${recovery}`);
      if (version === "old") {
        assert.notEqual(result.code, 0, `step=refuse old host; recovery=${recovery}`);
        assert.match(result.stderr, /requires Codex >= 0\.164\.0/, `step=report minimum host version; recovery=${recovery}`);
      } else {
        assert.equal(result.code, 0, `step=admit supported host; recovery=${recovery}`);
        const report = JSON.parse(result.stdout);
        assert.equal(report.host, "codex");
        assert.equal(report.minimum_codex, "0.164.0");
        assert.deepEqual(report.additional_sandbox_roots, []);
      }
    } catch (cause) {
      throw new Error(`step=${step}: ${cause.message}; recovery=${recovery}`, { cause });
    } finally {
      try {
        await processes.close();
        if (fixture) rmSync(fixture, { recursive: true });
      } catch (cause) {
        throw new Error(`step=drain processes and retire owned fixture; recovery=${recovery}`, { cause });
      }
    }
  });
}
