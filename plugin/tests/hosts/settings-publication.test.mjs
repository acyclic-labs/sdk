import assert from "node:assert/strict";
import { existsSync, mkdirSync, readFileSync, readdirSync, realpathSync, rmSync, statSync, writeFileSync } from "node:fs";
import { isAbsolute, join, toNamespacedPath } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { Processes } from "../../../scripts/graphcoder-e2e/process.mjs";
import { hostFixtureDirectory } from "../../../scripts/graphcoder-e2e/fixture-directory.mjs";
import { environment } from "../../../scripts/graphcoder-e2e/scratch.mjs";

const recovery = "Set ACYCLIC_TEST_BINARY to the admitted shipping executable; run node --test --test-reporter=tap --test-concurrency=1 plugin/tests/hosts/settings-publication.test.mjs";
const binary = process.env.ACYCLIC_TEST_BINARY;
assert.ok(binary && isAbsolute(binary), `step=admit shipping executable; recovery=${recovery}`);
const packageRoot = fileURLToPath(new URL("../../", import.meta.url));

for (const host of ["claude-code", "pi"]) for (const selection of ["settings argument", "native profile"]) {
  test(`${host} CLI installs, preserves foreign settings and uninstalls using ${selection}`, async t => {
    const processes = new Processes();
    let fixture;
    let step = "create isolated settings fixture";
    try {
      fixture = hostFixtureDirectory("settings-publication-");
      const home = join(fixture, "home");
      const profile = join(fixture, "settings with spaces");
      const settings = selection === "native profile" && host === "pi" ? join(profile, "extensions") : profile;
      mkdirSync(settings, { recursive: true });
      const env = environment(home, { PATH: process.env.PATH ?? "", LANG: "C.UTF-8" });
      if (selection === "native profile") {
        env[host === "pi" ? "PI_CODING_AGENT_DIR" : "CLAUDE_CONFIG_DIR"] = profile;
        // A selected native profile owns configuration without a home lookup.
        // The previous installer either ignored the selector or resolved home
        // before considering it; this input refuses both paths.
        delete env.HOME;
        delete env.USERPROFILE;
      }
      for (const key of ["SystemRoot", "ComSpec", "PATHEXT", "E2E_PROCESS_HOST"]) {
        if (process.env[key]) env[key] = process.env[key];
      }
      const path = join(settings, host === "pi" ? "acyclic.ts" : "settings.json");
      if (selection === "native profile") {
        step = "refuse an empty native profile without a default home";
        const refused = await processes.runOwned(binary,
          ["install", host, "--plugin-root", packageRoot], {
            cwd: fixture,
            env: { ...env, [host === "pi" ? "PI_CODING_AGENT_DIR" : "CLAUDE_CONFIG_DIR"]: "" },
          });
        t.diagnostic(JSON.stringify(refused));
        assert.notEqual(refused.code, 0, `step=${step}; recovery=${recovery}`);
        assert.match(refused.stderr, /host home is unset/, `step=${step}; recovery=${recovery}`);
      }
      const foreign = {
        permissions: { allow: ["Read"], deny: ["Bash(rm *)"] }, fixtureMarker: "preserve",
        hooks: { Stop: [{ hooks: [{ type: "command", command: "foreign-fixture-hook" }] }] },
      };
      if (host === "claude-code") writeFileSync(path, JSON.stringify(foreign), { flag: "wx", mode: 0o600 });
      const invoke = async command => {
        step = `${command} ${host} via shipping CLI`;
        const argv = [command, host, "--plugin-root", packageRoot,
          ...(selection === "settings argument" ? ["--settings", path] : [])];
        const result = await processes.runOwned(binary, argv, { cwd: fixture, env });
        t.diagnostic(JSON.stringify({ argv, ...result }));
        assert.equal(result.signal, null);
        assert.equal(result.code, 0, `step=${step}; recovery=${recovery}`);
      };
      await invoke("install");
      const installed = readFileSync(path, "utf8");
      // Unix exposes the private-file contract as mode bits. Windows privacy
      // is checked by the canonical handle/DACL unit tests, never chmod here.
      if (process.platform !== "win32") assert.equal(statSync(path).mode & 0o777, 0o600,
        `step=private settings publication; recovery=${recovery}`);
      if (host === "pi") {
        assert.equal(installed, readFileSync(join(packageRoot, "pi/index.ts"), "utf8")
          .replaceAll('"__ACYCLIC_EXECUTABLE__"', JSON.stringify(toNamespacedPath(realpathSync(binary)))));
      } else {
        const document = JSON.parse(installed);
        assert.deepEqual(document.permissions, foreign.permissions);
        assert.equal(document.fixtureMarker, foreign.fixtureMarker);
        assert.deepEqual(Object.keys(document.hooks).sort(), ["SessionEnd", "SessionStart", "Stop"]);
        assert.deepEqual(document.hooks.Stop[0], foreign.hooks.Stop[0]);
      }
      await invoke("install");
      assert.equal(readFileSync(path, "utf8"), installed);
      assert.deepEqual(readdirSync(settings), [host === "pi" ? "acyclic.ts" : "settings.json"]);
      await invoke("uninstall");
      if (host === "pi") {
        assert.equal(existsSync(path), false);
        assert.deepEqual(readdirSync(settings), []);
      } else {
        const document = JSON.parse(readFileSync(path, "utf8"));
        assert.deepEqual(document, foreign);
        if (process.platform !== "win32") assert.equal(statSync(path).mode & 0o777, 0o600);
        assert.deepEqual(readdirSync(settings), ["settings.json"]);
      }
    } catch (cause) {
      throw new Error(`step=${step}: ${cause.message}; recovery=${recovery}`, { cause });
    } finally {
      try {
        await processes.close();
        if (fixture) rmSync(fixture, { recursive: true });
      } catch (cause) {
        throw new Error(`step=drain processes and retire settings fixture; recovery=${recovery}`, { cause });
      }
    }
  });
}
