import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { isAbsolute, join } from "node:path";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { discoverAndLoadExtensions } from "@earendil-works/pi-coding-agent";

const execute = promisify(execFile);

test("public Pi installer pins a loadable extension and restores owned settings", async () => {
  const requestedBinary = process.env.ACYCLIC_BINARY;
  assert.ok(requestedBinary && isAbsolute(requestedBinary), "ACYCLIC_BINARY must name the built absolute CLI path");
  const binary = realpathSync(requestedBinary);
  const directory = mkdtempSync(fileURLToPath(new URL(".pi-installation-", import.meta.url)));
  let drain: (() => Promise<void>) | undefined;
  try {
    const project = join(directory, "project");
    const agent = join(directory, "agent");
    const userSettings = join(agent, "settings.json");
    const projectSettings = join(project, ".pi", "settings.json");
    mkdirSync(agent, { recursive: true });
    mkdirSync(join(project, ".pi"), { recursive: true });
    const prior = { theme: "dark", extensions: ["./foreign-extension.ts"] };
    for (const settings of [userSettings, projectSettings]) {
      writeFileSync(settings, JSON.stringify(prior));
    }
    const environment: NodeJS.ProcessEnv = {
      HOME: directory, USERPROFILE: directory, PI_CODING_AGENT_DIR: agent,
      LOCALAPPDATA: join(directory, "local"), APPDATA: join(directory, "config"),
      XDG_STATE_HOME: join(directory, "state"), XDG_CONFIG_HOME: join(directory, "config"),
    };
    for (const name of ["SystemRoot", "TEMP", "TMP"]) {
      if (process.env[name]) environment[name] = process.env[name];
    }
    const command = (...args: string[]) => {
      const invocation = execute(binary, args, {
        cwd: project, env: environment, windowsHide: true,
        timeout: 30_000, maxBuffer: 1024 * 1024, encoding: "utf8",
      });
      invocation.child.stdin?.end();
      return invocation;
    };
    const settingsAt = (path: string) => JSON.parse(readFileSync(path, "utf8"));
    const doctor = async () => {
      // Doctor cold-starts only the service bound to this fixture's data root.
      drain = async () => {
        await command("__service-drain");
        const status = JSON.parse((await command("__service-status")).stdout);
        assert.equal(status.reachableIdentity, null);
        assert.equal(status.lockAcquirable, true);
      };
      let stdout: string;
      try {
        ({ stdout } = await command("doctor", "--json"));
      } catch (error) {
        assert.ok(error instanceof Error && "code" in error && "stdout" in error);
        assert.equal(error.code, 1);
        stdout = String(error.stdout);
      }
      const report = JSON.parse(stdout);
      assert.equal(report.schemaVersion, 2);
      assert.equal(report.capabilities.processConfinement.qualified, false);
      const pi = report.checks.find((check: { name: string }) => check.name === "pi-install");
      assert.ok(pi, "doctor must include Pi installation disposition");
      return pi;
    };
    for (const args of [["install", "pi"], ["install", "pi", "--project"]]) {
      await command(...args);
      const settings = args.includes("--project") ? projectSettings : userSettings;
      const installed = settingsAt(settings);
      assert.equal(installed.theme, prior.theme);
      assert.equal(installed.extensions.length, 2);
      assert.equal(installed.extensions[0], prior.extensions[0]);
      const extension = installed.extensions[1];
      assert.ok(isAbsolute(extension));
      assert.ok(readFileSync(extension, "utf8").includes(JSON.stringify(binary)));
      const loaded = await discoverAndLoadExtensions([extension], project, agent);
      assert.deepEqual(loaded.errors, []);
      assert.equal(loaded.extensions.length, 1);
      assert.ok(loaded.extensions[0].commands.has("acyclic-doctor"));
      await command(...args);
      assert.deepEqual(settingsAt(settings), installed);
    }
    const configured = await doctor();
    assert.equal(configured.status, "warn");
    assert.match(configured.detail, /recursive execution and platform qualification pending/);

    const installed = settingsAt(userSettings);
    const extension = installed.extensions[1];
    const source = readFileSync(extension);
    writeFileSync(extension, "corrupted extension");
    const corrupted = await doctor();
    assert.equal(corrupted.status, "fail");
    assert.ok(corrupted.detail.includes(extension));
    assert.match(corrupted.detail, /differs from its content identity/);
    rmSync(extension);
    const missing = await doctor();
    assert.equal(missing.status, "fail");
    assert.ok(missing.detail.includes(extension));
    assert.match(missing.detail, /cannot read Pi extension asset/);
    writeFileSync(extension, source);
    const edited = { ...installed, theme: "user-edit" };
    writeFileSync(userSettings, JSON.stringify(edited));
    assert.equal((await doctor()).status, "fail");
    for (const [action, reason] of [
      ["install", /inconsistent; refusing to overwrite/],
      ["uninstall", /modified; leaving them untouched/],
    ] as const) {
      await assert.rejects(() => command(action, "pi"), (error: unknown) => {
        assert.ok(error instanceof Error && "code" in error && "stderr" in error);
        assert.equal(error.code, 1);
        assert.match(String(error.stderr), reason);
        return true;
      });
      assert.deepEqual(settingsAt(userSettings), edited);
    }
    writeFileSync(userSettings, JSON.stringify(installed));

    await command("uninstall", "pi");
    for (const settings of [userSettings, projectSettings]) {
      assert.deepEqual(settingsAt(settings), prior);
    }
    assert.ok(readFileSync(installed.extensions[1], "utf8").length > 0);
    const removed = await doctor();
    assert.equal(removed.status, "warn");
    assert.match(removed.detail, /no owned per-user Pi extension/);
  } finally {
    await drain?.();
    rmSync(directory, { recursive: true, force: true });
  }
});
