import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { existsSync, mkdtempSync, mkdirSync, readFileSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { isAbsolute, join } from "node:path";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import { test } from "bun:test";
import { piEnvironment } from "./pi-fixture.ts";
import { discoverAndLoadExtensions } from "@earendil-works/pi-coding-agent";

const execute = promisify(execFile);

test("public Pi installer pins a loadable extension and restores owned settings", async () => {
  const requestedBinary = process.env.ACYCLIC_BINARY;
  assert.ok(requestedBinary && isAbsolute(requestedBinary), "ACYCLIC_BINARY must name the built absolute CLI path");
  const binary = realpathSync(requestedBinary);
  const directory = mkdtempSync(fileURLToPath(new URL(".pi-installation-", import.meta.url)));
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
    const environment = piEnvironment(directory);
    const command = (...args: string[]) => {
      const invocation = execute(binary, args, {
        cwd: project, env: environment, windowsHide: true,
        timeout: 30_000, maxBuffer: 1024 * 1024, encoding: "utf8",
      });
      invocation.child.stdin?.end();
      return invocation;
    };
    const settingsAt = (path: string) => JSON.parse(readFileSync(path, "utf8"));
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
      assert.ok(loaded.extensions[0].handlers.has("session_start"));
      assert.ok(loaded.extensions[0].handlers.has("tool_call"));
      assert.ok(loaded.extensions[0].handlers.has("tool_result"));
      assert.ok(loaded.extensions[0].handlers.has("session_shutdown"));
      await command(...args);
      assert.deepEqual(settingsAt(settings), installed);
    }
    const installed = settingsAt(userSettings);
    const extension = installed.extensions[1];
    const source = readFileSync(extension);
    writeFileSync(extension, "corrupted extension");
    await assert.rejects(() => command("install","pi"),error => {
      assert.ok(error instanceof Error && "stderr" in error);
      assert.ok(String(error.stderr).includes(extension));
      return true;
    });
    assert.equal(readFileSync(extension,"utf8"),"corrupted extension");
    rmSync(extension);
    assert.equal(existsSync(extension),false);
    await command("install","pi");
    assert.deepEqual(readFileSync(extension),source,"reinstallation repairs a missing immutable asset");
    assert.deepEqual(settingsAt(userSettings),installed);
    const edited = { ...installed, theme: "user-edit" };
    writeFileSync(userSettings, JSON.stringify(edited));
    for (const action of ["install", "uninstall"]) {
      await assert.rejects(() => command(action, "pi"), (error: unknown) => {
        assert.ok(error instanceof Error && "code" in error);
        assert.equal(error.code, 1);
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
    assert.equal(existsSync(join(directory,"owner")),false,"installation never starts a provider owner");
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
