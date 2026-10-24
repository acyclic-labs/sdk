import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { hostLogin, linkLogin } from "./login-link.mjs";

// A fixture login with a marker in place of real credentials.
function fixture() {
  const root = mkdtempSync(join(tmpdir(), "acyclic-login-link-"));
  mkdirSync(join(root, "config"));
  writeFileSync(join(root, "config", "auth.json"), "fixture-marker");
  const cwd = process.cwd();
  process.chdir(root);
  return { root, restore: () => { process.chdir(cwd); rmSync(root, { recursive: true, force: true }); } };
}

const hosts = [["codex", "CODEX_HOME", "auth.json"], ["claude-code", "CLAUDE_CONFIG_DIR", ".credentials.json"]];
// Both link kinds run on every platform: a hard link (Windows) and a symbolic
// link (everywhere else).
for (const [[host, variable, file], platform] of hosts.flatMap(entry => [[entry, "win32"], [entry, "linux"]])) {
  test(`a relative ${variable} links the ${host} login it names (${platform} link)`, () => {
    const { root, restore } = fixture();
    try {
      if (file !== "auth.json") writeFileSync(join(root, "config", file), "fixture-marker");
      const login = hostLogin(host, join(root, "fixture", "home"), { [variable]: "config" });
      assert.equal(login.source, join(root, "config", file));
      linkLogin(login, platform);
      assert.ok(readFileSync(login.link, "utf8") === "fixture-marker", "the link does not reach the login");
    } finally {
      restore();
    }
  });
}

// Negative control: the same relative path used as a symbolic link target
// resolves from the link's directory and leaves a dangling link.
test("a relative symbolic link target does not reach the login", () => {
  const { root, restore } = fixture();
  try {
    const link = join(root, "fixture", "home", "codex", "auth.json");
    mkdirSync(join(root, "fixture", "home", "codex"), { recursive: true });
    symlinkSync(join("config", "auth.json"), link);
    assert.equal(existsSync(link), false);
  } finally {
    restore();
  }
});
