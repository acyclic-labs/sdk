import assert from "node:assert/strict";
import { existsSync, lstatSync, mkdirSync, mkdtempSync, readFileSync, renameSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { hostLogin, linkLogin, releaseLogin } from "./login-link.mjs";

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

// A host refreshing its token writes a new file and renames it over the link.
for (const platform of ["win32", "linux"]) {
  test(`a login the host refreshed during the run replaces the source (${platform} link)`, () => {
    const { root, restore } = fixture();
    try {
      const login = hostLogin("codex", join(root, "fixture", "home"), { CODEX_HOME: "config" });
      linkLogin(login, platform);
      writeFileSync(`${login.link}.tmp`, "rotated-marker");
      renameSync(`${login.link}.tmp`, login.link);
      assert.equal(releaseLogin(login), "restored");
      assert.equal(readFileSync(login.source, "utf8"), "rotated-marker");
      assert.equal(existsSync(login.link), false);
    } finally {
      restore();
    }
  });

  test(`a refreshed login on another filesystem replaces the source (${platform} link)`, () => {
    const { root, restore } = fixture();
    try {
      const login = hostLogin("codex", join(root, "fixture", "home"), { CODEX_HOME: "config" });
      linkLogin(login, platform);
      writeFileSync(`${login.link}.tmp`, "rotated-marker");
      renameSync(`${login.link}.tmp`, login.link);
      const crossDevice = () => {
        throw Object.assign(new Error("cross-device link not permitted"), { code: "EXDEV" });
      };
      assert.equal(releaseLogin(login, crossDevice), "restored");
      assert.equal(readFileSync(login.source, "utf8"), "rotated-marker");
      assert.equal(existsSync(login.link), false);
      assert.equal(existsSync(`${login.source}.acyclic-restore`), false);
    } finally {
      restore();
    }
  });

  test(`an untouched link is only removed (${platform} link)`, () => {
    const { root, restore } = fixture();
    try {
      const login = hostLogin("codex", join(root, "fixture", "home"), { CODEX_HOME: "config" });
      linkLogin(login, platform);
      assert.equal(releaseLogin(login), "unlinked");
      assert.equal(readFileSync(login.source, "utf8"), "fixture-marker");
      assert.equal(lstatSync(login.source).isFile(), true);
      assert.equal(existsSync(login.link), false);
    } finally {
      restore();
    }
  });
}

// Negative control: removing the replaced file, as the run did before, loses
// the only valid login and leaves the revoked one in place.
test("removing a refreshed login leaves the revoked one behind", () => {
  const { root, restore } = fixture();
  try {
    const login = hostLogin("codex", join(root, "fixture", "home"), { CODEX_HOME: "config" });
    linkLogin(login, "win32");
    writeFileSync(`${login.link}.tmp`, "rotated-marker");
    renameSync(`${login.link}.tmp`, login.link);
    rmSync(login.link, { force: true });
    assert.equal(readFileSync(login.source, "utf8"), "fixture-marker");
  } finally {
    restore();
  }
});
