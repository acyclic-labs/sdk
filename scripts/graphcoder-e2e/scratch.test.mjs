import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, isAbsolute, join, relative } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { environment, scratchDirectory } from "./scratch.mjs";
import { Processes } from "./process.mjs";

test("rejected missing scratch parents never create source directories through direct paths or aliases", () => {
  const repository = realpathSync(fileURLToPath(new URL("../../", import.meta.url)));
  const root = mkdtempSync(join(tmpdir(), "scratch-missing-contract-"));
  const missing = join(repository, basename(root));
  try {
    const alias = join(root, "source-alias");
    symlinkSync(repository, alias, process.platform === "win32" ? "junction" : "dir");
    for (const source of [repository, alias]) {
      assert.equal(existsSync(missing), false);
      assert.throws(() => scratchDirectory("refused-", {
        E2E_SCRATCH_ROOT: join(source, basename(root), "nested", "scratch"),
      }), /outside the source checkout/);
      assert.equal(existsSync(missing), false, "rejected source paths must have zero filesystem side effects");
    }
    const allowed = join(root, "missing", "outside");
    const owned = scratchDirectory("allowed-", { E2E_SCRATCH_ROOT: allowed });
    assert.equal(dirname(owned), realpathSync(allowed));
  } finally {
    rmSync(missing, { recursive: true, force: true });
    rmSync(root, { recursive: true, force: true });
  }
});

test("scratch roots cannot overlap the source checkout through direct paths or aliases", () => {
  const repository = realpathSync(fileURLToPath(new URL("../../", import.meta.url)));
  const root = mkdtempSync(join(tmpdir(), "scratch-source-contract-"));
  try {
    const alias = join(root, "source-alias");
    symlinkSync(repository, alias, process.platform === "win32" ? "junction" : "dir");
    for (const parent of [repository, join(repository, "scripts"), alias]) {
      let unexpected;
      try {
        assert.throws(() => { unexpected = scratchDirectory("source-overlap-control-", { E2E_SCRATCH_ROOT: parent }); },
          /outside the source checkout/);
      } finally { if (unexpected) rmSync(unexpected, { recursive: true, force: true }); }
    }
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test("fixture descendants cannot inherit host profiles or temporary directories outside their lease", async () => {
  const root = mkdtempSync(join(tmpdir(), "scratch-contract-")), processes = new Processes();
  try {
    const outside = join(root, "unowned"), settings = join(root, "leased", "settings");
    const keys = ["TEMP", "TMP", "TMPDIR", "CODEX_HOME", "CLAUDE_CONFIG_DIR", "PI_CODING_AGENT_DIR", "XDG_RUNTIME_DIR"];
    const inherited = { ...process.env, ...Object.fromEntries(keys.map(key => [key, outside])) };
    const env = environment(settings, inherited);
    for (const key of ["TEMP", "TMP", "TMPDIR"]) assert.equal(env[key], join(settings, "tmp"));
    for (const [key, path] of [["CODEX_HOME", ".codex"], ["CLAUDE_CONFIG_DIR", ".claude"], ["PI_CODING_AGENT_DIR", ".pi/agent"],
      ["XDG_RUNTIME_DIR", "runtime"]])
      assert.equal(env[key], join(settings, path), `${key} must stay inside its fixture`);
    for (const key of ["HOME", "USERPROFILE", "CODEX_HOME", "CLAUDE_CONFIG_DIR", "PI_CODING_AGENT_DIR", "APPDATA", "LOCALAPPDATA", "TEMP", "XDG_RUNTIME_DIR"])
      assert.ok(existsSync(env[key]), `${key} must exist before native host discovery`);
    // Real Node temp discovery and real writes through every inherited host selector.
    const code = `const fs=require('node:fs'),path=require('node:path');
      const roots=[require('node:os').tmpdir(),...${JSON.stringify(keys)}.map(key=>process.env[key])];
      const files=roots.map((root,index)=>{const file=path.join(root,index+'.txt');fs.writeFileSync(file,'leased');return file});
      console.log(JSON.stringify(files))`;
    const child = await processes.run(process.execPath, ["-e", code], { env });
    assert.equal(child.code, 0, child.stderr);
    const files = JSON.parse(child.stdout);
    assert.deepEqual(files, [join(settings, "tmp"), ...keys.map(key => env[key])].map((root, index) => join(root, `${index}.txt`)));
    for (const file of files) assert.equal(readFileSync(file, "utf8"), "leased");
    assert.equal(existsSync(outside), false, "the inherited directory must never be touched");
    for (const key of keys) assert.equal(inherited[key], outside, "the caller's environment stays unchanged");
  } finally { await processes.close(); rmSync(root, { recursive: true, force: true }); }
});

test("the cross-language profile bridge returns only the shared owned environment", async () => {
  const root = mkdtempSync(join(tmpdir(), "scratch-profile-bridge-")), processes = new Processes();
  try {
    const settings = join(root, "profile");
    const program = fileURLToPath(new URL("./scratch.mjs", import.meta.url));
    const result = await processes.run(process.execPath, [program, "profile", settings],
      { env: { ...process.env, PROFILE_BRIDGE_SENTINEL: "must-never-be-returned" } });
    assert.equal(result.code, 0, result.stderr);
    assert.ok(result.stdout.trim(), "shared profile bridge must publish its owned environment");
    const actual = JSON.parse(result.stdout);
    assert.deepEqual(actual, environment(settings, {}));
    assert.equal(actual.PROFILE_BRIDGE_SENTINEL, undefined);
    for (const args of [["profile", "relative-settings"], ["unknown", settings], ["profile", settings, "extra"]]) {
      const refused = await processes.run(process.execPath, [program, ...args], { cwd: root });
      assert.equal(refused.code, 1);
      assert.equal(refused.stdout, "");
      assert.match(refused.stderr, /scratch-profile step=admit-or-create failed:/);
      assert.match(refused.stderr, /recovery command: node scripts\/graphcoder-e2e\/scratch.mjs profile ABSOLUTE_SETTINGS/);
    }
    assert.equal(existsSync(join(root, "relative-settings")), false);
  } finally { await processes.close(); rmSync(root, { recursive: true, force: true }); }
});

test("Windows scratch requires one explicit root and rejects the competing old roots", () => {
  assert.throws(() => scratchDirectory("refused-", {}, "win32"), /E2E_SCRATCH_ROOT/);
  assert.throws(() => scratchDirectory("refused-", { E2E_SCRATCH_ROOT: "relative" }, "win32"), /absolute/);
  const root = mkdtempSync(join(tmpdir(), "scratch-root-contract-"));
  try {
    for (const key of ["E2E_WORKSPACE_ROOT", "E2E_NTFS_ROOT"]) {
      assert.throws(() => scratchDirectory("refused-", { E2E_SCRATCH_ROOT: root, [key]: join(root, "other") }, "win32"), /all acceptance scratch/);
    }
    for (const platform of ["linux", "darwin", "win32"]) {
      const child = scratchDirectory("owned-", { E2E_SCRATCH_ROOT: root }, platform);
      assert.ok(isAbsolute(child));
      assert.ok(!relative(realpathSync(root), child).startsWith(".."));
    }
    const target = join(root, "destination"), alias = join(root, "alias");
    mkdirSync(target);
    symlinkSync(target, alias, process.platform === "win32" ? "junction" : "dir");
    const child = scratchDirectory("canonical-", { E2E_SCRATCH_ROOT: alias });
    assert.equal(dirname(child), realpathSync(target), "scratch ownership uses filesystem identity across aliases");
  } finally { rmSync(root, { recursive: true, force: true }); }
});
