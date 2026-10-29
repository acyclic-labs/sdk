import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, realpathSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

// One explicit root owns every temporary directory in a Windows invocation.
// Never discover a real user profile or silently fall back to the system drive.
export function scratchDirectory(prefix, env = process.env, platform = process.platform) {
  assert.ok(!env.E2E_WORKSPACE_ROOT && !env.E2E_NTFS_ROOT,
    "use E2E_SCRATCH_ROOT for all acceptance scratch directories");
  const parent = env.E2E_SCRATCH_ROOT ?? (platform === "win32" ? undefined : tmpdir());
  assert.ok(parent && isAbsolute(parent), "E2E_SCRATCH_ROOT must name an absolute scratch directory on Windows");
  const repository = realpathSync(fileURLToPath(new URL("../../", import.meta.url)));
  const admit = canonical => {
    const within = relative(repository, canonical);
    assert.ok(within && (within === ".." || within.startsWith(`..${sep}`) || isAbsolute(within)),
      "acceptance scratch must be outside the source checkout, including through aliases");
  };
  let ancestor = parent;
  while (!existsSync(ancestor)) {
    const next = dirname(ancestor);
    assert.notEqual(next, ancestor, "scratch root must have an existing filesystem ancestor");
    ancestor = next;
  }
  admit(resolve(realpathSync(ancestor), relative(ancestor, parent)));
  mkdirSync(parent, { recursive: true });
  const canonical = realpathSync(parent);
  admit(canonical);
  return mkdtempSync(join(canonical, prefix));
}

// This same profile reaches the shipping program, PTY transport, host-test
// supervisor, mock, Git and helper workers. Temporary files share its lease.
export function environment(settings, inherited = process.env) {
  const temporary = join(settings, "tmp");
  const env = { ...inherited, TERM: "xterm-256color", HOME: settings, USERPROFILE: settings,
    LOCALAPPDATA: join(settings, "AppData/Local"), APPDATA: join(settings, "AppData/Roaming"),
    CODEX_HOME: join(settings, ".codex"),
    CLAUDE_CONFIG_DIR: join(settings, ".claude"), PI_CODING_AGENT_DIR: join(settings, ".pi/agent"),
    XDG_CONFIG_HOME: settings, XDG_DATA_HOME: settings, XDG_CACHE_HOME: settings, XDG_STATE_HOME: settings,
    XDG_RUNTIME_DIR: join(settings, "runtime"),
    ACYCLIC_SIGNING_CUSTODY: "private-file", ACYCLIC_SIGNING_CUSTODY_DIRECTORY: settings,
    TEMP: temporary, TMP: temporary, TMPDIR: temporary };
  for (const key of Object.keys(env)) if (/API_KEY$|AUTH_TOKEN$|^GRAPHCODER_MODEL$/.test(key)) delete env[key];
  for (const path of [settings, env.LOCALAPPDATA, env.APPDATA, env.CODEX_HOME,
    env.CLAUDE_CONFIG_DIR, env.PI_CODING_AGENT_DIR, env.XDG_RUNTIME_DIR, temporary])
    mkdirSync(path, { recursive: true, mode: 0o700 });
  return env;
}

// Cross-language callers receive only the shared profile values, never this
// process's inherited environment or credentials. They retain process custody.
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const [mode, settings, ...extra] = process.argv.slice(2);
  try {
    assert.ok(mode === "profile" && settings && isAbsolute(settings) && !extra.length,
      "profile requires exactly one absolute settings directory");
    console.log(JSON.stringify(environment(settings, {})));
  } catch (error) {
    console.error(`scratch-profile step=admit-or-create failed: ${error.message}; `
      + "recovery command: node scripts/graphcoder-e2e/scratch.mjs profile ABSOLUTE_SETTINGS");
    process.exitCode = 1;
  }
}
