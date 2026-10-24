import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, realpathSync } from "node:fs";
import { tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";

// One explicit root owns every temporary directory in a Windows invocation.
// Never discover a real user profile or silently fall back to the system drive.
export function scratchDirectory(prefix, env = process.env, platform = process.platform) {
  assert.ok(!env.E2E_WORKSPACE_ROOT && !env.E2E_NTFS_ROOT,
    "use E2E_SCRATCH_ROOT for all acceptance scratch directories");
  const parent = env.E2E_SCRATCH_ROOT ?? (platform === "win32" ? undefined : tmpdir());
  assert.ok(parent && isAbsolute(parent), "E2E_SCRATCH_ROOT must name an absolute scratch directory on Windows");
  mkdirSync(parent, { recursive: true });
  return mkdtempSync(join(realpathSync(parent), prefix));
}

// This same profile reaches the shipping program, PTY transport, host-test
// supervisor, mock, Git and helper workers. Temporary files share its lease.
export function environment(settings, inherited = process.env) {
  const temporary = join(settings, "tmp");
  const env = { ...inherited, TERM: "xterm-256color", HOME: settings, USERPROFILE: settings,
    LOCALAPPDATA: join(settings, "AppData/Local"), APPDATA: join(settings, "AppData/Roaming"),
    CODEX_HOME: join(settings, ".codex"),
    XDG_CONFIG_HOME: settings, XDG_DATA_HOME: settings, XDG_CACHE_HOME: settings, XDG_STATE_HOME: settings,
    TEMP: temporary, TMP: temporary, TMPDIR: temporary };
  for (const key of Object.keys(env)) if (/API_KEY$|^GRAPHCODER_MODEL$|^ANTHROPIC_AUTH_TOKEN$/.test(key)) delete env[key];
  for (const path of [settings, env.LOCALAPPDATA, env.APPDATA, env.CODEX_HOME, temporary])
    mkdirSync(path, { recursive: true });
  return env;
}
