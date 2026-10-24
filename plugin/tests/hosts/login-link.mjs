// Links a host's login file into a fixture home for one live run. The login
// is never read or copied: only the link is made, and the caller removes it.
import { linkSync, mkdirSync, symlinkSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";

/** The login file a host uses, as an absolute path, and where its fixture link goes. */
export function hostLogin(host, home, env = process.env) {
  return host === "codex"
    ? { source: resolve(env.CODEX_HOME || join(homedir(), ".codex"), "auth.json"), link: join(home, "codex", "auth.json") }
    : { source: resolve(env.CLAUDE_CONFIG_DIR || join(homedir(), ".claude"), ".credentials.json"), link: join(home, "claude", ".credentials.json") };
}

/**
 * A symbolic link reaches the login across filesystems (the fixture root is
 * often on a tmpfs /tmp); Windows reserves symbolic links for privileged
 * accounts, so there the login is hard-linked within its volume. The target
 * is absolute: a relative symbolic link target resolves from the link's own
 * directory, not from the caller's.
 */
export function linkLogin({ source, link }, platform = process.platform) {
  mkdirSync(dirname(link), { recursive: true });
  (platform === "win32" ? linkSync : symlinkSync)(resolve(source), link);
}
