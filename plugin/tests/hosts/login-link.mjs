// Links a host's login file into a fixture home for one live run. The login
// is never read or copied: only the link is made, and the caller removes it.
import { copyFileSync, linkSync, lstatSync, mkdirSync, renameSync, rmSync, statSync, symlinkSync } from "node:fs";
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

/**
 * Ends the run's use of the login. A host that refreshes its token replaces
 * the login file instead of writing through the link, and a refresh rotates
 * the token: the one the link reached is then revoked. The replacement is the
 * only valid login, so it is moved back over the source, never read. Across
 * filesystems the kernel copies it beside the source, which is then replaced
 * in one rename. A link that still reaches the source (symbolic, or a hard
 * link to the same file) is only removed.
 */
export function releaseLogin({ source, link }, rename = renameSync) {
  let entry;
  try {
    entry = lstatSync(link);
  } catch {
    return "absent";
  }
  const unchanged = entry.isSymbolicLink() || (() => {
    try {
      const original = statSync(source);
      return original.ino === entry.ino && original.dev === entry.dev;
    } catch {
      return false;
    }
  })();
  if (unchanged) {
    rmSync(link, { force: true });
    return "unlinked";
  }
  try {
    rename(link, source);
  } catch (error) {
    if (error?.code !== "EXDEV") throw error;
    const staged = `${source}.acyclic-restore`;
    copyFileSync(link, staged);
    renameSync(staged, source);
    rmSync(link, { force: true });
  }
  return "restored";
}
