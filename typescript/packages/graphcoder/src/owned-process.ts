import { spawn, type ChildProcess, type SpawnOptions } from "node:child_process";
import { join } from "node:path";

/**
 * Start a host-owned process in its own process group.
 *
 * The group is an ownership boundary for cleanup only. It does not provide a
 * sandbox or change the command's filesystem permissions. Keeping this small
 * primitive here lets the bridge and the native entrypoint share one cleanup
 * policy without importing repository-only test helpers.
 */
export function spawnOwnedProcess(
  executable: string,
  args: readonly string[],
  options: SpawnOptions,
): ChildProcess {
  return spawn(executable, [...args], {
    ...options,
    detached: true,
    windowsHide: true,
  });
}

/**
 * Request termination of an owned process and its descendants.
 *
 * Windows has no portable process-group signal, so use the system taskkill
 * utility with the exact root PID and tree flag. Unix hosts use the detached
 * process group and escalate once after a short grace period. The caller
 * remains responsible for observing the child's close event.
 */
export async function terminateOwnedProcess(child: ChildProcess, graceMs = 250): Promise<void> {
  if (!Number.isSafeInteger(graceMs) || graceMs < 0) throw new RangeError("process cleanup grace must be a nonnegative safe integer");
  const pid = child.pid;
  if (pid === undefined || !Number.isSafeInteger(pid) || pid <= 0 || !isAlive(child)) return;

  if (process.platform === "win32") {
    await terminateWindowsProcessTree(pid);
    if (isAlive(child)) {
      try { child.kill(); } catch { /* close remains authoritative */ }
    }
    // taskkill reports the root before every inherited stdio handle has
    // drained. Keep the ownership operation alive for one grace interval so
    // callers that await cleanup do not race a descendant's final writes.
    await waitForClose(child, graceMs);
    await new Promise<void>(resolve => setTimeout(resolve, graceMs));
    return;
  }

  signalProcessGroup(pid, "SIGTERM", child);
  await waitForClose(child, graceMs);
  if (isAlive(child)) signalProcessGroup(pid, "SIGKILL", child);
}

function isAlive(child: ChildProcess): boolean {
  return child.exitCode === null && child.signalCode === null;
}

function signalProcessGroup(pid: number, signal: NodeJS.Signals, child: ChildProcess): void {
  try {
    process.kill(-pid, signal);
  } catch {
    try { child.kill(signal); } catch { /* close remains authoritative */ }
  }
}

function waitForClose(child: ChildProcess, timeoutMs: number): Promise<void> {
  if (!isAlive(child) || timeoutMs === 0) return Promise.resolve();
  return new Promise(resolve => {
    let settled = false;
    const finish = (): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve();
    };
    const timer = setTimeout(finish, timeoutMs);
    child.once("close", finish);
    child.once("error", finish);
  });
}

function terminateWindowsProcessTree(pid: number): Promise<void> {
  const systemRoot = process.env.SystemRoot;
  const taskkill = systemRoot === undefined || systemRoot.trim() === ""
    ? "taskkill.exe"
    : join(systemRoot, "System32", "taskkill.exe");
  // Keep the utility's environment minimal. In particular, never forward the
  // bridge runtime's explicit model/provider environment to cleanup tooling.
  const env = systemRoot === undefined ? {} : { SystemRoot: systemRoot };
  return new Promise(resolve => {
    const killer = spawn(taskkill, ["/PID", String(pid), "/T", "/F"], {
      env,
      shell: false,
      stdio: "ignore",
      windowsHide: true,
    });
    killer.once("error", () => resolve());
    killer.once("close", () => resolve());
  });
}
