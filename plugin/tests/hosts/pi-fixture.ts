import { execFile } from "node:child_process";
import { copyFileSync, mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { promisify } from "node:util";
import { fileURLToPath, pathToFileURL } from "node:url";

const execute = promisify(execFile);
const cli = fileURLToPath(new URL(
  "node_modules/@earendil-works/pi-coding-agent/dist/bundle/cli.js", import.meta.url,
));

/** Keep Pi and its native service under the same owned fixture directory. */
export function piEnvironment(directory: string): NodeJS.ProcessEnv {
  const environment: NodeJS.ProcessEnv = {
    HOME: directory, USERPROFILE: directory, PI_CODING_AGENT_DIR: join(directory, "agent"),
    LOCALAPPDATA: join(directory, "local"), APPDATA: join(directory, "config"),
    XDG_STATE_HOME: join(directory, "state"), XDG_CONFIG_HOME: join(directory, "config"),
  };
  for (const name of ["SystemRoot", "TEMP", "TMP"]) {
    if (process.env[name]) environment[name] = process.env[name];
  }
  return environment;
}

/** Invoke the pinned native CLI with bounded output and isolated host settings. */
export async function invokePi(args: string[], cwd: string, directory: string,
  environment: NodeJS.ProcessEnv = piEnvironment(directory)) {
  const invocation = execute(process.execPath, [cli, ...args], {
    cwd, env: environment, windowsHide: true,
    timeout: 60_000, maxBuffer: 1024 * 1024, encoding: "utf8",
  });
  invocation.child.stdin?.end();
  return await invocation;
}

/** Resolve the exact production helper source through the pinned SDK package. */
export async function loadPiContextHelpers() {
  const directory = mkdtempSync(fileURLToPath(new URL(".pi-context-", import.meta.url)));
  try {
    const extension = join(directory, "index.ts");
    copyFileSync(new URL("../../pi/index.ts", import.meta.url), extension);
    return await import(pathToFileURL(extension).href) as typeof import("../../pi/index.ts");
  } finally {
    rmSync(directory, { recursive: true });
  }
}
