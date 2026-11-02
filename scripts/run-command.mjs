import { spawn } from "node:child_process";

/** Run a command with native output backpressure and join its original close event. */
/** @param {string} command @param {string[]} args @param {import("node:child_process").SpawnOptions} [options] */
export async function runCommand(command, args, options = {}) {
  try {
    const child = spawn(command, args, {
      ...options, windowsHide: true, stdio: ["ignore", "inherit", "inherit"],
    });
    let failure;
    const status = await /** @type {Promise<number | null>} */ (new Promise(resolve => {
      child.once("error", error => { failure = error; });
      child.once("close", resolve);
    }));
    if (failure) throw failure;
    return status ?? 1;
  } catch (cause) {
    const windows = process.platform === "win32";
    const quote = value => `'${String(value).replaceAll("'", windows ? "''" : "'\\''")}'`;
    throw new Error(`step=command-run; recovery: ${windows ? "& " : ""}${[command, ...args].map(quote).join(" ")}`, { cause });
  }
}
