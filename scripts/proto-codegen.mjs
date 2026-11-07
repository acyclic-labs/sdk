import { execFileSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

function cargoArguments(args, offline) {
  return ["run", ...(offline ? ["--offline"] : []), "--locked", "--quiet", "-p", "sdk-proto-codegen", "--", ...args];
}

/**
 * Use an explicitly supplied producer, or let Cargo prepare it once per invocation.
 * @param {string[]} args
 * @param {{offline?: boolean, stdio?: import("node:child_process").StdioOptions}} [options]
 */
export function generate(args, { offline = false, stdio = "pipe" } = {}) {
  const binary = process.env.SDK_PROTO_CODEGEN;
  const commandArgs = binary === undefined ? cargoArguments(args, offline) : args;
  try {
    return execFileSync(binary ?? "cargo", commandArgs,
      { cwd: root, encoding: "utf8", stdio, maxBuffer: Infinity, windowsHide: true });
  } catch (cause) {
    const quote = value => `'${value.replaceAll("'", process.platform === "win32" ? "''" : "'\\''")}'`;
    const cargoArgs = binary === undefined ? commandArgs : cargoArguments(args, offline);
    const recovery = process.platform === "win32"
      ? `Set-Location ${quote(root)}; & 'cargo' ${cargoArgs.map(quote).join(" ")}`
      : `cd ${quote(root)} && 'cargo' ${cargoArgs.map(quote).join(" ")}`;
    throw Object.assign(new Error(`protobuf generator step ${args[0] ?? "mode selection"} failed; recovery: ${recovery}`, { cause }),
      { code: "PROTO_CODEGEN_FAILED" });
  }
}
