import { execFileSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { bazelCommand, bazelExecutable } from "./bazel-executables.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
export const PROTO_CODEGEN = "//rust/crates/proto-codegen:sdk-proto-codegen";

/**
 * Run the protobuf code generator, built by Bazel at most once per invocation.
 * @param {string[]} args
 * @param {{stdio?: import("node:child_process").StdioOptions}} [options]
 */
export function generate(args, { stdio = "pipe" } = {}) {
  try {
    return execFileSync(bazelExecutable(root, PROTO_CODEGEN), args,
      { cwd: root, encoding: "utf8", stdio, maxBuffer: Infinity, windowsHide: true });
  } catch (cause) {
    const quote = value => `'${value.replaceAll("'", process.platform === "win32" ? "''" : "'\''")}'`;
    const command = [bazelCommand(), "run", PROTO_CODEGEN, "--", ...args].map(quote).join(" ");
    const recovery = process.platform === "win32"
      ? `Set-Location ${quote(root)}; & ${command}`
      : `cd ${quote(root)} && ${command}`;
    throw Object.assign(new Error(`protobuf generator step ${args[0] ?? "mode selection"} failed; recovery: ${recovery}`, { cause }),
      { code: "PROTO_CODEGEN_FAILED" });
  }
}
