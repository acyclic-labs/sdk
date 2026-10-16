import { execFileSync } from "node:child_process";
import { resolve } from "node:path";

/**
 * @param {string} root
 * @param {(command: string, args: string[], options: { cwd: string, stdio: 'inherit', env: NodeJS.ProcessEnv }) => unknown} [run]
 */
export function fetchLocked(root, run = execFileSync) {
  run("cargo", ["fetch", "--locked", "--manifest-path", resolve(root, "Cargo.toml")], {
    cwd: root,
    stdio: "inherit",
    env: { ...process.env, CARGO_NET_OFFLINE: "false" },
  });
}
