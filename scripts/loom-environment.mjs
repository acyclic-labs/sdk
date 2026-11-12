import { fileURLToPath } from "node:url";

// cfg(loom) selects the SDK's sync shims, not registry runtimes' independent
// model configurations. Cargo propagates RUSTFLAGS to every dependency.
export function loomCompilerEnvironment(env = process.env) {
  const scoped = { ...env };
  for (const [name, separator] of [["RUSTFLAGS", " "], ["CARGO_ENCODED_RUSTFLAGS", "\x1f"]]) {
    if (env[name] === undefined) continue;
    const flags = separator === " " ? env[name].split(/\s+/).filter(Boolean) : env[name].split(separator);
    const retained = [];
    for (let index = 0; index < flags.length; index++) {
      if (flags[index] === "--cfg=loom") continue;
      if (flags[index] === "--cfg" && flags[index + 1] === "loom") { index++; continue; }
      retained.push(flags[index]);
    }
    if (retained.length === flags.length) continue;
    if (retained.length) scoped[name] = retained.join(separator);
    else delete scoped[name]; // Restore workspace rustflags when only Loom was supplied.
  }
  return scoped;
}

export function loomEnvironment(env = process.env) {
  return { ...loomCompilerEnvironment(env), RUSTC_WORKSPACE_WRAPPER: fileURLToPath(new URL(
    process.platform === "win32" ? "./loom-rustc.cmd" : "./loom-rustc.sh", import.meta.url)) };
}
