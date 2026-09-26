import { mkdir } from "node:fs/promises";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const providers = new Set(["filesystem", "stream", "objects", "machines"]);

function bashExecutable() {
  if (process.platform !== "win32") return "bash";
  const cygpath = Bun.which("cygpath");
  if (cygpath) {
    const gitBash = join(dirname(dirname(cygpath)), "bin", "bash.exe");
    if (existsSync(gitBash)) return gitBash;
  }
  return "bash";
}

async function run(command, cwd, stdout = "inherit") {
  const child = Bun.spawn(command, { cwd, stdout, stderr: "inherit" });
  const captured = stdout === "pipe" ? await new Response(child.stdout).text() : undefined;
  if (await child.exited !== 0) throw new Error(`${command[0]} failed`);
  return captured?.trim();
}

function shellPath(path) {
  if (process.platform !== "win32") return path;
  for (const converter of ["cygpath", "wslpath"]) {
    try {
      const conversion = Bun.spawnSync([converter, "-u", path]);
      if (conversion.exitCode === 0) return new TextDecoder().decode(conversion.stdout).trim();
    } catch { /* Try the next shell path converter. */ }
  }
  return path;
}

export async function buildProviderWasm(name) {
  if (!providers.has(name)) throw new Error(`unsupported WASM provider: ${name}`);
  const packageDir = join(root, "typescript", "packages", name);
  const output = join(packageDir, "generated", "wasm");
  const bindgen = await run([bashExecutable(), shellPath(join(root, "scripts", "ensure-wasm-bindgen.sh"))], packageDir, "pipe");
  if (!bindgen) throw new Error("pinned wasm-bindgen resolver returned no executable");
  const cargo = process.platform === "win32" ? "cargo.exe" : "cargo";
  const manifest = join(root, "Cargo.toml");
  const metadata = JSON.parse(await run([cargo, "metadata", "--manifest-path", manifest,
    "--no-deps", "--format-version", "1", "--locked"], packageDir, "pipe"));
  const artifact = `acyclic_${name === "filesystem" ? "fs" : name}_wasm`;
  const wasm = join(metadata.target_directory, "wasm32-unknown-unknown", "wasm-release", `${artifact}.wasm`);
  await run([cargo, "build", "--manifest-path", manifest, "-p", `acyclic-${name === "filesystem" ? "fs" : name}-wasm`,
    "--target", "wasm32-unknown-unknown", "--profile", "wasm-release", "--locked"], packageDir);
  await mkdir(output, { recursive: true });
  await run([bindgen, wasm, "--target", "web", "--out-dir", output, "--out-name", artifact], packageDir);
}
