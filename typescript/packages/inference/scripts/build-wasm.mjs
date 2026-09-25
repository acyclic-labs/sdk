import { existsSync } from "node:fs";
import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const packageDir = fileURLToPath(new URL("../", import.meta.url));
const root = fileURLToPath(new URL("../../../../", import.meta.url));
const output = fileURLToPath(new URL("../generated/wasm/", import.meta.url));
const cygpath = process.platform === "win32" ? Bun.which("cygpath") : undefined;
const gitBash = cygpath?.replace(/cygpath\.exe$/i, "bash.exe");
const bash = gitBash && existsSync(gitBash) ? gitBash : "bash";

async function run(command, stdout = "inherit") {
  const child = Bun.spawn(command, { cwd: packageDir, stdout, stderr: "inherit" });
  const captured = stdout === "pipe" ? await new Response(child.stdout).text() : undefined;
  if (await child.exited !== 0) throw new Error(`${command[0]} failed`);
  return captured?.trim();
}

function shellPath(path) {
  if (process.platform !== "win32") return path;
  const converters = bash === gitBash
    ? [["cygpath", "-u", path]]
    : [["wsl.exe", "wslpath", "-u", path.replaceAll("\\", "/")]];
  for (const converter of converters) {
    try {
      const conversion = Bun.spawnSync(converter);
      if (conversion.exitCode === 0) return new TextDecoder().decode(conversion.stdout).trim();
    } catch { /* Try the next shell path converter. */ }
  }
  return path;
}

const bindgen = await run([bash, shellPath(fileURLToPath(new URL("../../../../scripts/ensure-wasm-bindgen.sh", import.meta.url)))], "pipe");
if (!bindgen) throw new Error("pinned wasm-bindgen resolver returned no executable");
const cargo = process.platform === "win32" ? "cargo.exe" : "cargo";
const manifest = join(root, "Cargo.toml");
const metadata = JSON.parse(await run([cargo, "metadata", "--manifest-path", manifest,
  "--no-deps", "--format-version", "1", "--locked"], "pipe"));
const wasm = join(metadata.target_directory, "wasm32-unknown-unknown", "wasm-release", "acyclic_inference_wasm.wasm");
await run([cargo, "build", "--manifest-path", manifest,
  "-p", "acyclic-inference-wasm", "--target", "wasm32-unknown-unknown", "--profile", "wasm-release", "--locked"]);
await mkdir(output, { recursive: true });
await run([bindgen, wasm, "--target", "web", "--out-dir", output, "--out-name", "acyclic_inference_wasm"]);
