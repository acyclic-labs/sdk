import { createHash } from "node:crypto";
import { accessSync, constants, createReadStream, existsSync, mkdirSync, readFileSync, readdirSync, realpathSync, statSync } from "node:fs";
import { dirname, join, resolve, delimiter } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

export function bindingKey(moduleDigest, runnerIdentity, args, environment, script) {
  return createHash("sha256").update(JSON.stringify([moduleDigest, runnerIdentity, process.version, args, environment, script])).digest("hex");
}

async function digest(path) {
  const hash = createHash("sha256");
  for await (const bytes of createReadStream(path)) hash.update(bytes);
  return hash.digest("hex");
}

export async function runnerIdentity(command) {
  const directories = /[/\\]/u.test(command) ? [""] : (process.env.PATH ?? "").split(delimiter);
  const suffixes = process.platform === "win32" ? ["", ...(process.env.PATHEXT ?? ".EXE;.COM").split(";")] : [""];
  for (const directory of directories) {
    for (const suffix of suffixes) {
      const candidate = resolve(directory, command + suffix);
      try {
        accessSync(candidate, constants.X_OK);
        if (!statSync(candidate).isFile()) continue;
      } catch { continue; }
      const executable = realpathSync(candidate);
      return {executable, digest:await digest(executable)};
    }
  }
  throw new Error(`WASM runner unavailable: ${command}`);
}

export function cachedRunner(cache) {
  return readdirSync(cache).flatMap(name => ["run.cjs", "run.mjs"].map(entry => join(cache, name, entry)))
    .find(path => existsSync(path));
}

/** @param {(command: string, args: string[], options: import("node:child_process").SpawnSyncOptions) => import("node:child_process").SpawnSyncReturns<unknown>} [execute] */
export async function runNodeWasmTests(args, runner = process.env.ACYCLIC_WASM_TEST_RUNNER ?? "wasm-bindgen-test-runner", execute = spawnSync) {
  if (process.env.LLVM_PROFILE_FILE || args.includes("--bench")) {
    const result = execute(runner, args, {stdio:"inherit",windowsHide:true});
    if (result.error) throw result.error;
    return result.status ?? 1;
  }
  const module = resolve(args[0]);
  const identity = await runnerIdentity(runner);
  const environment = Object.entries(process.env).filter(([key]) => /^(WASM_BINDGEN_|NODE_|LLVM_PROFILE_FILE)/u.test(key)).sort();
  const key = bindingKey(await digest(module), identity, [process.cwd(), ...args.slice(1)], environment, readFileSync(fileURLToPath(import.meta.url), "utf8"));
  const cache = join(dirname(module), ".node-wasm-tests", key);
  mkdirSync(cache, {recursive:true});
  const cached = cachedRunner(cache);
  // The upstream runner owns generation and the test harness. Reuse its exact JS.
  const command = cached ? "node" : identity.executable;
  const commandArgs = cached ? ["--expose-gc", ...(process.env.NODE_ARGS ?? "").split(",").filter(Boolean), cached] : args;
  const result = execute(command, commandArgs, {
    stdio:"inherit", windowsHide:true,
    env:{...process.env, TMPDIR:cache, TMP:cache, TEMP:cache, WASM_BINDGEN_KEEP_TEST_BUILD:"1", ...(cached ? {NODE_PATH:[process.env.NODE_PATH,process.cwd(),dirname(cached)].filter(Boolean).join(delimiter)} : {})},
  });
  if (result.error) throw result.error;
  return result.status ?? 1;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  process.exitCode = await runNodeWasmTests(process.argv.slice(2));
}
