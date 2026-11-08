import { spawnSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const quick = process.argv[2] === "--no-compile";
if (process.argv.length > (quick ? 3 : 2)) {
  throw new Error("STEP_FAILED generated-selection: expected --no-compile or no arguments; recovery: node scripts/check-generated.mjs");
}
const commands = quick
  ? [["generate-sdk-readmes.mjs", "check"], ["check-baselines.mjs", "--no-compile"]]
  : [["generate.mjs", "check"], ["check-baselines.mjs"], ["generate-public-rpc-matrix.mjs", "check"]];
for (const args of commands) {
  const result = spawnSync(process.execPath, [resolve(root, "scripts", args[0]), ...args.slice(1)], { cwd: root, stdio: "inherit" });
  if (result.error || result.status !== 0) {
    throw new Error(`STEP_FAILED generated-freshness: ${args.join(" ")}; recovery: bun run generate`, { cause: result.error });
  }
}
