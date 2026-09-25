import { spawnSync } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const packages = ["filesystem", "stream", "objects", "machines", "inference", "harness"];

for (const name of packages) {
  const result = spawnSync(process.execPath, ["run", "build:wasm"], {
    cwd: join(root, "typescript", "packages", name),
    stdio: "inherit",
  });
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
}
