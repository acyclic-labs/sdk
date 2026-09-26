import { spawnSync } from "node:child_process";
import { mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { generatedDescriptors } from "./generated-bindings.mjs";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const buf = join(root, "node_modules", ".bin", process.platform === "win32" ? "buf.exe" : "buf");
const run = args => {
  const result = spawnSync(buf, args, { cwd: root, stdio: "inherit" });
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
};

run(["generate"]);
for (const [source, destination] of generatedDescriptors) {
  mkdirSync(dirname(join(root, destination)), { recursive: true });
  run(["build", "--path", source, "-o", join(root, destination)]);
}
await import("./sync-generated.mjs");
