import { spawnSync } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { generatedDescriptors } from "./generated-bindings.mjs";
import { filesystemDescriptorDigestSource } from "./filesystem-descriptor-digest.mjs";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const buf = join(root, "node_modules", ".bin", process.platform === "win32" ? "buf.exe" : "buf");
const run = args => {
  const result = spawnSync(buf, args, { cwd: root, stdio: "inherit" });
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
};

const actors = spawnSync(process.execPath, [join(root, "scripts/generate-actors.mjs"), "rust"], {
  cwd: root,
  stdio: "inherit",
});
if (actors.error) throw actors.error;
if (actors.status !== 0) process.exit(actors.status ?? 1);

const workers = spawnSync("cargo", ["run", "--offline", "--locked", "-p", "acyclic-workers", "--example", "workers-http-routes", "--", join(root, "proto"), join(root, "typescript/packages/workers/src/generated/semantic")], { cwd: root, stdio: "inherit" });
if (workers.error) throw workers.error;
if (workers.status !== 0) process.exit(workers.status ?? 1);

run(["generate"]);
for (const [source, destination] of generatedDescriptors) {
  mkdirSync(dirname(join(root, destination)), { recursive: true });
  run(["build", "--path", source, "-o", join(root, destination)]);
}
writeFileSync(
  join(root, "typescript/packages/filesystem/generated/descriptor-digest.js"),
  filesystemDescriptorDigestSource(root),
);
writeFileSync(
  join(root, "typescript/packages/filesystem/generated/descriptor-digest.d.ts"),
  filesystemDescriptorDigestSource(root, true),
);
await import("./sync-generated.mjs");
