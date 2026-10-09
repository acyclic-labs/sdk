import { spawnSync } from "node:child_process";
import { mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { generatedDescriptors, writeChanged } from "./generated-bindings.mjs";
import { filesystemDescriptorDigestSource } from "./filesystem-descriptor-digest.mjs";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const buf = join(root, "node_modules", ".bin", process.platform === "win32" ? "buf.exe" : "buf");
const run = (args, stdout = "inherit") => {
  const result = spawnSync(buf, args, { cwd: root, stdio: ["inherit", stdout, "inherit"], maxBuffer: 1 << 20 });
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
  return result.stdout;
};

run(["generate"]);
for (const [source, destination] of generatedDescriptors) {
  mkdirSync(dirname(join(root, destination)), { recursive: true });
  writeChanged(join(root, destination), run(["build", "--path", source, "-o", "-"], "pipe"));
}
writeChanged(
  join(root, "typescript/packages/filesystem/generated/descriptor-digest.js"),
  filesystemDescriptorDigestSource(root),
);
writeChanged(
  join(root, "typescript/packages/filesystem/generated/descriptor-digest.d.ts"),
  filesystemDescriptorDigestSource(root, true),
);
await import("./sync-generated.mjs");
