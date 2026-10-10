import { serviceGenerate } from "./generate-actors.mjs";
import { spawnSync } from "node:child_process";
import { copyFileSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { generatedDescriptors, writeChanged } from "./generated-bindings.mjs";
import { filesystemDescriptorDigestSource } from "./filesystem-descriptor-digest.mjs";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
for (const key of ["actors", "workers", "stream", "filesystem"]) {
  const facts = spawnSync("cargo", ["run", "--offline", "--locked", "--quiet", "-p", "sdk-proto-codegen", "--", "native-family", root, key], { cwd: root, encoding: "utf8" });
  if (facts.error) throw facts.error;
  if (facts.status !== 0) throw new Error(facts.stderr || `native-family ${key} generation failed`);
  const output = join(root, "scripts/generated/native-families", `${key}.json`);
  mkdirSync(dirname(output), { recursive: true });
  writeChanged(output, `${JSON.stringify(JSON.parse(facts.stdout), null, 2)}\n`);
}
const buf = join(root, "node_modules", ".bin", process.platform === "win32" ? "buf.exe" : "buf");
const run = (args, capture = false) => {
  const result = spawnSync(buf, args, { cwd: root, stdio: ["inherit", capture ? "pipe" : "inherit", "inherit"], maxBuffer: 1 << 20 });
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
  return result.stdout;
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
run(["format", "-w", join(root, "proto/workers/v1/workers.proto")]);
await serviceGenerate("Workers", join(root, "typescript/packages/workers/src/generated"), join(root, "typescript/packages/workers/src/generated/workers-service.ts"));
copyFileSync(join(root, "typescript/packages/actors/src/generated/readonly.ts"), join(root, "typescript/packages/workers/src/generated/readonly.ts"));

run(["generate"]);
for (const [source, destination] of generatedDescriptors) {
  mkdirSync(dirname(join(root, destination)), { recursive: true });
  writeChanged(join(root, destination), run(["build", "--path", source, "-o", "-"], true));
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
