import { spawnSync } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { generatedDescriptors, rustAuthorityBufTemplate, rustAuthorityExport } from "./generated-bindings.mjs";
import { filesystemDescriptorDigestSource } from "./filesystem-descriptor-digest.mjs";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const buf = join(root, "node_modules", ".bin", process.platform === "win32" ? "buf.exe" : "buf");
let authorityCargoEnv = { ...process.env };
const run = (args, cwd = root) => {
  const result = spawnSync(buf, args, { cwd, stdio: "inherit", env: authorityCargoEnv });
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
};

const authority = rustAuthorityExport();
authorityCargoEnv = { ...process.env, CARGO_TARGET_DIR: authority.cargoTargetDir };
const authorityInput = authority.inputRoot ?? authority.root;
run(["generate", "--template", rustAuthorityBufTemplate(authority), "--output", root, authorityInput]);
const authorityFor = source => {
  const normalized = source
    .replaceAll("\\", "/")
    .replace(/^proto\//, "")
    .replace(/^rust\/crates\/stream\/proto\//, "");
  return authority.manifest.families.find(family =>
    family.source === normalized || family.source.startsWith(`${normalized}/`));
};
for (const [source, destination] of generatedDescriptors) {
  mkdirSync(dirname(join(root, destination)), { recursive: true });
  const family = authorityFor(source);
  const buildRoot = family ? authorityInput : root;
  const input = family ? family.source : source;
  run(["build", "--path", input, "-o", join(root, destination)], buildRoot);
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
