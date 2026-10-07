import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { copyFile, mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const generator = join(here, "kind-payload-types.mjs");
const sourceRoot = join(here, "../../../../../..");
const fixtureRoot = await mkdtemp(join(tmpdir(), "filesystem-kind-payload-"));

try {
  const fixtureTypes = join(fixtureRoot, "rust/crates/filesystem/src/kernel");
  await mkdir(fixtureTypes, { recursive: true });
  await copyFile(
    join(sourceRoot, "rust/crates/filesystem/src/kernel/types.rs"),
    join(fixtureTypes, "types.rs"),
  );
  await copyFile(
    join(sourceRoot, "rust/crates/filesystem/src/kernel/file_table.rs"),
    join(fixtureTypes, "file_table.rs"),
  );

  const outputPath = join(fixtureRoot, "filesystem-kind-payload-types.d.ts");
  const output = execFileSync(process.execPath, [generator, "--source-root", fixtureRoot, "--output", outputPath], {
    encoding: "utf8",
  });
  const generated = await readFile(outputPath, "utf8");
  assert.match(generated, /FilesystemFileKind = "regular"/);
  assert.match(generated, /"mount-boundary"/);
  assert.match(generated, /FilesystemFilePayloadKind = "inline-regular"/);
  assert.match(generated, /"reparse-point"/);
  assert.match(output, /"source_files"/);

  const typesPath = join(fixtureTypes, "types.rs");
  const types = await readFile(typesPath, "utf8");
  await writeFile(typesPath, types.replace("    MountBoundary,", "    MountBoundary,\n    SyntheticKind,"));
  const mutatedOutputPath = join(fixtureRoot, "filesystem-kind-payload-types-mutated.d.ts");
  execFileSync(process.execPath, [generator, "--source-root", fixtureRoot, "--output", mutatedOutputPath], {
    encoding: "utf8",
  });
  const mutated = await readFile(mutatedOutputPath, "utf8");
  assert.match(mutated, /"synthetic-kind"/);
  assert.notEqual(mutated, generated, "generated union must follow the canonical Rust enum");
  console.log("filesystem kind/payload generator regression tests passed");
} finally {
  await rm(fixtureRoot, { recursive: true, force: true });
}
