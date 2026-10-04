import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { join } from "node:path";
import { tmpdir } from "node:os";

export function runRustContractGenerator(root, mode, outputDirectory) {
  const result = spawnSync(
    process.env.ACYCLIC_CARGO_BIN || "cargo",
    [
      "run", "--manifest-path", join(root, "rust/crates/sdk-typescript/Cargo.toml"),
      "--bin", "sdk-contracts", "--locked", "--quiet", "--",
      mode, root, outputDirectory ?? root,
    ],
    { cwd: root, encoding: "utf8" },
  );
  if (result.status !== 0) {
    process.stderr.write(result.stdout ?? "");
    process.stderr.write(result.stderr ?? "");
    throw new Error(`Rust contract generator failed with status ${result.status ?? "unknown"}`);
  }
}

export function checkRustContractGenerator(root, mode, files, relativeDirectory) {
  const temporary = mkdtempSync(join(tmpdir(), "acyclic-rust-contracts-"));
  try {
    runRustContractGenerator(root, mode.replace(/-check$/, "-write"), temporary);
    for (const file of files) {
      const generated = join(temporary, relativeDirectory, file);
      const committed = join(root, relativeDirectory, file);
      if (!existsSync(committed) || !readFileSync(generated).equals(readFileSync(committed))) {
        throw new Error(`Rust-generated contract is stale; run bun run generate (${file})`);
      }
    }
  } finally {
    rmSync(temporary, { recursive: true, force: true });
  }
}
