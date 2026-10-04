import { spawnSync } from "node:child_process";
import { resolve } from "node:path";

const root = resolve(import.meta.dirname, "..");
const mode = process.argv[2] ?? "write";
if (mode !== "write" && mode !== "check") throw new Error("expected write or check");

// The Rust generator owns both the contract projection and the package import
// layout.  Keep this file as a small workspace entrypoint only: JavaScript
// must not read Rust metadata and rewrite it with a second renderer.
const generated = spawnSync(
  "cargo",
  [
    "run",
    "--manifest-path",
    "rust/crates/sdk-typescript/Cargo.toml",
    "--quiet",
    "--locked",
    "--offline",
    "--",
    `packages-${mode}`,
    root,
  ],
  { cwd: root, encoding: "utf8" },
);
if (generated.status !== 0) throw new Error(generated.stderr || "Rust TypeScript generation failed");
