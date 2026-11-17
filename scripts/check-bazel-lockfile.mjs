// The committed MODULE.bazel.lock is the module resolution of the checked-in module files. A stale one
// fails here instead of being rewritten; ACYCLIC_BAZEL selects the Bazel through the shared resolver.
import { execFileSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { bazelCommand } from "./bazel-executables.mjs";

execFileSync(bazelCommand(), ["mod", "deps", "--lockfile_mode=error"], {
  cwd: resolve(dirname(fileURLToPath(import.meta.url)), ".."),
  stdio: "inherit",
});
