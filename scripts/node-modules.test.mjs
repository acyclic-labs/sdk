import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { chmodSync, cpSync, existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { bazelCommand } from "./bazel-executables.mjs";

function makeDirectoriesWritable(directory) {
  chmodSync(directory, 0o700);
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    if (entry.isDirectory()) makeDirectoriesWritable(join(directory, entry.name));
  }
}

test("declared package files retain package-relative paths in a dependent workspace", () => {
  const root = mkdtempSync(join(tmpdir(), "sdk-node-modules-"));
  const dependency = join(root, "dependency");
  try {
    mkdirSync(join(dependency, "pkg/src"), { recursive: true });
    cpSync(new URL("../node_modules.bzl", import.meta.url), join(dependency, "node_modules.bzl"));
    writeFileSync(join(dependency, "MODULE.bazel"), 'module(name = "dependency", version = "1.0")\n');
    writeFileSync(join(dependency, "pkg/package.json"), '{"name":"@scope/pkg"}\n');
    writeFileSync(join(dependency, "pkg/src/index.ts"), 'export const value = "declared";\n');
    writeFileSync(join(dependency, "BUILD.bazel"), `load(":node_modules.bzl", "node_modules")
filegroup(name = "source", srcs = ["pkg/package.json", "pkg/src/index.ts"])
node_modules(name = "runtime", packages = {":source": "@scope/pkg=pkg"}, visibility = ["//visibility:public"])
`);
    writeFileSync(join(root, "MODULE.bazel"), `module(name = "consumer")
bazel_dep(name = "dependency", version = "1.0")
local_path_override(module_name = "dependency", path = "dependency")
`);
    writeFileSync(join(root, "BUILD.bazel"), "");
    const run = (...args) => execFileSync(bazelCommand(), ["--batch", `--output_base=${join(root, "output")}`, ...args], {
      cwd: root, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"],
    });
    const symlinkPrefix = join(root, "bazel-");
    run("build", `--symlink_prefix=${symlinkPrefix}`, "--remote_download_outputs=toplevel", "@dependency//:runtime");
    const output = run("cquery", "@dependency//:runtime", "--output=files").trim();
    assert.ok(output.startsWith("bazel-out/"), `unexpected output path: ${output}`);
    const outputDirectory = join(`${symlinkPrefix}out`, output.slice("bazel-out/".length));
    assert.deepEqual(readdirSync(outputDirectory, { recursive: true }).map(file => file.replaceAll("\\", "/")).sort(), [
      "@scope", "@scope/pkg", "@scope/pkg/package.json", "@scope/pkg/src", "@scope/pkg/src/index.ts",
    ].sort());
    for (const file of ["package.json", "src/index.ts"]) {
      assert.equal(readFileSync(join(outputDirectory, "@scope/pkg", file), "utf8"), readFileSync(join(dependency, "pkg", file), "utf8"));
    }
  } finally {
    if (existsSync(join(root, "output"))) makeDirectoriesWritable(join(root, "output"));
    rmSync(root, { recursive: true, force: true });
  }
});
