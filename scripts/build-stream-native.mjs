import { randomUUID } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { NapiCli } from "@napi-rs/cli";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const output = resolve(root, "typescript/packages/stream/generated/native");
const target = process.argv[2] === "--target" ? process.argv[3] : "x86_64-pc-windows-msvc";
if (target !== "x86_64-pc-windows-msvc") {
  throw new Error("Stream native packaging currently qualifies x86_64-pc-windows-msvc only");
}

const packageJson = JSON.parse(await readFile(resolve(root, "typescript/packages/stream/package.json"), "utf8"));
const temporary = await mkdtemp(resolve(tmpdir(), "acyclic-stream-napi-package-"));
const packagePath = resolve(temporary, `${randomUUID()}.json`);
await writeFile(packagePath, JSON.stringify({ ...packageJson, napi: {
  binaryName: "index",
  packageName: "@acyclic-labs/stream",
  targets: [target],
} }));
try {
  const build = await new NapiCli().build({
    cwd: root,
    packageJsonPath: packagePath,
    manifestPath: resolve(root, "rust/crates/stream-napi/Cargo.toml"),
    outputDir: output,
    target,
    targetDir: resolve(root, "target"),
    platform: true,
    jsPackageName: "@acyclic-labs/stream",
    jsBinding: "binding.cjs",
    dts: "binding.d.ts",
    release: true,
  });
  await build.task;
} finally {
  await rm(temporary, { recursive: true, force: true });
}
