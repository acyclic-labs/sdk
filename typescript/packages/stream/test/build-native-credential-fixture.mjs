import { NapiCli } from "@napi-rs/cli";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

const root = fileURLToPath(new URL("../../../..", import.meta.url));
const result = await new NapiCli().build({
  cwd: root,
  packageJsonPath: resolve(root, "typescript/packages/stream/package.json"),
  manifestPath: resolve(root, "rust/crates/stream-napi/Cargo.toml"),
  outputDir: resolve(root, "typescript/packages/stream/generated/native"),
  targetDir: process.env.CARGO_TARGET_DIR,
  platform: true,
  jsPackageName: "@acyclic-labs/stream",
  jsBinding: "binding.cjs",
  dts: "binding.d.ts",
  release: false,
});
await result.task;
