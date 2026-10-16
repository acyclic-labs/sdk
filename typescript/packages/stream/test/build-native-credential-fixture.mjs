import { NapiCli } from "@napi-rs/cli";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

const family = process.argv[2] ?? "stream";
if (!["stream", "actors"].includes(family)) throw new TypeError("unknown credential fixture family");
const root = fileURLToPath(new URL("../../../..", import.meta.url));
const result = await new NapiCli().build({
  cwd: root,
  packageJsonPath: resolve(root, `typescript/packages/${family}/package.json`),
  manifestPath: resolve(root, `rust/crates/${family}-napi/Cargo.toml`),
  outputDir: resolve(root, `typescript/packages/${family}/generated/native`),
  targetDir: process.env.CARGO_TARGET_DIR,
  platform: true,
  jsPackageName: `@acyclic-labs/${family}`,
  jsBinding: "binding.cjs",
  dts: "binding.d.ts",
  release: false,
});
await result.task;
