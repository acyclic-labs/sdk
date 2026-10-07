import { generateNapiTypes } from "./napi-types.mjs";

const mode = process.argv[2];
await generateNapiTypes({
  mode,
  family: "filesystem",
  manifest: "rust/crates/filesystem-napi/Cargo.toml",
  target: "typescript/packages/filesystem/generated/native/binding.d.ts",
  forceBuildEnvironment: "NAPI_FORCE_BUILD_ACYCLIC_FS_NAPI",
});
