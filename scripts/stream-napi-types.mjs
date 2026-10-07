import { generateNapiTypes } from "./napi-types.mjs";

await generateNapiTypes({
  mode: process.argv[2],
  family: "stream",
  manifest: "rust/crates/stream-napi/Cargo.toml",
  target: "typescript/packages/stream/generated/native/binding.d.ts",
  forceBuildEnvironment: "NAPI_FORCE_BUILD_ACYCLIC_STREAM_NAPI",
});
