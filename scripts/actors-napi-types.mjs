import { generateNapiTypes } from "./napi-types.mjs";

await generateNapiTypes({
  mode: process.argv[2],
  family: "actors",
  manifest: "rust/crates/actors-napi/Cargo.toml",
  target: "typescript/packages/actors/generated/native/binding.d.ts",
  forceBuildEnvironment: "NAPI_FORCE_BUILD_ACYCLIC_ACTORS_NAPI",
});
