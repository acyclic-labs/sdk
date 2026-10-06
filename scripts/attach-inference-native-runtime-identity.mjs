#!/usr/bin/env node

import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

const args = process.argv.slice(2);
const value = (name) => {
  const index = args.indexOf(name);
  return index < 0 ? undefined : args[index + 1];
};
const packagePath = value("--package-json");
const identityPath = value("--identity");
if (!packagePath || !identityPath) {
  throw new Error("usage: attach-inference-native-runtime-identity.mjs --package-json FILE --identity FILE");
}
const packageJson = JSON.parse(readFileSync(resolve(packagePath), "utf8"));
const identity = JSON.parse(readFileSync(resolve(identityPath), "utf8"));
if (identity.schema !== "acyclic.sdk.inference.native.runtime-source-identity.v1" ||
    !/^sha256:[0-9a-f]{64}$/.test(identity.closure_sha256 ?? "") ||
    !/^sha256:[0-9a-f]{64}$/.test(identity.recipe_sha256 ?? "")) {
  throw new Error("runtime identity is missing its Rust closure and build recipe hashes");
}
if (packageJson.name !== "@acyclic-labs/inference") {
  throw new Error(`expected the Rust-generated Inference package, got ${packageJson.name ?? "missing"}`);
}
packageJson.acyclicGenerated ??= {};
packageJson.acyclicGenerated.nativeRuntime = {
  runtime_source_closure_sha256: identity.closure_sha256,
  runtime_build_recipe_sha256: identity.recipe_sha256,
};
writeFileSync(resolve(packagePath), `${JSON.stringify(packageJson, null, 2)}\n`);
console.log(JSON.stringify(packageJson.acyclicGenerated.nativeRuntime));
