import { generate } from "./proto-codegen.mjs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { projectGenerated } from "./generated-bindings.mjs";

const sourceRoot = fileURLToPath(new URL("../", import.meta.url));

/** Derive native provenance through the same Rust owner used at build admission. */
export function generateNativeFamily(key, root = sourceRoot) {
  return JSON.parse(generate(["native-family", root, key]));
}

/** Refresh or check the selected families with the canonical generated-file policy. */
export function generateNativeFamilies(packages = new Set(["actors", "stream"]), check = false) {
  for (const family of ["actors", "stream"]) {
    if (packages.has(family)) projectGenerated(resolve(sourceRoot, "compatibility/native-families", `${family}.json`),
      `${JSON.stringify(generateNativeFamily(family), null, 2)}\n`, check);
  }
}

if (import.meta.main) {
  if (!["write", "check"].includes(process.argv[2]) || process.argv.length !== 3) {
    throw new Error("STEP_FAILED native-family-generation: expected write|check; recovery: node scripts/native-family.mjs write");
  }
  generateNativeFamilies(undefined, process.argv[2] === "check");
}
