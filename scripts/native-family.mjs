import { readFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { projectGenerated } from "./generated-bindings.mjs";

const sourceRoot = fileURLToPath(new URL("../", import.meta.url));

/** Derive native provenance through the same Rust owner used at build admission. */
export function generateNativeFamily(key, root = sourceRoot) {
  const args = ["run", "--offline", "--locked", "--quiet", "-p", "sdk-proto-codegen", "--", "native-family", root, key];
  try {
    return JSON.parse(execFileSync("cargo", args, { cwd: root, encoding: "utf8", windowsHide: true }));
  } catch (cause) {
    const quote = value => `'${value.replaceAll("'", process.platform === "win32" ? "''" : "'\\''")}'`;
    throw new Error(`STEP_FAILED native-family-generation: ${key}; recovery: cargo ${args.map(quote).join(" ")}`, { cause });
  }
}

/** Refresh or check the selected families with the canonical generated-file policy. */
export function generateNativeFamilies(packages = new Set(["actors", "stream"]), check = false) {
  for (const family of ["actors", "stream"]) {
    if (packages.has(family)) projectGenerated(resolve(sourceRoot, "compatibility/native-families", `${family}.json`),
      `${JSON.stringify(generateNativeFamily(family), null, 2)}\n`, check);
  }
}

function immutable(value) {
  if (value !== null && typeof value === "object") {
    for (const child of Object.values(value)) immutable(child);
    Object.freeze(value);
  }
  return value;
}
/** Read the committed native provenance admission baseline; importing this module never runs Cargo. */
export function nativeFamily(key) {
  const value = JSON.parse(readFileSync(new URL(`../compatibility/native-families/${key}.json`, import.meta.url), "utf8"));
  if (value.schema !== "acyclic.native-family.v1" || value.key !== key) throw new Error("Rust native family identity differs");
  return immutable(value);
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  if (!["write", "check"].includes(process.argv[2]) || process.argv.length !== 3) {
    throw new Error("STEP_FAILED native-family-generation: expected write|check; recovery: node scripts/native-family.mjs write");
  }
  generateNativeFamilies(undefined, process.argv[2] === "check");
}
