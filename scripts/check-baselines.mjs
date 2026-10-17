import "./check-compatibility-digests.mjs";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { compatibilityArtifacts, nativeWasmVector, packagedSourceCopies } from "./generated-bindings.mjs";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");

export function generate(args) {
  const binary = process.env.SDK_PROTO_CODEGEN;
  return execFileSync(binary ?? "cargo", binary ? args : ["run", "--locked", "--quiet", "-p", "sdk-proto-codegen", "--", ...args],
    { cwd: root, encoding: "utf8", windowsHide: true });
}

export function checkDescriptors(sourceRoot = root) {
  const output = mkdtempSync(join(tmpdir(), "sdk-descriptors-"));
  try {
    generate(["descriptors", sourceRoot, output]);
    for (const [family, artifacts] of Object.entries(compatibilityArtifacts)) {
      if (!artifacts.descriptorDigest) continue;
      const baseline = readFileSync(join(root, artifacts.descriptorDigest));
      const generated = readFileSync(join(output, artifacts.descriptorDigest.split("/").at(-1)));
      if (!generated.equals(baseline)) throw new Error(`${family} generated descriptor differs from its wire baseline`);
    }
  } finally {
    rmSync(output, { recursive: true, force: true });
  }
}
for (const [source, packaged] of packagedSourceCopies) {
  if (!readFileSync(join(root, source)).equals(readFileSync(join(root, packaged)))) {
    throw new Error(`packaged source drift: ${packaged}`);
  }
}
const harnessSuite = JSON.parse(readFileSync(join(root, compatibilityArtifacts.harness.conformanceDigest), "utf8"));
const nativeWasmCase = harnessSuite.cases.find(item => item.name === "native-wasm-replay-is-byte-equivalent");
const vector = JSON.parse(readFileSync(join(root, nativeWasmVector), "utf8"));
if (JSON.stringify(nativeWasmCase?.vector) !== JSON.stringify(vector)) {
  throw new Error("native/WASM fixture is not bound into the Harness suite");
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) checkDescriptors();
