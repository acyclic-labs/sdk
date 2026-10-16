import "./check-compatibility-digests.mjs";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { compatibilityArtifacts, nativeWasmVector, packagedSourceCopies } from "./generated-bindings.mjs";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
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
