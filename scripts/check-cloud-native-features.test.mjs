import assert from "node:assert/strict";
import test from "node:test";
import { localCargoPackages } from "./publish-cargo-crates.mjs";
import { checkCloudNativeFeatures, policy } from "./check-cloud-native-features.mjs";

test("Cloud native feature policy accepts real manifests and rejects restored or renamed native switches", () => {
  const packages = localCargoPackages();
  checkCloudNativeFeatures(packages);
  let controls = 0;
  for (const [name, contract] of Object.entries(policy.packages)) {
    if (contract.retired) continue;
    for (const feature of ["local", "native-work"]) {
      const mutated = packages.map(item => item.name === name
        ? { ...item, features: { ...item.features, [feature]: [] } } : item);
      assert.throws(() => checkCloudNativeFeatures(mutated), /Cloud native feature policy: .*unconditional on native targets/);
      controls++;
    }
  }
  assert.ok(controls > 0, "the real Cloud inventory must run negative controls");
});
