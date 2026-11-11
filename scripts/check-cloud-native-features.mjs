import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { localCargoPackages } from "./publish-cargo-crates.mjs";

// The inventory is derived from Cloud's working-tree manifests, including its
// nested workspace. Each retained feature has a reason unrelated to selecting
// native implementations. Reject new names too, so renaming a switch cannot
// evade the native-feature policy.
export const policy = JSON.parse(readFileSync(new URL("../release/cloud-native-features.v1.json", import.meta.url), "utf8"));

export function checkCloudNativeFeatures(packages) {
  for (const [name, contract] of Object.entries(policy.packages)) {
    const item = packages.find(candidate => candidate.name === name);
    if (contract.retired) {
      if (item) throw new Error(`Cloud native feature policy: revived package ${name} needs a feature contract`);
      continue;
    }
    if (!item) throw new Error(`Cloud native feature policy: missing package ${name}`);
    for (const feature of Object.keys(item.features)) {
      if (!contract.features[feature]) {
        throw new Error(`Cloud native feature policy: ${name}/${feature} has no non-native-feature reason; native implementations must be unconditional on native targets`);
      }
    }
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  checkCloudNativeFeatures(localCargoPackages());
  console.log("Cloud native feature policy: PASS");
}
