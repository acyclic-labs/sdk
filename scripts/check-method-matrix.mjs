import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const matrix = JSON.parse(await readFile(join(root, "compatibility/sdk-method-matrix.json"), "utf8"));
const failures = [];

function rpcInventory(proto) {
  const inventory = [];
  let service;
  for (const line of proto.split(/\r?\n/u)) {
    const serviceMatch = line.match(/^\s*service\s+(\w+)\s*\{/u);
    if (serviceMatch) service = serviceMatch[1];
    const rpcMatch = line.match(/^\s*rpc\s+(\w+)\s*\(/u);
    if (rpcMatch) {
      if (!service) throw new Error(`RPC ${rpcMatch[1]} appears outside a service`);
      inventory.push(`${service}/${rpcMatch[1]}`);
    }
    if (/^\s*\}\s*$/u.test(line)) service = undefined;
  }
  return inventory.sort();
}

function snake(value) {
  return value.replace(/([a-z0-9])([A-Z])/gu, "$1_$2").toLowerCase();
}

for (const family of matrix.families) {
  const proto = await readFile(join(root, family.proto), "utf8");
  const publicSource = await readFile(join(root, family.publicSource), "utf8");
  const transportSource = await readFile(join(root, family.transportSource), "utf8");
  const conformanceSource = await readFile(join(root, family.conformanceSource), "utf8");
  const actual = rpcInventory(proto);
  const declared = family.methods.map(([rpc]) => rpc).sort();
  if (JSON.stringify(actual) !== JSON.stringify(declared)) {
    failures.push(`${family.name}: RPC inventory differs: expected ${actual.join(", ")}; matrix has ${declared.join(", ")}`);
  }
  if (!/(?:test|conformance)/iu.test(conformanceSource)) {
    failures.push(`${family.name}: conformance source contains no executable test or conformance surface`);
  }
  for (const [rpc, publicMethod] of family.methods) {
    const rpcName = rpc.split("/")[1];
    if (!new RegExp(`\\b${publicMethod}\\b`, "u").test(publicSource)) {
      failures.push(`${family.name}: public method ${publicMethod} for ${rpc} is absent`);
    }
    const transportMethod = snake(rpcName);
    if (!new RegExp(`\\b${transportMethod}\\b`, "u").test(transportSource)) {
      failures.push(`${family.name}: transport method ${transportMethod} for ${rpc} is absent`);
    }
  }
}

if (failures.length) {
  console.error(failures.join("\n"));
  process.exitCode = 1;
} else {
  console.log(`SDK method matrix covers ${matrix.families.reduce((count, family) => count + family.methods.length, 0)} RPCs across ${matrix.families.length} families`);
}
