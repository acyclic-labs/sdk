// Rust owns wire identities, routes, streaming shapes, and operation policy.
// JavaScript only launches the renderer and inspects target-runtime exposure.
import { spawnSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { createSourceInspector } from "./public-rpc-source.mjs";

const root = new URL("../", import.meta.url);
const mode = process.argv[2];
if (!["write", "check", "complete"].includes(mode)) throw new Error("expected write, check, or complete");
const rendered = spawnSync(process.env.ACYCLIC_CARGO_BIN || process.env.SDK_CARGO || "cargo", [
  "run", "--quiet", "--locked", "--manifest-path",
  fileURLToPath(new URL("rust/crates/sdk-typescript/Cargo.toml", root)),
  "--bin", "sdk-rpc-contracts",
], { cwd: fileURLToPath(root), encoding: "utf8", maxBuffer: 8 * 1024 * 1024 });
if (rendered.error || rendered.status !== 0) throw new Error(rendered.error?.message || rendered.stderr || "Rust RPC inventory failed");
const contracts = JSON.parse(rendered.stdout);
const inspectSource = createSourceInspector(root);
// Preserve the existing four-family source-exposure audit. Other registered
// families appear in the complete contract inventory; runtime qualification
// is provided by the central installed-package scenarios, not this scan.
const exposureFamilies = new Set(["actors", "workers", "stream", "objects"]);
const rows = contracts.map(({ service, method, localName, ...contract }) => {
  const retired = contract.contractStatus === "retired";
  const inspected = !retired && exposureFamilies.has(contract.family);
  const exposure = inspected ? inspectSource({ typeName: service }, {
    name: method, localName, methodKind: contract.kind,
    input: { typeName: contract.request }, output: { typeName: contract.response },
  }) : { rustGrpc: null, rustHttp: null, typescriptGrpcNodeBun: null, typescriptHttp: null, typescriptPackageExported: null };
  return { ...contract, exposureScope: retired ? "retired" : inspected ? "source-inspected" : "runtime-qualification", ...exposure };
});
const output = JSON.stringify(rows, null, 2) + "\n";
const path = new URL("compatibility/public-rpc-matrix.json", root);
if (mode === "write") writeFileSync(path, output);
else if (readFileSync(path, "utf8") !== output) throw new Error("public RPC matrix is stale");
if (mode === "complete") {
  const gaps = rows.filter(row => row.exposureScope === "source-inspected" &&
    (!row.rustGrpc || !row.rustHttp || !row.typescriptGrpcNodeBun || !row.typescriptHttp || !row.typescriptPackageExported));
  if (gaps.length) throw new Error(`public SDK source exposure incomplete:\n${gaps.map(row => row.rpc).join("\n")}`);
}
console.log(`Public RPC inventory: ${rows.filter(row => row.contractStatus === "target").length} active, ${rows.filter(row => row.contractStatus === "retired").length} retired; source scans do not certify runtime qualification`);