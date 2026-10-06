#!/usr/bin/env node

// Release/manual qualification for Rust-generated remote TypeScript facades.
// This check intentionally reads the emitted package tree: a source checkout
// can contain a newer Rust contract while an archive still contains stale JS.
// The default transport and credential policy must therefore agree in the
// package manifest, provenance, and generated client binding.
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";

const args = process.argv.slice(2);
const value = (name) => {
  const index = args.indexOf(name);
  return index < 0 ? undefined : args[index + 1];
};
if (args.includes("--help") || !value("--packages-root") || !value("--output")) {
  throw new Error("usage: qualify-typescript-remote-defaults.mjs --packages-root DIR --output FILE");
}

const packagesRoot = resolve(value("--packages-root"));
const outputPath = resolve(value("--output"));
const families = ["actors", "workers", "objects", "stream", "inference", "machines", "filesystem"];
const expectedPackageNames = new Map(families.map((family) => [family, `@acyclic-labs/${family === "filesystem" ? "fs" : family}`]));
const expectedPolicyNames = new Map(families.map((family) => [family, `${family.toUpperCase()}_REMOTE_POLICY`]));

const fail = (message) => { throw new Error(message); };
const readJson = (path) => JSON.parse(readFileSync(path, "utf8"));
const sha256 = (path) => createHash("sha256").update(readFileSync(path)).digest("hex");
const requireFile = (path, label) => { if (!existsSync(path)) fail(`missing ${label}: ${path}`); };

// The generated policy is a literal Rust projection. Extracting only this
// literal keeps qualification independent of package runtime dependencies,
// which is necessary before npm install has linked the archive's graph.
function policyLiteral(source, name, path) {
  const match = source.match(new RegExp(`export const ${name}\\s*=\\s*(\\{[\\s\\S]*?\\});`));
  if (!match) fail(`${path} does not export ${name}`);
  const native = match[1].match(/native:\s*\[([^\]]*)\]/)?.[1] ?? "";
  const browser = match[1].match(/browser:\s*\[([^\]]*)\]/)?.[1] ?? "";
  const kinds = (value) => [...value.matchAll(/kind:\s*"([^"]+)"/g)].map((entry) => entry[1]);
  const credentialPolicy = match[1].match(/credentialPolicy:\s*"([^"]+)"/)?.[1];
  return { native: kinds(native), browser: kinds(browser), credentialPolicy };
}

const checks = [];
for (const family of families) {
  const packageRoot = join(packagesRoot, family);
  const packageManifestPath = join(packageRoot, "package.json");
  const provenancePath = join(packageRoot, "generated", "rust-provenance.json");
  const generatedClientPath = join(packageRoot, "dist", "generated-client.js");
  const generatedTypesPath = join(packageRoot, "dist", "generated-client.d.ts");
  for (const [path, label] of [[packageManifestPath, "package manifest"], [provenancePath, "Rust provenance"], [generatedClientPath, "generated client"], [generatedTypesPath, "generated declarations"]]) requireFile(path, `${family} ${label}`);
  const manifest = readJson(packageManifestPath);
  const provenance = readJson(provenancePath);
  const generated = manifest.acyclicGenerated ?? {};
  const recorded = provenance.acyclicGenerated ?? provenance;
  if (manifest.name !== expectedPackageNames.get(family)) fail(`${family} package name is ${manifest.name}, expected ${expectedPackageNames.get(family)}`);
  if (generated.family !== family || recorded.family !== family) fail(`${family} Rust provenance family mismatch`);
  if (generated.sourceGitSha !== recorded.sourceGitSha || generated.sourceModelRevision !== recorded.sourceModelRevision || generated.sourceContentSha256 !== recorded.sourceContentSha256) fail(`${family} manifest/provenance source identities differ`);
  const sourceGitSha = recorded.sourceGitSha;
  if (!/^[0-9a-f]{40}$/i.test(sourceGitSha ?? "")) fail(`${family} provenance has no Git revision`);
  if (!/^[0-9a-f]{64}$/i.test(recorded.sourceModelRevision ?? "")) fail(`${family} provenance has no model identity`);
  if (!/^[0-9a-f]{64}$/i.test(recorded.sourceContentSha256 ?? "")) fail(`${family} provenance has no source-content identity`);
  const generatedClientSha = recorded.generatedClientSha256;
  if (generatedClientSha !== sha256(generatedClientPath)) fail(`${family} generated client hash differs from provenance`);
  const source = readFileSync(generatedClientPath, "utf8");
  if (!manifest.exports?.["./generated-client"]) fail(`${family} does not expose its Rust-generated binding`);
  if (!source.includes("RUST_OWNED_CREDENTIAL_POLICY") || !source.includes("validateRustOwnedCredentialPolicy")) fail(`${family} has no Rust-owned credential validation facade`);
  const policy = policyLiteral(source, expectedPolicyNames.get(family), generatedClientPath);
  const defaults = generated.defaultTransports;
  if (defaults?.native !== policy.native[0] || defaults?.browser !== (policy.browser[0] ?? null)) fail(`${family} manifest default transport differs from generated Rust policy`);
  if (policy.native.length === 0) fail(`${family} has no native transport`);
  if (policy.credentialPolicy !== "bearer-no-crlf" && family !== "machines") fail(`${family} has an unexpected credential policy: ${policy.credentialPolicy}`);
  if (family === "filesystem" && policy.browser[0] !== "grpc-web") fail("filesystem browser transport must be Rust-qualified grpc-web");
  if (family === "machines" && policy.browser[0] !== "grpc-web") fail("machines browser transport must be Rust-qualified grpc-web");
  checks.push({ family, package: manifest.name, sourceGitSha, sourceModelRevision: recorded.sourceModelRevision, sourceContentSha256: recorded.sourceContentSha256, generatedClientSha256: generatedClientSha, defaultTransports: defaults, credentialPolicy: policy.credentialPolicy, generatedDeclarations: generatedTypesPath });
}

const receipt = {
  schema: "acyclic.sdk.typescript.remote-defaults-qualification.v1",
  status: "passed",
  packagesRoot: packagesRoot,
  families: checks,
  checks: [
    "Rust-generated package manifest and provenance identities agree",
    "generated JavaScript and declarations are present",
    "generated JavaScript hash matches Rust provenance",
    "native and browser defaults match emitted Rust transport policy",
    "filesystem and machines browser policies are Rust-qualified grpc-web",
    "credential metadata and validation entrypoints are Rust-owned",
  ],
};
mkdirSync(resolve(outputPath, ".."), { recursive: true });
writeFileSync(outputPath, `${JSON.stringify(receipt, null, 2)}\n`);
console.log(JSON.stringify({ schema: receipt.schema, status: receipt.status, output: outputPath, families: families.length }));
