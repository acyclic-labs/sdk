const availableModules = [
  "generate-stream-authorization.mjs",
  "generate-objects-authorization.mjs",
  "generate-actors-authorization.mjs",
  "generate-machines-authorization.mjs",
  "generate-workers-authorization.mjs",
  "generate-filesystem-authorization.mjs",
  "generate-actors-routes.mjs",
  "generate-actors-rpc-inventory.mjs",
  "generate-stream-rpc-inventory.mjs",
  "generate-objects-rpc-inventory.mjs",
  "generate-inference-routes.mjs",
  "filesystem-napi-types.mjs",
  "stream-napi-types.mjs",
  "actors-napi-types.mjs",
  "filesystem-descriptor-digest.mjs",
  "generate-filesystem-defaults.mjs",
  "generate-filesystem-hosted-contract.mjs",
  "generate-filesystem-object-references.mjs",
  "generate-filesystem-git-compat-contract.mjs",
  "generate-inference-defaults.mjs",
  "generate-inference-terminal-metadata.mjs",
  "generate-inference-widths.mjs",
  "generate-stream-http-contract.mjs",
  "generate-stream-token-operations.mjs",
  "generate-harness-model-fixtures.mjs",
  "generate-harness-file-tools-contract.mjs",
  "generate-machines-managed-oci-contract.mjs",
];

export function selectContractModules(families, knownFamilies) {
  // A complete family or one exact contract can be selected. Exact selection
  // keeps a narrow contract update from compiling unrelated bindings.
  const matchesFamily = (name, family) => name === `generate-${family}.mjs` || name.startsWith(`generate-${family}-`) || name.startsWith(`${family}-`);
  for (const family of families) {
    if (!knownFamilies.has(family) && !availableModules.includes(`generate-${family}.mjs`)) {
      throw new Error(`STEP_FAILED contract-selection: unknown family ${JSON.stringify(family)}; recovery: bun scripts/generate-contracts.mjs check`);
    }
  }
  return availableModules.filter(name => !families.length || families.some(family => matchesFamily(name, family)));
}
