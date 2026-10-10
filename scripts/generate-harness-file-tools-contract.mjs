export const rust = [["acyclic-harness", "example", "file-tools-contract"]];

export function render([stdout]) {
  const contract = JSON.parse(stdout);
  const fields = ["exact_read", "file_result", "literal_search", "range_read", "source", "tool_definitions"];
  if (Object.keys(contract).sort().join(",") !== fields.join(",")) {
    throw new Error("Harness file-tool contract field inventory drift");
  }
  if (!Array.isArray(contract.tool_definitions) || contract.tool_definitions.length === 0) {
    throw new Error("Harness file-tool contract has no native definitions");
  }
  const variants = new Set();
  for (const entry of contract.tool_definitions) {
    if (typeof entry.variant !== "string" || variants.has(entry.variant) ||
        !entry.definition || !Array.isArray(entry.definition_digest) ||
        entry.definition_digest.length !== 32 ||
        entry.definition_digest.some(byte => !Number.isInteger(byte) || byte < 0 || byte > 255)) {
      throw new Error("Harness file-tool contract has an invalid definition identity");
    }
    variants.add(entry.variant);
  }
  return {
    "fixtures/harness/v2/file-tools-contract.json": `${JSON.stringify(contract, null, 2)}\n`,
  };
}
