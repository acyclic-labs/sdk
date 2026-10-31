export const rust = [["acyclic-fs", "example", "filesystem-object-reference-contract"]];

export function render([stdout]) {
  const contract = JSON.parse(stdout);
  const lengths = new Array(256).fill(0);
  const kinds = new Array(256).fill(0);
  for (const { tag, kind, length } of contract.layouts) {
    if (!Number.isInteger(tag) || tag < 0 || tag > 255 || lengths[tag] !== 0
      || !Number.isInteger(kind) || kind < 1 || kind > 255
      || !Number.isInteger(length) || length < 1 || length > 255) {
      throw new Error("ObjectId step=canonical-layout invalid; recovery=bun scripts/generate-contracts.mjs write filesystem");
    }
    lengths[tag] = length;
    kinds[tag] = kind;
  }
  const header = "// @generated from acyclic_fs::ObjectId; do not edit.\n\n";
  const source = `${header}const lengths = new Uint8Array(${JSON.stringify(lengths)});\n` +
    `const kinds = new Uint8Array(${JSON.stringify(kinds)});\n\n` +
    `export function isObjectReference(value, kind) {\n` +
    `  if (!(value instanceof Uint8Array)) return false;\n` +
    `  const tag = value[0];\n` +
    `  return tag !== undefined && lengths[tag] !== 0 && value.byteLength === lengths[tag]\n` +
    `    && (kind === undefined || kind === kinds[tag]);\n` +
    `}\n`;
  return {
    "typescript/packages/filesystem/generated/object-references.js": source,
    "typescript/packages/filesystem/generated/object-references.d.ts": `${header}` +
      `export declare function isObjectReference(value: unknown, kind?: number): value is Uint8Array;\n`,
    "typescript/packages/filesystem/test/generated/object-references.json": `${JSON.stringify(contract)}\n`,
  };
}
