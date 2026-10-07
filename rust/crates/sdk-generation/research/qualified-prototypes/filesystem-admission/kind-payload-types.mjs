import { readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

const args = process.argv.slice(2);
const argument = name => {
  const index = args.indexOf(name);
  if (index < 0 || args[index + 1] === undefined) {
    throw new Error(`missing ${name}`);
  }
  return args[index + 1];
};

const sourceRoot = resolve(argument("--source-root"));
const outputPath = args.includes("--output") ? resolve(argument("--output")) : undefined;

const enumSources = {
  FileKind: "rust/crates/filesystem/src/kernel/types.rs",
  FilePayload: "rust/crates/filesystem/src/kernel/file_table.rs",
};

const stripRustComments = source => source
  .replace(/\/\*[\s\S]*?\*\//g, "")
  .replace(/(^|\s)\/\/.*$/gm, "$1");

const enumBody = (source, enumName) => {
  const marker = `pub enum ${enumName}`;
  const markerIndex = source.indexOf(marker);
  if (markerIndex < 0) throw new Error(`missing canonical Rust enum ${enumName}`);
  const open = source.indexOf("{", markerIndex + marker.length);
  if (open < 0) throw new Error(`missing body for canonical Rust enum ${enumName}`);
  let depth = 0;
  let quote;
  let escaped = false;
  for (let index = open; index < source.length; index += 1) {
    const character = source[index];
    if (quote !== undefined) {
      if (escaped) escaped = false;
      else if (character === "\\") escaped = true;
      else if (character === quote) quote = undefined;
      continue;
    }
    if (character === '"' || character === "'") {
      quote = character;
      continue;
    }
    if (character === "{") depth += 1;
    else if (character === "}") {
      depth -= 1;
      if (depth === 0) return source.slice(open + 1, index);
    }
  }
  throw new Error(`unterminated canonical Rust enum ${enumName}`);
};

const splitTopLevel = body => {
  const parts = [];
  let start = 0;
  let depth = 0;
  let quote;
  let escaped = false;
  for (let index = 0; index < body.length; index += 1) {
    const character = body[index];
    if (quote !== undefined) {
      if (escaped) escaped = false;
      else if (character === "\\") escaped = true;
      else if (character === quote) quote = undefined;
      continue;
    }
    if (character === '"' || character === "'") {
      quote = character;
      continue;
    }
    if ("({[".includes(character)) depth += 1;
    else if (")}]".includes(character)) depth -= 1;
    else if (character === "," && depth === 0) {
      parts.push(body.slice(start, index));
      start = index + 1;
    }
  }
  parts.push(body.slice(start));
  return parts;
};

const rustVariants = (source, enumName) => splitTopLevel(enumBody(stripRustComments(source), enumName))
  .map(part => part.replace(/#\[[\s\S]*?\]\s*/g, "").trim())
  .map(part => part.match(/^([A-Z][A-Za-z0-9_]*)/)?.[1])
  .filter(Boolean);

const toKebabCase = variant => variant
  .replace(/([a-z0-9])([A-Z])/g, "$1-$2")
  .replace(/([A-Z]+)([A-Z][a-z])/g, "$1-$2")
  .toLowerCase();

const readCanonicalEnums = async () => {
  const result = {};
  for (const [enumName, relativePath] of Object.entries(enumSources)) {
    const source = await readFile(resolve(sourceRoot, relativePath), "utf8");
    const variants = rustVariants(source, enumName);
    if (variants.length === 0 || new Set(variants).size !== variants.length) {
      throw new Error(`canonical Rust enum ${enumName} has no unique variants`);
    }
    result[enumName] = variants.map(variant => ({ variant, value: toKebabCase(variant) }));
  }
  return result;
};

const union = entries => entries.map(({ value }) => JSON.stringify(value)).join(" | ");
const render = enums => `/* Generated from canonical Rust enums; do not hand-edit. */
export type FilesystemFileKind = ${union(enums.FileKind)};
export type FilesystemFilePayloadKind = ${union(enums.FilePayload)};

export interface FilesystemKindPayloadTypes {
  readonly fileKind: FilesystemFileKind;
  readonly payloadKind: FilesystemFilePayloadKind;
}
`;

const enums = await readCanonicalEnums();
const output = render(enums);
if (outputPath !== undefined) await writeFile(outputPath, output, { flag: "wx" });
process.stdout.write(JSON.stringify({
  source_root: sourceRoot,
  source_files: enumSources,
  typescript: output,
}, null, 2) + "\n");
