import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { describe, expect, test } from "bun:test";

const packageRoot = join(import.meta.dir, "..");
const sourcePath = join(packageRoot, "../../../rust/crates/harness/src/wasm.rs");
const publicDeclarationPath = join(packageRoot, "generated/wasm/acyclic_harness_wasm.d.ts");
const lowLevelDeclarationPath = join(packageRoot, "generated/wasm/acyclic_harness_wasm_bg.wasm.d.ts");

interface ExportedFunction {
  readonly name: string;
  readonly rustName: string;
  readonly method: boolean;
}

/**
 * Read wasm-bindgen function attributes without trying to parse Rust.  The
 * attribute is line-oriented in this source and may be followed by an
 * `#[expect(...)]` helper attribute before the function declaration.
 */
function sourceExports(source: string): ExportedFunction[] {
  const lines = source.split(/\r?\n/u);
  const exports: ExportedFunction[] = [];
  let braceDepth = 0;
  let implDepth: number | undefined;
  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index] ?? "";
    const impl = /^\s*(?:pub\s+)?impl\b/u.test(line);
    if (impl && implDepth === undefined) implDepth = braceDepth;
    if (!/^\s*#\[wasm_bindgen\b/u.test(line)) {
      braceDepth += braceDelta(line);
      if (implDepth !== undefined && braceDepth <= implDepth) implDepth = undefined;
      continue;
    }
    let attribute = lines[index] ?? "";
    while (!/\)\]\s*$/u.test(attribute) && !/^\s*#\[wasm_bindgen\]\s*$/u.test(attribute)) {
      index += 1;
      attribute += `\n${lines[index] ?? ""}`;
    }
    let declaration = index + 1;
    while (/^\s*#\[/u.test(lines[declaration] ?? "")) declaration += 1;
    const functionLine = lines[declaration] ?? "";
    const functionMatch = /^\s*pub\s+(?:async\s+)?fn\s+([A-Za-z0-9_]+)/u.exec(functionLine);
    if (functionMatch === null) continue;
    const nameMatch = /\bjs_name\s*=\s*(?:"([A-Za-z0-9_]+)"|([A-Za-z0-9_]+))/u.exec(attribute);
    const name = nameMatch?.[1] ?? nameMatch?.[2] ?? functionMatch[1];
    // Constructors are represented by a class constructor rather than an
    // exported function. Methods are checked through the public declaration,
    // while the low-level assertion below covers only free functions.
    const method = implDepth !== undefined || lines.slice(Math.max(0, index - 3), index)
      .some(previous => /^\s*(?:pub\s+)?impl\b/u.test(previous));
    exports.push({ name, rustName: functionMatch[1], method });
    braceDepth += braceDelta(attribute);
    if (implDepth !== undefined && braceDepth <= implDepth) implDepth = undefined;
  }
  return exports;
}

function braceDelta(line: string): number {
  const withoutStrings = line
    .replace(/"(?:\\.|[^"\\])*"/gu, "")
    .replace(/\/\/.*$/u, "");
  return [...withoutStrings].filter(character => character === "{").length
    - [...withoutStrings].filter(character => character === "}").length;
}

function declarationHasCall(source: string, name: string): boolean {
  const escaped = name.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
  return new RegExp(`\\b${escaped}\\s*\\(`, "u").test(source);
}

function declarationHasLowLevelExport(source: string, name: string): boolean {
  const escaped = name.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
  return new RegExp(`\\b${escaped}\\s*:`, "u").test(source);
}

describe("generated Harness WASM contract surface", () => {
  test("keeps every wasm-bindgen function represented in the public declaration", async () => {
    const [rust, declaration] = await Promise.all([
      readFile(sourcePath, "utf8"),
      readFile(publicDeclarationPath, "utf8"),
    ]);
    const missing = sourceExports(rust)
      .filter(({ name }) => name !== "new" && !declarationHasCall(declaration, name))
      .map(({ name, rustName }) => `${name} (${rustName})`);
    expect(missing).toEqual([]);
  });

  test("keeps free wasm-bindgen functions represented in the low-level declaration", async () => {
    const [rust, declaration] = await Promise.all([
      readFile(sourcePath, "utf8"),
      readFile(lowLevelDeclarationPath, "utf8"),
    ]);
    const missing = sourceExports(rust)
      .filter(({ name, rustName, method }) => !method && name !== "new" && rustName !== "path_conflicts"
        && !declarationHasLowLevelExport(declaration, name))
      .map(({ name, rustName }) => `${name} (${rustName})`);
    expect(missing).toEqual([]);
  });
});
