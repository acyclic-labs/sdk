import assert from "node:assert/strict";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

const tools = new URL("../tools/", import.meta.url);

test("every tool module resolves its relative imports", () => {
  for (const name of readdirSync(tools).filter(name => name.endsWith(".mjs"))) {
    const file = new URL(name, tools);
    const source = readFileSync(file, "utf8");
    for (const [, , specifier] of source.matchAll(/(?:from\s*|import\s*\(\s*|import\s*)(["'])(\.{1,2}\/[^"']+)\1/g)) {
      assert.ok(existsSync(resolve(dirname(fileURLToPath(file)), specifier)), `${name} imports missing ${specifier}`);
    }
  }
});
