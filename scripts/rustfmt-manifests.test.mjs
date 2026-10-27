import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { filesByEdition, parseManifest } from "./qualify-rustfmt.mjs";

test("the formatter admits the actual planner consumer-feature manifest", () => {
  // Real tracked input from #1622: nested arrays are valid TOML and were
  // rejected by Bun 1.3.14's parser. Do not copy or simplify this manifest.
  const manifest = readFileSync(new URL("../rust/crates/harness/Cargo.toml", import.meta.url), "utf8");
  assert.match(manifest, /consumer-features\s*=\s*\[\[/);
  assert.equal(parseManifest(manifest).edition, "inherit");
});

test("every current manifest shape passes the formatter's edition planner", () => {
  const groups = filesByEdition(fileURLToPath(new URL("../", import.meta.url)));
  assert(groups.get("2024")?.includes("rust/crates/harness/src/lib.rs"));
  assert([...groups.values()].flat().length > 0);
});
