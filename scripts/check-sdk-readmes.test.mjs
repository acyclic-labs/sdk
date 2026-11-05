import { readFileSync } from "node:fs";
import assert from "node:assert/strict";
import test from "node:test";
import { checkCopies, examples } from "./check-sdk-readmes.mjs";
import { generatedReadmes, services } from "./generate-sdk-readmes.mjs";

const guide = readFileSync(new URL("../typescript/packages/sdk/README.md", import.meta.url), "utf8");
test("all services have complete examples and matching llms copies", () => {
  const read = file => readFileSync(new URL(`../${file}`, import.meta.url), "utf8").replaceAll("\r\n", "\n");
  for (const [file, { source, generated }] of generatedReadmes(read)) assert.equal(source, generated, file);
  const ts = examples(guide, "ts");
  assert.deepEqual(examples(guide.replaceAll("\n", "\r\n"), "ts"), ts);
  checkCopies(ts, examples(readFileSync(new URL("../llms.txt", import.meta.url), "utf8"), "ts"));
  const rust = examples(readFileSync(new URL("../rust/README.md", import.meta.url), "utf8"), "rust,no_run");
  for (const name of services) {
    assert.match(ts.get(name), /import /);
    assert.match(rust.get(name), /#\[tokio::main\]\nasync fn main\(/);
  }
});
test("missing services, disabled doctests and duplicate snippets fail", () => {
  assert.throws(() => examples(guide.replace("## Streams", "## Missing"), "ts"), /missing Streams/);
  assert.throws(() => examples("## Objects\n```rust,ignore\nfn main() {}\n```", "rust,no_run"), /expected one/);
  assert.throws(() => examples(guide + "\n## Streams\n```ts\nimport 'wrong';\n```", "ts"), /expected one/);
});
test("llms drift fails even when the code still compiles", () => {
  const ts = examples(guide, "ts");
  const changed = new Map(ts);
  changed.set("Streams", `${changed.get("Streams")}\nconsole.log('drift');`);
  assert.throws(() => checkCopies(ts, changed), /Streams: llms.txt differs/);
  const read = file => readFileSync(new URL(`../${file}`, import.meta.url), "utf8").replaceAll("\r\n", "\n");
  const original = generatedReadmes(read);
  for (const extension of ["rs", "mts"]) {
    const changedSource = generatedReadmes(file => `${read(file)}${file.endsWith(`workers.${extension}`) ? "// source changed\n" : ""}`);
    const copies = extension === "rs" ? ["rust/README.md", "rust/crates/workers/README.md"]
      : ["typescript/packages/sdk/README.md", "llms.txt", "typescript/packages/workers/README.md"];
    for (const file of copies) assert.notEqual(changedSource.get(file)?.generated, original.get(file)?.generated);
  }
});
