import { readFileSync } from "node:fs";
import assert from "node:assert/strict";
import test from "node:test";
import { checkCopies, examples, services } from "./check-sdk-readmes.mjs";

const guide = readFileSync(new URL("../typescript/packages/sdk/README.md", import.meta.url), "utf8");
test("all services have complete examples and matching llms copies", () => {
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
  assert.throws(() => examples(guide.replace("## Swarm", "## Missing"), "ts"), /missing Swarm/);
  assert.throws(() => examples("## Objects\n```rust,ignore\nfn main() {}\n```", "rust,no_run"), /expected one/);
  assert.throws(() => examples(guide + "\n## Swarm\n```ts\nimport 'wrong';\n```", "ts"), /expected one/);
});
test("llms drift fails even when the code still compiles", () => {
  const ts = examples(guide, "ts");
  const changed = new Map(ts);
  changed.set("Swarm", `${changed.get("Swarm")}\nconsole.log('drift');`);
  assert.throws(() => checkCopies(ts, changed), /Swarm: llms.txt differs/);
});
