import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

test("the browser bundle externalizes the wasm module to a page-resolvable path", () => {
  const source = readFileSync(new URL("../typescript/packages/stream/test/remote-session.mjs", import.meta.url), "utf8");
  const external = /onResolve\(\{ filter: \/@acyclic\\\/stream-wasm\$\/ \}, \(\) => \(\{ path: "([^"]+)", external: true \}\)\)/.exec(source)?.[1];
  assert.ok(external, "remote-session externalizes the stream wasm module");
  assert.ok(external.startsWith(".") || external.startsWith("/"), `browser bundle externalizes ${external}, which the page cannot resolve`);
});
