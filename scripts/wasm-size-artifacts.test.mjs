import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { reportWasm } from "./wasm-size-report.mjs";

test("generated real artifacts conserve independently counted file and code bytes", () => {
  for (const family of ["filesystem", "stream"]) {
    const stem = family === "filesystem" ? "fs" : family;
    const bytes = readFileSync(new URL(`../typescript/packages/${family}/generated/wasm/acyclic_${stem}_wasm_bg.wasm`, import.meta.url));
    const report = reportWasm(bytes, ["future"]);
    assert.equal(report.moduleBytes, bytes.length);
    assert.equal(report.headerBytes + report.sections.reduce((n, row) => n + row.encodedBytes, 0), bytes.length);
    assert.equal(report.code.payloadBytes, report.code.vectorBytes + report.functions.reduce((n, row) => n + row.bodyBytes + row.sizePrefixBytes, 0));
    assert.equal(report.namedFunctions, 0);
    assert.equal(report.nameCoverage, "unavailable");
    assert.equal(report.matches[0].encodedBytes, null);
    assert.ok(report.definedFunctions > 100);
  }
});
