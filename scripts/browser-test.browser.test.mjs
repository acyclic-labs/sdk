import assert from "node:assert/strict";
import test from "node:test";
import { browserTest } from "./browser-test.mjs";

for (const [page, source] of [["missing-import", "module"], ["syntax-error", "javascript"], ["rejected-startup", "javascript"]]) {
  test(`browser startup reports ${page} before a page result exists`, async () => {
    await assert.rejects(browserTest(`scripts/tests/fixtures/browser-startup/${page}.html`),
      error => error.code === "BROWSER_SOURCE_ENDED" && error.source === source);
  });
}
