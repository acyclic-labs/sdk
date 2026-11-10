import test from "node:test";
import { browserTest } from "../../../../scripts/browser-test.mjs";

for (const page of ["browser-agents", "browser-agent-guest", "browser-agents-tool", "browser-models"]) {
  test(page, async () => {
    console.log(await browserTest(`typescript/packages/harness/test/${page}.html`));
  });
}
