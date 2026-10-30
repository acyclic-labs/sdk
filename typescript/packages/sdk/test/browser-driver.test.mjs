import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { chromeExecutable, spawnDriver, connectBrowser, navigateChrome } from "./browser-driver.mjs";

test("real browser results settle on publication, script failure and producer retirement", async () => {
  const recovery = "node --test typescript/packages/sdk/test/browser-driver.test.mjs";
  const profile = mkdtempSync(join(tmpdir(), "acyclic-browser-events-"));
  let launched;
  let browser;
  try {
    launched = await spawnDriver(chromeExecutable(`CHROME=/path/to/chrome ${recovery}`), [
      "--headless=new", "--remote-debugging-port=0", `--user-data-dir=${profile}`,
      "--no-first-run", "--no-default-browser-check", "about:blank",
    ], /DevTools listening on (ws:\/\/\S+)/, recovery);
    browser = await connectBrowser(launched.endpoint, recovery);
    const page = async () => {
      const { targetId } = await browser.send("Target.createTarget", { url: "about:blank" });
      const { sessionId } = await browser.send("Target.attachToTarget", { targetId, flatten: true });
      await browser.send("Runtime.enable", {}, sessionId);
      return { targetId, sessionId };
    };
    const url = script => `data:text/html,${encodeURIComponent(`<body><script>${script}</script>`)}`;
    for (const [script, status] of [
      ["document.body.dataset.result = 'passed'", "passed"],
      ["throw new Error('producer failed before publishing a result')", "failed"],
    ]) {
      const { targetId, sessionId } = await page();
      const result = await navigateChrome(browser, sessionId, url(script));
      assert.equal(result.status, status);
      if (status === "failed") assert.match(result.text, /producer failed before publishing/);
      await browser.send("Target.closeTarget", { targetId });
    }
    for (const retireConnection of [false, true]) {
      const { targetId, sessionId } = await page();
      await browser.send("Runtime.addBinding", { name: "__sdkQualificationStarted" }, sessionId);
      const entered = browser.event(message => message.method === "Runtime.bindingCalled"
        && message.sessionId === sessionId && message.params.name === "__sdkQualificationStarted", sessionId);
      const outcome = navigateChrome(browser, sessionId, url("globalThis.__sdkQualificationStarted('entered')"));
      await Promise.race([entered.promise, outcome.then(() => { throw new Error("page settled before the retirement control"); })]);
      // Negative controls drop the producer while its result is still pending.
      const refused = assert.rejects(outcome, /target detached|connection closed/);
      if (retireConnection) browser.close();
      else await browser.send("Target.closeTarget", { targetId });
      await refused;
    }
  } finally {
    browser?.close();
    if (launched) {
      if (launched.child.exitCode === null && launched.child.signalCode === null) launched.child.kill();
      await launched.exited;
    }
    rmSync(profile, { recursive: true, force: true });
  }
});
