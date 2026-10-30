// Runs the browser qualification pages in a private headless Chrome or Firefox and
// reports each page's result, with every uncaught exception and console error
// each page raised while it ran.
//
// Nothing here waits on a guess: the file server is listening before the browser
// starts, the driver publishes its protocol endpoint on its output, and each
// page is navigated only once the driver is observing it.
//
// Usage: node test/browser-qualify.mjs [page ...]   (default: every page)
// CHROME selects Chromium. BROWSER=firefox uses GECKODRIVER and optional FIREFOX.
import { existsSync, mkdtempSync, readFileSync, rmSync, statSync } from "node:fs";
import { createServer } from "node:http";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { extname, join, normalize, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnDriver, chromeExecutable, connectBrowser as connect, navigateChrome, RESULT_SCRIPT } from "../../sdk/test/browser-driver.mjs";

const PAGES = [
  "browser-smoke.html",
  "browser-multitab.html",
  "browser-publication.html",
  "browser-publication.html?profile=opfs",
  "browser-stream.html",
  "../harness/test/browser-wire.html",
  "../harness/test/browser-context.html",
  "../harness/test/browser-wasm-init-failure.html",
  "../harness/test/browser-agents.html",
  "../harness/test/browser-agents-tool.html",
  "../harness/test/browser-agent-guest.html",
];
const root = resolve(fileURLToPath(new URL("..", import.meta.url)));
const require = createRequire(import.meta.url);
// Resolve dependencies through Node's package resolver instead of assuming
// that Bun has exposed a workspace-level node_modules symlink. The browser
// import map uses the stable /node_modules URL; this table makes that URL
// work with both isolated and hoisted installs.
const dependencyRoots = new Map([
  ["/rust/crates/conformance/cloudflare/", resolve(root, "../../../rust/crates/conformance/cloudflare")],
  ["/stream/", resolve(root, "../stream")],
  ["/actors/", resolve(root, "../actors")],
  ["/auth/", resolve(root, "../auth")],
  ["/harness/node_modules/@bufbuild/protobuf/", resolve(require.resolve("@bufbuild/protobuf"), "../../..")],
  ["/harness/", resolve(root, "../harness")],
  ["/node_modules/@bufbuild/protobuf/", resolve(require.resolve("@bufbuild/protobuf"), "../../..")],
]);
const types = {
  ".html": "text/html",
  ".js": "text/javascript",
  ".mjs": "text/javascript",
  ".wasm": "application/wasm",
  ".json": "application/json",
};

// Requests the file server did not answer in full, printed with a failed
// page: a page's "Failed to fetch" names no request, but these do.
const anomalies = [];

async function serve() {
  const server = createServer((request, response) => {
    const started = Date.now();
    response.on("close", () => {
      if (response.statusCode !== 200 || !response.writableFinished) {
        const finished = response.writableFinished ? "" : " (closed before finishing)";
        anomalies.push(`${request.url} -> ${response.statusCode}${finished} after ${Date.now() - started} ms`);
      }
    });
    const pathname = decodeURIComponent(new URL(request.url, "http://host").pathname);
    const dependency = [...dependencyRoots].find(([prefix]) => pathname.startsWith(prefix));
    const base = dependency?.[1] ?? root;
    const relative = dependency === undefined ? pathname : pathname.slice(dependency[0].length);
    const path = normalize(join(base, relative));
    const boundary = base.endsWith(sep) ? base : `${base}${sep}`;
    if (!path.startsWith(boundary) || !existsSync(path) || !statSync(path).isFile()) {
      response.writeHead(404, { connection: "close" }).end();
      return;
    }
    // One request per connection: a reused keep-alive socket that Node is
    // closing as Chrome sends on it surfaces in the page as "Failed to fetch".
    response.writeHead(200, {
      connection: "close",
      "content-type": types[extname(path)] ?? "application/octet-stream",
    });
    response.end(readFileSync(path));
  });
  await new Promise((resolveListen, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolveListen);
  });
  return server;
}

async function launchChrome(profile) {
  const { child: chrome, exited, endpoint } = await spawnDriver(
    chromeExecutable("CHROME=/path/to/chrome node test/browser-qualify.mjs"),
    [
      "--headless=new",
      "--remote-debugging-port=0",
      `--user-data-dir=${profile}`,
      "--no-first-run",
      "--no-default-browser-check",
      "--disable-popup-blocking",
      // Actor windows run concurrently with the page that opened them; a
      // backgrounded or occluded window must not be throttled or suspended.
      "--disable-background-timer-throttling",
      "--disable-backgrounding-occluded-windows",
      "--disable-renderer-backgrounding",
      "about:blank",
    ],
    /DevTools listening on (ws:\/\/\S+)/,
    "set CHROME to a working Chrome/Chromium executable and rerun node test/browser-qualify.mjs",
  );
  return { chrome, exited, endpoint };
}

async function launchFirefox(profile) {
  const { child: driver, exited, endpoint: address } = await spawnDriver(
    process.env.GECKODRIVER ?? "geckodriver", ["--port", "0"],
    /Listening on (127\.0\.0\.1:\d+)/,
    "set GECKODRIVER to a working executable and rerun BROWSER=firefox node test/browser-qualify.mjs",
  );
  const endpoint = `http://${address}`;
  const request = async (method, path, body) => {
    const response = await fetch(`${endpoint}${path}`, {
      method, headers: { "content-type": "application/json" },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
    const result = await response.json();
    if (!response.ok || result.value?.error) throw new Error(JSON.stringify(result.value));
    return result.value;
  };
  try {
    const firefoxOptions = {
      args: ["-headless", "-profile", profile],
      prefs: { "dom.disable_open_during_load": false, "dom.popup_maximum": 100 },
      ...(process.env.FIREFOX ? { binary: process.env.FIREFOX } : {}),
    };
    const session = await request("POST", "/session", {
      capabilities: { alwaysMatch: { browserName: "firefox", webSocketUrl: true, "moz:firefoxOptions": firefoxOptions } },
    });
    const sessionPath = `/session/${session.sessionId}`;
    const browser = await connect(session.capabilities.webSocketUrl, "BROWSER=firefox node test/browser-qualify.mjs");
    await browser.send("session.subscribe", { events: ["log.entryAdded"] });
    const observer = { events: [] };
    browser.listen(message => {
      if (message.method !== "log.entryAdded" || message.params.level !== "error") return;
      observer.events.push(`${message.params.type === "javascript" ? "uncaught" : "console.error"}: ${message.params.text}`);
    });
    return {
      chrome: driver, exited, browser, observer,
      request: (method, path, body) => request(method, `${sessionPath}${path}`, body),
      close: async () => { await request("DELETE", sessionPath); browser.close(); },
    };
  } catch (error) {
    if (driver.exitCode === null) driver.kill();
    await exited;
    throw error;
  }
}

async function runFirefoxPage(launched, observer, origin, page) {
  observer.events = [];
  const events = observer.events;
  const browser = launched.browser;
  const { context } = await browser.send("browsingContext.create", { type: "tab" });
  let preload;
  try {
    preload = await browser.send("script.addPreloadScript", {
      functionDeclaration: `() => { ${RESULT_SCRIPT} }`, contexts: [context],
    });
    await browser.send("browsingContext.navigate", { context, url: `${origin}/test/${page}`, wait: "complete" });
    const evaluated = await browser.send("script.evaluate", {
      expression: "globalThis.__sdkQualificationResult.then(value => JSON.stringify(value))",
      target: { context }, awaitPromise: true,
    });
    if (evaluated.type === "exception") throw new Error(`browser script.evaluate: ${evaluated.exceptionDetails.text}; recovery: BROWSER=firefox node test/browser-qualify.mjs`);
    const observed = JSON.parse(evaluated.result.value);
    return { page, passed: observed.status === "passed", text: observed.text, events };
  } catch (error) {
    return { page, passed: false, text: String(error.stack ?? error), events };
  } finally {
    if (preload) await browser.send("script.removePreloadScript", preload).catch(() => {});
    await browser.send("browsingContext.close", { context }).catch(() => {});
  }
}

function describeException(details) {
  const exception = details.exception;
  return exception?.description ?? exception?.value ?? details.text;
}

// Collects the uncaught exceptions and console errors of the qualification
// pages into `observer.events`. Only the page itself is attached: its actor
// windows report their failures to it, and attaching to windows a page opens
// can leave them unable to start.
function observe(browser) {
  const observer = { events: [] };
  browser.listen((message) => {
    if (message.method === "Runtime.exceptionThrown") {
      observer.events.push(`uncaught: ${describeException(message.params.exceptionDetails)}`);
    } else if (message.method === "Runtime.consoleAPICalled" && message.params.type === "error") {
      const text = message.params.args.map((argument) => argument.description ?? argument.value).join(" ");
      observer.events.push(`console.error: ${text}`);
    }
  });
  return observer;
}

async function runPage(browser, observer, origin, page) {
  observer.events = [];
  const events = observer.events;
  // The page is navigated only once its session is observing it.
  const { targetId } = await browser.send("Target.createTarget", { url: "about:blank" });
  const { sessionId } = await browser.send("Target.attachToTarget", { targetId, flatten: true });
  await browser.send("Runtime.enable", {}, sessionId);
  let observed;
  try {
    observed = await navigateChrome(browser, sessionId, `${origin}/test/${page}`);
  } catch (error) {
    return { page, passed: false, text: String(error.stack ?? error), events };
  } finally {
    await browser.send("Target.closeTarget", { targetId }).catch(() => {});
  }
  return { page, passed: observed.status === "passed", text: observed.text, events };
}

const pages = process.argv.length > 2 ? process.argv.slice(2) : PAGES;
const server = await serve();
const origin = `http://127.0.0.1:${server.address().port}`;
const profile = mkdtempSync(join(tmpdir(), "acyclic-fs-browser-"));
let launched;
let failed = false;
try {
  const firefox = process.env.BROWSER === "firefox";
  if (process.env.BROWSER && !["chrome", "chromium", "firefox"].includes(process.env.BROWSER)) {
    throw new Error(`unsupported BROWSER: ${process.env.BROWSER}`);
  }
  launched = firefox ? await launchFirefox(profile) : await launchChrome(profile);
  const browser = firefox ? launched.browser : await connect(launched.endpoint, "node test/browser-qualify.mjs");
  const observer = firefox ? launched.observer : observe(browser);
  for (const page of pages) {
    const outcome = await (firefox ? runFirefoxPage(launched, observer, origin, page)
      : runPage(browser, observer, origin, page)).catch((error) => ({
      page,
      passed: false,
      text: String(error.stack ?? error),
      events: [],
    }));
    failed ||= !outcome.passed;
    process.stdout.write(`${outcome.passed ? "PASS" : "FAIL"} ${page}: ${outcome.text}\n`);
    for (const event of outcome.events) process.stdout.write(`  ${event}\n`);
    if (!outcome.passed) {
      for (const anomaly of anomalies) process.stdout.write(`  served: ${anomaly}\n`);
    }
    anomalies.length = 0;
  }
  if (firefox) await launched.close();
  else {
    await browser.send("Browser.close").catch(() => {});
    browser.close();
  }
} finally {
  if (launched !== undefined) {
    if (launched.chrome.exitCode === null) launched.chrome.kill();
    await launched.exited;
  }
  server.close();
  try {
    rmSync(profile, { recursive: true, force: true });
  } catch (error) {
    process.stderr.write(`browser profile cleanup: ${error.message}; recovery: close browser processes using ${profile}, then remove that directory
`);
  }
}
process.exit(failed ? 1 : 0);
