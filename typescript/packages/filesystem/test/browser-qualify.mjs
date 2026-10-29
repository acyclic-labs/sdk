// Runs the browser qualification pages in a private headless Chrome or Firefox and
// reports each page's result, with every uncaught exception and console error
// each page raised while it ran.
//
// Nothing here waits on a guess: the file server is listening before the browser
// starts, the driver publishes its own protocol endpoint in its profile, and each
// page is navigated only once the driver is observing it.
//
// Usage: node test/browser-qualify.mjs [page ...]   (default: every page)
// CHROME selects Chromium. BROWSER=firefox uses GECKODRIVER and optional FIREFOX.
import { spawn } from "node:child_process";
import { connectBrowserProtocol as connect } from "../../../../scripts/browser-protocol.mjs";
import { chromiumPageResult, waitForBrowserResult } from "../../../../scripts/browser-result.mjs";
import { waitForFileValue } from "../../../../scripts/graphcoder-e2e/wait-path.mjs";
import { existsSync, mkdtempSync, readFileSync, rmSync, statSync } from "node:fs";
import { createServer } from "node:http";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { extname, join, normalize, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

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
// The one deadline a page has. Pages wait on their own actors without one,
// except for an actor to start (see `openActor`).
const PAGE_DEADLINE_MS = 600_000;
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

function chromeExecutable() {
  const candidates = [
    process.env.CHROME,
    "/usr/bin/google-chrome",
    "/usr/bin/google-chrome-stable",
    "/usr/bin/chromium",
    "/usr/bin/chromium-browser",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    process.env.ProgramFiles && join(process.env.ProgramFiles, "Google/Chrome/Application/chrome.exe"),
    process.env.LOCALAPPDATA && join(process.env.LOCALAPPDATA, "Google/Chrome/Application/chrome.exe"),
  ];
  const found = candidates.find((candidate) => candidate && existsSync(candidate));
  if (found === undefined) throw new Error("no Chrome executable found; set CHROME");
  return found;
}

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
  const chrome = spawn(
    chromeExecutable(),
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
    { stdio: "ignore", windowsHide: true },
  );
  const exited = new Promise(resolveExit => chrome.once("close", resolveExit));
  const controller = new AbortController();
  const failed = code => controller.abort(new Error(`Chrome exited with ${code}`));
  const failedSpawn = error => controller.abort(error);
  chrome.once("exit", failed);
  chrome.once("error", failedSpawn);
  // Chrome's complete endpoint may follow its initial file creation event.
  const activePort = join(profile, "DevToolsActivePort");
  const [port, path] = await waitForFileValue(activePort, file => {
    try {
      const lines = readFileSync(file, "utf8").split("\n");
      return lines.length >= 2 && lines[1] !== "" ? lines : undefined;
    } catch (error) {
      if (error.code === "EBUSY" || error.code === "ENOENT") return undefined;
      throw error;
    }
  }, { timeoutMs: 30_000, signal: controller.signal }).catch(async error => {
    if (chrome.exitCode === null) chrome.kill();
    await exited;
    throw error;
  }).finally(() => {
    chrome.off("exit", failed);
    chrome.off("error", failedSpawn);
  });
  return { chrome, exited, endpoint: `ws://127.0.0.1:${port}${path.trim()}` };
}

async function launchFirefox(profile) {
  const driver = spawn(process.env.GECKODRIVER ?? "geckodriver", ["--port", "0"], {
    stdio: ["ignore", "pipe", "pipe"],
    windowsHide: true,
  });
  const exited = new Promise(resolveExit => driver.once("close", resolveExit));
  let logs = "";
  const endpoint = await new Promise((resolveEndpoint, reject) => {
    const timeout = setTimeout(() => reject(new Error(`geckodriver did not listen: ${logs}`)), 30_000);
    const receive = chunk => {
      logs += chunk.toString();
      const match = /Listening on (127\.0\.0\.1:\d+)/.exec(logs);
      if (match) { clearTimeout(timeout); resolveEndpoint(`http://${match[1]}`); }
    };
    driver.stdout.on("data", receive);
    driver.stderr.on("data", receive);
    driver.once("error", error => { clearTimeout(timeout); reject(error); });
    driver.once("exit", code => { clearTimeout(timeout); reject(new Error(`geckodriver exited ${code}: ${logs}`)); });
  }).catch(async error => {
    if (driver.exitCode === null) driver.kill();
    await exited;
    throw error;
  });
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
    const browser = await connect(session.capabilities.webSocketUrl);
    await browser.send("session.subscribe", { events: ["log.entryAdded"] });
    const observer = { events: [] };
    browser.listen(message => {
      if (message.method !== "log.entryAdded" || message.params.level !== "error") return;
      const uncaught = message.params.type === "javascript";
      observer.events.push(`${uncaught ? "uncaught" : "console.error"}: ${message.params.text}`);
      if (uncaught) observer.fail?.({ status: "failed", text: message.params.text });
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

async function runFirefoxPage(browser, observer, origin, page) {
  observer.events = [];
  const events = observer.events;
  const original = await browser.request("GET", "/window");
  const tab = await browser.request("POST", "/window/new", { type: "tab" });
  await browser.request("POST", "/window", { handle: tab.handle });
  let observed;
  const failure = new Promise(resolveFailure => { observer.fail = resolveFailure; });
  try {
    await browser.request("POST", "/timeouts", { script: PAGE_DEADLINE_MS });
    await browser.request("POST", "/url", { url: `${origin}/test/${page}` });
    observed = await Promise.race([failure, browser.request("POST", "/execute/async", {
      script: `(${waitForBrowserResult})(${PAGE_DEADLINE_MS}).then(arguments[arguments.length - 1]);`,
      args: [],
    })]);
    const uncaught = events.find(event => event.startsWith("uncaught:"));
    if (uncaught !== undefined) observed = { status: "failed", text: uncaught };
  } catch (error) {
    return { page, passed: false, text: String(error.stack ?? error), events };
  } finally {
    observer.fail = undefined;
    await browser.request("DELETE", "/window").catch(() => {});
    await browser.request("POST", "/window", { handle: original }).catch(() => {});
  }
  return { page, passed: observed.status === "passed", text: observed.text, events };
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
  let observed;
  try {
    observed = await chromiumPageResult(browser, `${origin}/test/${page}`, PAGE_DEADLINE_MS);
  } catch (error) {
    return { page, passed: false, text: String(error.stack ?? error), events };
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
  const browser = firefox ? launched.browser : await connect(launched.endpoint);
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
  // Chrome's helper processes can hold profile files briefly after the
  // browser itself exits; a profile that still cannot be removed is only
  // reported, since the qualification result is already known.
  try {
    rmSync(profile, { recursive: true, force: true, maxRetries: 3, retryDelay: 200 });
  } catch (error) {
    process.stderr.write(`could not remove browser profile ${profile}: ${error.message}
`);
  }
}
process.exit(failed ? 1 : 0);
