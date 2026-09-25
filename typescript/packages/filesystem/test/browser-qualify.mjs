// Runs the browser qualification pages in a private headless Chrome and
// reports each page's result, with every uncaught exception and console error
// each page raised while it ran.
//
// Nothing here waits on a guess: the file server is listening before Chrome
// starts, Chrome publishes its own DevTools endpoint in its profile, and each
// page is navigated only once the driver is observing it.
//
// Usage: node test/browser-qualify.mjs [page ...]   (default: every page)
// CHROME selects the browser executable.
import { spawn } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync, statSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { extname, join, normalize, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const PAGES = ["browser-smoke.html", "browser-multitab.html"];
// The one deadline a page has; pages wait on their own actors without one.
const PAGE_DEADLINE_MS = 600_000;
const root = resolve(fileURLToPath(new URL("..", import.meta.url)));
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

async function serve() {
  const server = createServer((request, response) => {
    const path = normalize(join(root, decodeURIComponent(new URL(request.url, "http://host").pathname)));
    if (!path.startsWith(root + sep) || !existsSync(path) || !statSync(path).isFile()) {
      response.writeHead(404).end();
      return;
    }
    response.writeHead(200, { "content-type": types[extname(path)] ?? "application/octet-stream" });
    response.end(readFileSync(path));
  });
  await new Promise((resolveListen, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolveListen);
  });
  return server;
}

async function until(deadline, description, probe) {
  for (;;) {
    const value = await probe();
    if (value !== undefined) return value;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${description}`);
    await new Promise((resolveDelay) => setTimeout(resolveDelay, 50));
  }
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
    { stdio: "ignore" },
  );
  const exited = new Promise((resolveExit) => chrome.once("exit", resolveExit));
  // Chrome writes its chosen port and browser endpoint here once it listens.
  const activePort = join(profile, "DevToolsActivePort");
  const [port, path] = await until(Date.now() + 30_000, "Chrome DevTools endpoint", () => {
    if (chrome.exitCode !== null) throw new Error(`Chrome exited with ${chrome.exitCode}`);
    if (!existsSync(activePort)) return undefined;
    const lines = readFileSync(activePort, "utf8").split("\n");
    return lines.length >= 2 && lines[1] !== "" ? lines : undefined;
  });
  return { chrome, exited, endpoint: `ws://127.0.0.1:${port}${path.trim()}` };
}

async function connect(endpoint) {
  const socket = new WebSocket(endpoint);
  await new Promise((resolveOpen, reject) => {
    socket.addEventListener("open", resolveOpen, { once: true });
    socket.addEventListener("error", () => reject(new Error("DevTools connection failed")), { once: true });
  });
  let sequence = 0;
  const pending = new Map();
  const listeners = new Set();
  socket.addEventListener("message", (event) => {
    const message = JSON.parse(event.data);
    if (message.id === undefined) {
      for (const listener of listeners) listener(message);
      return;
    }
    const handler = pending.get(message.id);
    pending.delete(message.id);
    if (message.error === undefined) handler?.resolve(message.result);
    else handler?.reject(new Error(`${message.error.message} (${message.error.code})`));
  });
  return {
    send(method, params = {}, sessionId = undefined) {
      const id = ++sequence;
      return new Promise((resolveSend, reject) => {
        pending.set(id, { resolve: resolveSend, reject });
        socket.send(JSON.stringify({ id, method, params, sessionId }));
      });
    },
    listen(listener) {
      listeners.add(listener);
    },
    close() {
      socket.close();
    },
  };
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
  await browser.send("Page.navigate", { url: `${origin}/test/${page}` }, sessionId);
  const deadline = Date.now() + PAGE_DEADLINE_MS;
  let observed;
  let last;
  try {
    observed = await until(deadline, `${page} result`, async () => {
      const evaluated = await browser.send(
        "Runtime.evaluate",
        {
          expression: "(() => { const node = document.querySelector('#result'); return node === null ? null : { status: node.dataset.status ?? null, text: node.textContent, waiting: node.dataset.waiting ?? null }; })()",
          returnByValue: true,
        },
        sessionId,
      );
      const value = evaluated.result.value;
      last = value;
      return value?.status === "passed" || value?.status === "failed" ? value : undefined;
    }).catch((error) => {
      throw new Error(`${error.message}; the page was waiting for: ${last?.waiting || "nothing"}`);
    });
  } finally {
    await browser.send("Target.closeTarget", { targetId }).catch(() => {});
  }
  return { page, passed: observed.status === "passed", text: observed.text, events };
}

const pages = process.argv.length > 2 ? process.argv.slice(2) : PAGES;
const server = await serve();
const origin = `http://127.0.0.1:${server.address().port}`;
const profile = mkdtempSync(join(tmpdir(), "acyclic-fs-browser-"));
const { chrome, exited, endpoint } = await launchChrome(profile);
let failed = false;
try {
  const browser = await connect(endpoint);
  const observer = observe(browser);
  for (const page of pages) {
    const outcome = await runPage(browser, observer, origin, page).catch((error) => ({
      page,
      passed: false,
      text: String(error.stack ?? error),
      events: [],
    }));
    failed ||= !outcome.passed;
    process.stdout.write(`${outcome.passed ? "PASS" : "FAIL"} ${page}: ${outcome.text}\n`);
    for (const event of outcome.events) process.stdout.write(`  ${event}\n`);
  }
  await browser.send("Browser.close").catch(() => {});
  browser.close();
} finally {
  if (chrome.exitCode === null) chrome.kill();
  await exited;
  server.close();
  // Chrome's helper processes can hold profile files briefly after the
  // browser itself exits; a profile that still cannot be removed is only
  // reported, since the qualification result is already known.
  try {
    rmSync(profile, { recursive: true, force: true, maxRetries: 50, retryDelay: 200 });
  } catch (error) {
    process.stderr.write(`could not remove browser profile ${profile}: ${error.message}
`);
  }
}
process.exit(failed ? 1 : 0);
