// Headless Chromium qualification over its pushed CDP pipe; no polling.
import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { existsSync, createReadStream } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join, resolve, sep } from "node:path";
import { publicationCommand as callerCommand } from "./registry-publication.mjs";

export class BrowserSourceEnded extends Error {
  constructor(source, cause) {
    super(`step=browser-${source} ${cause.message}; recovery command: ${callerCommand()}`, { cause });
    this.name = "BrowserSourceEnded";
    this.code = "BROWSER_SOURCE_ENDED";
    this.source = source;
  }
}

/** Process creation can fail without an exit event. */
export function childExit(child) {
  return new Promise(resolve => { child.once("exit", resolve); child.once("error", resolve); });
}

/** Every registered source wait ends at the same terminal failure. */
export function rejectWaits(error, ...registries) {
  for (const waits of registries) {
    for (const { reject } of waits.values()) reject(error);
    waits.clear();
  }
}

/** Startup failures can precede a page's own result reporter. */
export function pageFailure(message) {
  if (message.method === "Runtime.exceptionThrown") {
    const detail = message.params.exceptionDetails;
    return new BrowserSourceEnded("javascript", new Error(detail.exception?.description ?? detail.text));
  }
  if (message.method === "Network.loadingFailed" && message.params.type === "Script") {
    return new BrowserSourceEnded("module", new Error(message.params.errorText));
  }
  if (message.method === "Network.responseReceived" && message.params.type === "Script"
    && message.params.response.status >= 400) {
    const response = message.params.response;
    return new BrowserSourceEnded("module", new Error(`HTTP ${response.status}: ${response.url}`));
  }
}

export async function browserTest(module, args) {
  const root = resolve(import.meta.dirname, "..");
  const page = module.endsWith(".html");
  const executable = [process.env.CHROME, "/usr/bin/google-chrome", "/usr/bin/google-chrome-stable", "/usr/bin/chromium", "/usr/bin/chromium-browser"].find(path => path && existsSync(path));
  if (!executable) throw new BrowserSourceEnded("executable", new Error("no Chromium executable; set CHROME"));
  const server = createServer((request, response) => {
    const path = new URL(request.url, "http://localhost").pathname;
    if (path === "/") { response.setHeader("content-type", "text/html"); response.end("<!doctype html><title>Browser qualification</title>"); return; }
    const file = resolve(root, `.${decodeURIComponent(path)}`);
    if (!file.startsWith(`${root}${sep}`) || !existsSync(file)) { response.writeHead(404).end(); return; }
    response.setHeader("content-type", file.endsWith(".wasm") ? "application/wasm"
      : file.endsWith(".html") ? "text/html" : "text/javascript");
    createReadStream(file).pipe(response);
  });
  await new Promise((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
  const profile = await mkdtemp(join(tmpdir(), "acyclic-browser-test-"));
  const chrome = spawn(executable, ["--headless=new", "--no-sandbox", "--remote-debugging-pipe", "--no-first-run", `--user-data-dir=${profile}`, "about:blank"], { stdio: ["ignore", "ignore", "pipe", "pipe", "pipe"], windowsHide: true });
  const exited = childExit(chrome);
  const pending = new Map(), events = new Map();
  let sequence = 0, buffer = "", errors = "";
  chrome.stderr.on("data", data => { errors += data; });
  let failure;
  const fail = error => {
    if (failure) return;
    failure = error;
    rejectWaits(failure, pending, events);
  };
  chrome.once("error", error => fail(new BrowserSourceEnded("process", error)));
  chrome.once("exit", code => fail(new BrowserSourceEnded("process", new Error(`Chromium exited ${code}: ${errors}`))));
  for (const index of [3, 4]) {
    const pipe = chrome.stdio[index];
    pipe.once("error", error => fail(new BrowserSourceEnded("pipe", error)));
    pipe.once("close", () => fail(new BrowserSourceEnded("pipe", new Error(`CDP pipe ${index} closed`))));
  }
  chrome.stdio[4].once("end", () => fail(new BrowserSourceEnded("pipe", new Error("CDP producer reached EOF"))));
  chrome.stdio[4].on("data", data => {
    if (failure) return;
    buffer += data;
    try {
      for (;;) {
        const end = buffer.indexOf("\0");
        if (end < 0) break;
        const message = JSON.parse(buffer.slice(0, end)); buffer = buffer.slice(end + 1);
        if (message.id) {
          const result = pending.get(message.id); pending.delete(message.id);
          if (message.error) result?.reject(new Error(JSON.stringify(message.error)));
          else result?.resolve(message.result);
        } else {
          const error = pageFailure(message);
          if (error) { fail(error); return; }
          if (message.method === "Target.detachedFromTarget" || message.method === "Inspector.targetCrashed"
            || message.method === "Inspector.detached") {
            const reason = message.method === "Inspector.targetCrashed" ? "page crashed" : "page detached";
            fail(new BrowserSourceEnded("page", new Error(reason)));
            return;
          }
          const key = `${message.sessionId ?? ""}:${message.method}`;
          events.get(key)?.resolve(message.params); events.delete(key);
        }
      }
    } catch (error) { fail(new BrowserSourceEnded("message", error)); }
  });
  const send = (method, params = {}, sessionId) => new Promise((resolve, reject) => {
    if (failure) { reject(failure); return; }
    const id = ++sequence; pending.set(id, { resolve, reject });
    try { chrome.stdio[3].write(`${JSON.stringify({ id, method, params, sessionId })}\0`); }
    catch (error) { fail(new BrowserSourceEnded("pipe", error)); }
  });
  try {
    const { targetId } = await send("Target.createTarget", { url: "about:blank" });
    const { sessionId } = await send("Target.attachToTarget", { targetId, flatten: true });
    await send("Inspector.enable", {}, sessionId);
    await send("Page.enable", {}, sessionId);
    await send("Runtime.enable", {}, sessionId);
    await send("Network.enable", {}, sessionId);
    const loaded = new Promise((resolve, reject) => {
      if (failure) reject(failure);
      else events.set(`${sessionId}:Page.loadEventFired`, { resolve, reject });
    });
    await Promise.all([send("Page.navigate", { url: `http://127.0.0.1:${server.address().port}/${page ? module : ""}` }, sessionId), loaded]);
    const expression = page ? `new Promise((resolve, reject) => {
      const result = document.querySelector("#result");
      if (!result) throw new Error("browser page has no result");
      const observe = () => {
        const status = result.dataset.status;
        if (status === "passed" || status === "failed") {
          observer.disconnect();
          if (status === "passed") resolve(result.textContent);
          else reject(new Error(result.textContent));
        }
      };
      const observer = new MutationObserver(observe);
      observer.observe(result, { attributes: true, childList: true, subtree: true });
      observe();
    })` : `import(${JSON.stringify(`/${module}`)}).then(module => module.test(${JSON.stringify(args)}))`;
    const result = await send("Runtime.evaluate", { expression, awaitPromise: true, returnByValue: true }, sessionId);
    if (result.exceptionDetails) throw new Error(result.exceptionDetails.exception?.description ?? JSON.stringify(result.exceptionDetails));
    return result.result.value;
  } finally {
    if (chrome.exitCode === null) chrome.kill();
    await exited; server.closeAllConnections(); server.close();
    await rm(profile, { recursive: true, force: true }).catch(error => console.error(`profile cleanup: ${error.message}`));
  }
}
