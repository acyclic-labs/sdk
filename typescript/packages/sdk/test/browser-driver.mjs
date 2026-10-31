// Event-driven process and protocol ownership shared by browser qualification.
import { spawn } from "node:child_process";
import { createInterface } from "node:readline";
import { existsSync } from "node:fs";
import { join } from "node:path";

export function chromeExecutable(recovery) {
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
  if (found === undefined) throw new Error(`browser executable discovery: no Chrome executable found; recovery: ${recovery}`);
  return found;
}


export async function spawnDriver(command, args, ready, recovery) {
  const child = spawn(command, args, { stdio: ["ignore", "pipe", "pipe"], windowsHide: true });
  const exited = new Promise(resolve => child.once("close", resolve));
  const readers = [child.stdout, child.stderr].map(input => createInterface({ input }));
  try {
    const endpoint = await new Promise((resolve, reject) => {
      const fail = error => reject(new Error(`browser startup: ${error}; recovery: ${recovery}`));
      child.once("error", fail);
      child.once("close", (code, signal) => fail(`process closed (${code ?? signal}) before publishing its endpoint`));
      for (const reader of readers) reader.on("line", line => {
        const match = ready.exec(line);
        if (match) resolve(match[1]);
      });
    });
    return { child, exited, endpoint };
  } catch (error) {
    if (child.exitCode === null && child.signalCode === null) child.kill();
    await exited;
    throw error;
  } finally {
    for (const reader of readers) reader.close();
    // Keep draining diagnostics so a full pipe cannot block the driver.
    child.stdout.resume();
    child.stderr.resume();
  }
}

export async function connectBrowser(endpoint, recovery) {
  const socket = new WebSocket(endpoint);
  let sequence = 0;
  let failure;
  const pending = new Map();
  const sessions = new Map();
  const listeners = new Set();
  const eventWaiters = new Set();
  const fail = error => {
    failure ??= error;
    for (const handler of pending.values()) handler.reject(failure);
    pending.clear();
    sessions.clear();
    for (const waiter of eventWaiters) waiter.reject(failure);
    eventWaiters.clear();
  };
  socket.addEventListener("close", () => fail(new Error(`browser protocol: connection closed; recovery: ${recovery}`)));
  socket.addEventListener("error", () => fail(new Error(`browser protocol: connection failed; recovery: ${recovery}`)));
  await new Promise((resolve, reject) => {
    socket.addEventListener("open", resolve, { once: true });
    socket.addEventListener("close", () => reject(failure), { once: true });
    socket.addEventListener("error", () => reject(failure), { once: true });
  });
  socket.addEventListener("message", event => {
    let message;
    try { message = JSON.parse(event.data); }
    catch (error) { fail(new Error(`browser protocol decode: ${error.message}; recovery: ${recovery}`)); socket.close(); return; }
    if (message.id !== undefined) {
      const handler = pending.get(message.id);
      pending.delete(message.id);
      if (!handler) return;
      if (message.error === undefined) {
        if (handler.method === "Target.attachToTarget") sessions.set(message.result.sessionId, {});
        handler.resolve(message.result);
      }
      else handler?.reject(new Error(`browser ${handler.method}: ${typeof message.error === "string"
        ? `${message.error}: ${message.message}`
        : `${message.error.message} (${message.error.code})`}; recovery: ${recovery}`));
      return;
    }
    if (message.method === "Target.detachedFromTarget" || message.method === "Inspector.targetCrashed"
      || message.method === "Inspector.detached") {
      const sessionId = message.method === "Target.detachedFromTarget" ? message.params.sessionId : message.sessionId;
      const reason = message.method === "Inspector.targetCrashed" ? "target crashed" : "target detached";
      const error = new Error(`browser page: ${reason}; recovery: ${recovery}`);
      if (message.method === "Target.detachedFromTarget") sessions.delete(sessionId);
      else if (sessions.has(sessionId)) sessions.get(sessionId).failure = error;
      for (const [id, handler] of pending) if (handler.sessionId === sessionId) {
        pending.delete(id);
        handler.reject(error);
      }
      for (const waiter of eventWaiters) if (waiter.sessionId === sessionId) {
        eventWaiters.delete(waiter);
        waiter.reject(error);
      }
    }
    for (const waiter of eventWaiters) if (waiter.predicate(message)) {
      eventWaiters.delete(waiter);
      waiter.resolve(message);
    }
    for (const listener of listeners) listener(message);
  });
  const unavailable = sessionId => failure ?? (sessionId === undefined ? undefined
    : sessions.has(sessionId) ? sessions.get(sessionId).failure
    : new Error(`browser page: target detached; recovery: ${recovery}`));
  return {
    recovery,
    send(method, params = {}, sessionId = undefined) {
      const error = unavailable(sessionId);
      if (error) return Promise.reject(error);
      const id = ++sequence;
      return new Promise((resolve, reject) => {
        pending.set(id, { resolve, reject, sessionId, method });
        try { socket.send(JSON.stringify({ id, method, params, sessionId })); }
        catch (error) { pending.delete(id); reject(error); }
      });
    },
    event(predicate, sessionId) {
      let waiter;
      const promise = new Promise((resolve, reject) => {
        waiter = { predicate, resolve, reject, sessionId };
        const error = unavailable(sessionId);
        if (error) reject(error);
        else eventWaiters.add(waiter);
      });
      return { promise, cancel: () => eventWaiters.delete(waiter) };
    },
    listen(listener) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    close() { socket.close(); },
  };
}

// This runs before page scripts, so even an immediate failure has a producer.
function installResult() {
  globalThis.__sdkQualificationResult = new Promise(resolve => {
    const finish = value => {
      observer.disconnect();
      removeEventListener("error", failed, { capture: true });
      removeEventListener("unhandledrejection", rejected);
      resolve(value);
    };
    const failed = event => finish({ status: "failed", text: String(event.error?.stack ?? event.message ?? `resource failed: ${event.target?.src ?? event.target?.href}`) });
    const rejected = event => finish({ status: "failed", text: String(event.reason?.stack ?? event.reason) });
    const check = () => {
      const node = document.querySelector("#result");
      const status = node?.dataset.status ?? document.body?.dataset.result;
      if (status === "passed" || status === "failed" || status?.startsWith("failed:")) {
        finish({ status: status.startsWith("failed:") ? "failed" : status, text: node?.textContent ?? document.body.textContent });
      }
    };
    const observer = new MutationObserver(check);
    observer.observe(document, { childList: true, subtree: true, characterData: true, attributes: true, attributeFilter: ["data-status", "data-result"] });
    addEventListener("error", failed, { capture: true });
    addEventListener("unhandledrejection", rejected);
    check();
  });
  globalThis.__sdkQualificationResult.then(value => globalThis.__sdkQualificationComplete?.(JSON.stringify(value)));
}

export const RESULT_SCRIPT = `(${installResult.toString()})();`;

export async function navigateChrome(browser, sessionId, url) {
  await browser.send("Inspector.enable", {}, sessionId);
  await browser.send("Page.enable", {}, sessionId);
  await browser.send("Runtime.addBinding", { name: "__sdkQualificationComplete" }, sessionId);
  const { identifier } = await browser.send("Page.addScriptToEvaluateOnNewDocument", { source: RESULT_SCRIPT }, sessionId);
  const completed = browser.event(message => message.method === "Runtime.bindingCalled"
    && message.sessionId === sessionId && message.params.name === "__sdkQualificationComplete", sessionId);
  try {
    // Arm the result before navigation; a page can finish before its reply.
    const [, message] = await Promise.all([
      browser.send("Page.navigate", { url }, sessionId).then(navigation => {
        if (navigation.errorText) throw new Error(`browser navigation: ${navigation.errorText}; recovery: ${browser.recovery}`);
      }), completed.promise,
    ]);
    return JSON.parse(message.params.payload);
  } finally {
    completed.cancel();
    await browser.send("Page.removeScriptToEvaluateOnNewDocument", { identifier }, sessionId).catch(() => {});
    await browser.send("Runtime.removeBinding", { name: "__sdkQualificationComplete" }, sessionId).catch(() => {});
  }
}
