// This function is serialized into the qualification page by both browser
// protocols. Keep it self-contained: the DOM is the page's result channel.
export function waitForBrowserResult(timeoutMs) {
  return new Promise(resolve => {
    let timer;
    const result = () => {
      const node = document.querySelector("#result");
      if (node !== null) return {
        status: node.dataset.status ?? null,
        text: node.textContent,
        waiting: node.dataset.waiting ?? null,
      };
      const text = document.body?.dataset.result;
      return text ? { status: text.startsWith("failed:") ? "failed" : text, text } : null;
    };
    const finish = value => {
      observer.disconnect();
      clearTimeout(timer);
      globalThis.removeEventListener("error", failed);
      globalThis.removeEventListener("unhandledrejection", failed);
      resolve(value);
    };
    const failed = event => {
      const error = event.error ?? event.reason ?? event.message;
      finish({ status: "failed", text: String(error?.stack ?? error) });
    };
    const changed = () => {
      const value = result();
      if (value?.status === "passed" || value?.status === "failed") finish(value);
    };
    const observer = new MutationObserver(changed);
    // Arm before observing the current value: both early completion and later
    // publication use the same channel, including replacement of the body.
    observer.observe(document, {
      subtree: true, attributes: true, childList: true, characterData: true,
    });
    globalThis.addEventListener("error", failed);
    globalThis.addEventListener("unhandledrejection", failed);
    timer = setTimeout(() => {
      const value = result();
      finish({ status: "failed", text: `timed out waiting for page result; the page was waiting for: ${value?.waiting || "nothing"}` });
    }, timeoutMs);
    changed();
  });
}

// CDP exposes the DOM publication through a binding installed before scripts.
// The caller supplies its existing protocol connection and may attach an
// external event (for example server-observed request cancellation) to this tab.
export async function chromiumPageResult(browser, url, timeoutMs, { onReady, onResult } = {}) {
  const { targetId } = await browser.send("Target.createTarget", { url: "about:blank" });
  const binding = "__acyclicQualificationResult";
  let finish, fail;
  const result = new Promise(resolve => { finish = resolve; });
  const terminal = new Promise((_, reject) => { fail = reject; });
  let active = true;
  const send = async (...args) => {
    if (!active) throw new Error("page observation retired");
    const value = await browser.send(...args);
    if (!active) throw new Error("page observation retired");
    return value;
  };
  const disconnected = () => fail(browser.signal.reason);
  if (browser.signal.aborted) disconnected();
  else browser.signal.addEventListener("abort", disconnected, { once: true });
  const timeout = setTimeout(() => fail(new Error(`timed out waiting for ${url} result`)), timeoutMs);
  let retire, unlisten;
  try {
    // Attach a rejection handler before protocol setup: an early page failure
    // can occur while a command acknowledgment is still in flight.
    return await Promise.race([terminal, (async () => {
      const { sessionId } = await send("Target.attachToTarget", { targetId, flatten: true });
      unlisten = browser.listen(message => {
        if (message.sessionId !== sessionId) return;
        if (message.method === "Runtime.bindingCalled" && message.params.name === binding) {
          try { finish(JSON.parse(message.params.payload)); }
          catch (error) { fail(error); }
        } else if (message.method === "Runtime.exceptionThrown") {
          const details = message.params.exceptionDetails;
          finish({ status: "failed", text: details.exception?.description ?? details.exception?.value ?? details.text });
        }
      });
      await send("Runtime.enable", {}, sessionId);
      await send("Runtime.addBinding", { name: binding }, sessionId);
      await send("Page.addScriptToEvaluateOnNewDocument", {
        source: `(${waitForBrowserResult})(${timeoutMs}).then(value => globalThis.${binding}(JSON.stringify(value)));`,
      }, sessionId);
      retire = onReady?.(sessionId, fail);
      const navigation = await send("Page.navigate", { url }, sessionId);
      if (navigation.errorText) throw new Error(navigation.errorText);
      const observed = await result;
      await onResult?.(observed);
      return observed;
    })()]);
  } finally {
    active = false;
    clearTimeout(timeout);
    browser.signal.removeEventListener("abort", disconnected);
    try {
      retire?.();
    } finally {
      try {
        unlisten?.();
      } finally {
        await browser.send("Target.closeTarget", { targetId }).catch(() => {});
      }
    }
  }
}
