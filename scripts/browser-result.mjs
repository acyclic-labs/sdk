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
