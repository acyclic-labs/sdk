// One command/event owner for browser WebSocket protocols (CDP and WebDriver
// BiDi). A terminal connection failure retires both pending commands and waits.
export async function connectBrowserProtocol(endpoint) {
  const socket = new WebSocket(endpoint);
  const controller = new AbortController();
  const pending = new Map();
  const listeners = new Set();
  let sequence = 0;
  const fail = error => {
    if (controller.signal.aborted) return;
    controller.abort(error);
    for (const { reject } of pending.values()) reject(error);
    pending.clear();
    listeners.clear();
  };
  socket.addEventListener("error", () => fail(new Error("browser protocol connection failed")));
  socket.addEventListener("close", event => fail(new Error(`browser protocol connection closed (${event.code})`)));
  socket.addEventListener("message", event => {
    try {
      const message = JSON.parse(event.data);
      if (message.id === undefined) {
        for (const listener of listeners) listener(message);
      } else {
        const handler = pending.get(message.id);
        pending.delete(message.id);
        if (message.error === undefined) handler?.resolve(message.result);
        else handler?.reject(new Error(typeof message.error === "string"
          ? `${message.error}: ${message.message}`
          : `${message.error.message} (${message.error.code})`));
      }
    } catch (error) { fail(error); }
  });
  await new Promise((resolve, reject) => {
    const failed = () => { socket.removeEventListener("open", opened); reject(controller.signal.reason); };
    const opened = () => { controller.signal.removeEventListener("abort", failed); resolve(); };
    if (controller.signal.aborted) failed();
    else {
      socket.addEventListener("open", opened, { once: true });
      controller.signal.addEventListener("abort", failed, { once: true });
    }
  });
  return {
    signal: controller.signal,
    send(method, params = {}, sessionId = undefined) {
      return new Promise((resolve, reject) => {
        if (controller.signal.aborted) { reject(controller.signal.reason); return; }
        const id = ++sequence;
        pending.set(id, { resolve, reject });
        try { socket.send(JSON.stringify({ id, method, params, sessionId })); }
        catch (error) { pending.delete(id); reject(error); }
      });
    },
    listen(listener) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    close() {
      fail(new Error("browser protocol owner closed"));
      socket.close();
    },
  };
}
