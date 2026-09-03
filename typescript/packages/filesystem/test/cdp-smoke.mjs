const endpoint = process.env.ACYCLIC_FS_CDP ?? "http://127.0.0.1:9223";
const deadline = Date.now() + 60_000;

let page;
while (page === undefined && Date.now() < deadline) {
  try {
    const targets = await fetch(`${endpoint}/json/list`).then((response) => response.json());
    page = targets.find((target) => target.type === "page" && target.url.includes("browser-smoke.html"));
  } catch {
    // Browser startup is asynchronous; the absolute deadline remains authoritative.
  }
  if (page === undefined) await new Promise((resolve) => setTimeout(resolve, 100));
}
if (page === undefined) throw new Error("browser smoke page did not expose a DevTools target");

const socket = new WebSocket(page.webSocketDebuggerUrl);
await new Promise((resolve, reject) => {
  socket.addEventListener("open", resolve, { once: true });
  socket.addEventListener("error", reject, { once: true });
});

let sequence = 0;
const pending = new Map();
socket.addEventListener("message", (event) => {
  const message = JSON.parse(event.data);
  if (message.id === undefined) return;
  const handler = pending.get(message.id);
  if (handler === undefined) return;
  pending.delete(message.id);
  if (message.error === undefined) handler.resolve(message.result);
  else handler.reject(new Error(message.error.message));
});

function command(method, params = {}) {
  const id = ++sequence;
  return new Promise((resolve, reject) => {
    pending.set(id, { resolve, reject });
    socket.send(JSON.stringify({ id, method, params }));
  });
}

let observed;
while (Date.now() < deadline) {
  const evaluated = await command("Runtime.evaluate", {
    expression: "JSON.stringify({status: document.querySelector('#result')?.dataset.status, text: document.querySelector('#result')?.textContent})",
    returnByValue: true,
  });
  observed = JSON.parse(evaluated.result.value);
  if (observed.status === "passed") break;
  if (observed.status === "failed") throw new Error(observed.text);
  await new Promise((resolve) => setTimeout(resolve, 100));
}
if (observed?.status !== "passed") throw new Error(`browser smoke timed out: ${JSON.stringify(observed)}`);
process.stdout.write(`${observed.text}\n`);
await command("Browser.close");
