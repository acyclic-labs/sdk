import { fork } from "node:child_process";
import { once } from "node:events";
import { createServer } from "node:http";
import { fileURLToPath } from "node:url";

// A separate process serves synchronous real npm invocations without borrowing
// their caller's event loop. IPC acknowledges setup, observations and shutdown.
export async function registryFixture(t) {
  const child = fork(fileURLToPath(import.meta.url), ["serve"], { stdio: ["ignore", "ignore", "inherit", "ipc"] });
  t.after(async () => {
    if (child.exitCode !== null || child.signalCode !== null) return;
    const exited = once(child, "exit");
    child.send({ command: "close" });
    await exited;
  });
  const [address] = await once(child, "message");
  const exchange = async message => {
    const received = once(child, "message");
    child.send(message);
    return (await received)[0];
  };
  return {
    url: address.url,
    set: (body, status = 200) => exchange({ command: "set", body, status }),
    requests: async () => (await exchange({ command: "requests" })).requests,
    observations: async () => (await exchange({ command: "requests" })).observations,
  };
}

if (process.argv[2] === "serve") {
  let body = null;
  let status = 404;
  let requests = 0;
  let observations = [];
  const server = createServer((request, response) => {
    requests += 1;
    observations.push({ method: request.method, path: request.url });
    response.writeHead(status, { "Content-Type": "application/json" });
    response.end(JSON.stringify(body));
  });
  process.on("message", message => {
    if (message.command === "close") {
      server.close(() => process.disconnect());
    } else {
      if (message.command === "set") {
        ({ body, status } = message);
        requests = 0;
        observations = [];
      }
      process.send({ requests, observations });
    }
  });
  server.listen(0, "127.0.0.1", () => process.send({ url: `http://127.0.0.1:${server.address().port}` }));
}
