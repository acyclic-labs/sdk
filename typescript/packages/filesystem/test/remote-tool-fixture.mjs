// Explicit test provider: real HTTP and fsynced external-effect receipts.
// This is not a Harness scheduler/store or a production transport policy.
import { closeSync, existsSync, fsyncSync, mkdtempSync, openSync, readFileSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, sep } from "node:path";

export function remoteToolFixture() {
  const parent = realpathSync(tmpdir());
  const directory = realpathSync(mkdtempSync(join(parent, "harness-http-tool-")));
  if (!directory.startsWith(`${parent}${sep}`)) throw new Error("fixture escaped its temporary parent");
  const counts = new Map(); // Diagnostics only; the files own intent/result.
  const durableWrite = (path, bytes) => {
    const fd = openSync(path, "wx");
    try { writeFileSync(fd, bytes); fsyncSync(fd); } finally { closeSync(fd); }
  };
  return {
    handle(request, response, pathname) {
      const match = /^\/__harness-fixture\/tool\/([0-9a-f-]{36})\/(execute|reconcile|inspect)$/.exec(pathname);
      if (!match) return false;
      const [, namespace, method] = match;
      const intent = join(directory, `${namespace}.intent`);
      const result = join(directory, `${namespace}.result`);
      const reply = (status, value) => response.writeHead(status, { "content-type": "application/json", connection: "close" }).end(JSON.stringify(value));
      if (method === "inspect") {
        reply(200, { executions: counts.get(namespace) ?? 0, intent: existsSync(intent), result: existsSync(result) });
        return true;
      }
      if (request.method !== "POST") { reply(405, null); return true; }
      let body = "";
      request.setEncoding("utf8");
      request.on("data", chunk => {
        body += chunk;
        if (Buffer.byteLength(body) > 65536) request.destroy(); // Declared fixture bound.
      });
      request.on("end", () => {
        try {
          const invocation = JSON.parse(body);
          if (existsSync(intent) && readFileSync(intent, "utf8") !== body) { reply(409, null); return; }
          if (method === "reconcile") {
            reply(200, existsSync(result) ? JSON.parse(readFileSync(result, "utf8")) : null);
            return;
          }
          if (existsSync(intent)) { reply(409, null); return; }
          durableWrite(intent, body);
          counts.set(namespace, (counts.get(namespace) ?? 0) + 1);
          durableWrite(result, JSON.stringify({ value: invocation.arguments.value }));
          request.socket.destroy(); // Deliberately lose the reply after receipt persistence.
        } catch { if (!response.destroyed) reply(500, null); }
      });
      return true;
    },
    close() { rmSync(directory, { recursive: true }); },
  };
}
