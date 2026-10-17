import { afterAll } from "bun:test";
import { createSecureServer } from "node:http2";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { connectNodeAdapter } from "@connectrpc/connect-node";
import type { ConnectRouter } from "@connectrpc/connect";
import { ownFixtureServer } from "../../scripts/fixture-server.mjs";

let certificates: { directory: string; key: string; cert: string } | undefined;
afterAll(() => { if (certificates) rmSync(certificates.directory, { recursive: true, force: true }); });

export async function credentialTlsFixture(routes: (router: ConnectRouter) => void) {
  if (certificates === undefined) {
    const directory = mkdtempSync(join(tmpdir(), "client-credential-"));
    const keyPath = join(directory, "key.pem"), certPath = join(directory, "cert.pem");
    execFileSync("openssl", ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost", "-addext", "basicConstraints=critical,CA:FALSE", "-addext", "extendedKeyUsage=serverAuth", "-keyout", keyPath, "-out", certPath], { stdio: "ignore" });
    certificates = { directory, key: readFileSync(keyPath, "utf8"), cert: readFileSync(certPath, "utf8") };
  }
  const server = createSecureServer(certificates, connectNodeAdapter({ routes }));
  let connections = 0;
  server.on("session", () => { connections++; });
  const close = ownFixtureServer(server);
  await new Promise<void>(resolve => server.listen(0, "localhost", resolve));
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("missing fixture address");
  return { endpoint: `https://localhost:${address.port}`, certificate: certificates.cert, close, get connections() { return connections; } };
}
