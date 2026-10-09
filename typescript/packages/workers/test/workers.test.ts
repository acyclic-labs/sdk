import { describe, expect, test } from "bun:test";
import { once } from "node:events";
import { createSecureServer, type ServerHttp2Session, type ServerHttp2Stream } from "node:http2";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { WorkersClient } from "../src/index.js";
import { WorkersService } from "../generated/proto/workers/v1/workers_pb.js";

// The local gRPC peer uses maintained descriptors. The produced Rust binding
// owns configuration, request admission, and the actual client transport.
async function withPeer(run: (endpoint: string, seen: string[], caCertificate: string) => Promise<void>) {
  const identity = JSON.parse(execFileSync("cargo", ["run", "--offline", "--locked", "--quiet", "-p", "acyclic-actors", "--example", "conformance-certificate"], { cwd: fileURLToPath(new URL("../../../../", import.meta.url)), encoding: "utf8" }));
  const server = createSecureServer({ key: identity.key, cert: identity.certificate });
  const sessions = new Set<ServerHttp2Session>();
  const seen: string[] = [];
  server.on("session", session => { sessions.add(session); session.on("close", () => sessions.delete(session)); });
  server.on("stream", (stream: ServerHttp2Stream, headers) => {
    const chunks: Buffer[] = [];
    stream.on("data", chunk => chunks.push(Buffer.from(chunk)));
    stream.on("end", () => {
      const name = String(headers[":path"]).split("/").at(-1);
      const method = WorkersService.methods.find(method => method.name === name)!;
      const request = fromBinary(method.input, Buffer.concat(chunks).subarray(5));
      seen.push(method.name);
      expect(headers.authorization).toBe("Bearer local");
      if (method.name === "InvokeVersion") expect("versionSha256" in request && request.versionSha256).toEqual(new Uint8Array(32).fill(1));
      if (method.name === "InvokeDeployment") expect("alias" in request && request.alias).toBe("current");
      const body = toBinary(method.output, create(method.output, {
        status: 200, resolvedSha256: new Uint8Array(32).fill(1),
        ...(method.name === "InvokeDeployment" ? { resolvedRevision: 4n } : {}),
      }));
      const frame = Buffer.alloc(5 + body.length);
      frame.writeUInt32BE(body.length, 1);
      frame.set(body, 5);
      stream.respond({ ":status": 200, "content-type": "application/grpc" }, { waitForTrailers: true });
      stream.on("wantTrailers", () => stream.sendTrailers({ "grpc-status": "0" }));
      stream.end(frame);
    });
  });
  server.listen(0, "localhost");
  await once(server, "listening");
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("local peer address missing");
  try { await run(`https://localhost:${address.port}`, seen, identity.certificate); }
  finally { for (const session of sessions) session.destroy(); await new Promise<void>(resolve => server.close(() => resolve())); }
}

describe("Workers Rust-backed transport", () => {
  test("admits a trusted TLS peer and rejects plaintext through Rust configuration", async () => {
    await withPeer(async (endpoint, _seen, caCertificate) => {
      expect(await new WorkersClient({ endpoint, caCertificate, token: "local", transport: "native" }).transport).toBe("grpc");
    });
    await expect(new WorkersClient({ endpoint: "http://workers.example.test", token: "remote", transport: "native" }).transport).rejects.toMatchObject({ code: "invalid_argument" });
  });
  test("rejects header-unsafe and oversized credentials through Rust configuration", async () => {
    for (const token of [" ", "a\nb", "a\rb", "a\0b", "x".repeat(12 * 1024 + 1)]) {
      await expect(new WorkersClient({ endpoint: "https://workers.example.test", token, transport: "native" }).transport).rejects.toMatchObject({ code: "invalid_argument" });
    }
  });
  test("rejects path-like deployment aliases before dispatch", async () => {
    await withPeer(async (endpoint, seen, caCertificate) => {
      const client = new WorkersClient({ endpoint, caCertificate, token: "local", transport: "native" });
      await expect(client.invokeDeployment({ alias: "..", method: "GET", url: "https://example.test/", headers: [], body: new Uint8Array() })).rejects.toMatchObject({ code: "invalid_argument" });
      expect(seen).toEqual([]);
    });
  });
  test("dispatches pinned and alias invocations to distinct maintained RPCs", async () => {
    await withPeer(async (endpoint, seen, caCertificate) => {
      const client = new WorkersClient({ endpoint, caCertificate, token: "local", transport: "native" });
      const common = { method: "GET", url: "https://example.test/", headers: [], body: new Uint8Array() };
      const pinned = await client.invokeVersion({ ...common, versionSha256: new Uint8Array(32).fill(1) });
      const alias = await client.invokeDeployment({ ...common, alias: "current" });
      expect(pinned.resolvedRevision).toBeUndefined();
      expect(alias.resolvedRevision).toBe(4n);
      expect(seen).toEqual(["InvokeVersion", "InvokeDeployment"]);
    });
  });
});
