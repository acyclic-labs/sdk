// Small HTTPS/HTTP2 fixture used only by the installed-package qualification.
// It serves the Actors gRPC routes directly so the package check does not
// contend for Cargo's global build lock while proving cancellation at the
// server stream boundary.
import { execFileSync } from "node:child_process";
import { createSecureServer } from "node:http2";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const caPath = process.env.ACYCLIC_CA_PATH;
if (!caPath) throw new Error("ACYCLIC_CA_PATH must name the CA output");
const certDir = mkdtempSync(join(tmpdir(), "cpp-actors-installed-fixture-"));
const keyPath = join(certDir, "key.pem");
const certPath = join(certDir, "cert.pem");
try {
  execFileSync("openssl", [
    "req", "-x509", "-newkey", "rsa:2048", "-nodes",
    "-keyout", keyPath, "-out", certPath, "-subj", "/CN=localhost",
    "-days", "1", "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
  ], { stdio: "ignore" });
} catch (error) {
  rmSync(certDir, { recursive: true, force: true });
  throw error;
}
const certificate = readFileSync(certPath, "utf8");
writeFileSync(caPath, certificate);
const token = "conformance";
let inspectCount = 0;
let closed = false;

const server = createSecureServer({
  key: readFileSync(keyPath),
  cert: readFileSync(certPath),
  allowHTTP1: false,
});

function varint(value) {
  const bytes = [];
  while (value > 127) {
    bytes.push((value & 127) | 128);
    value = Math.floor(value / 128);
  }
  bytes.push(value);
  return Buffer.from(bytes);
}
function field(number, value) {
  const payload = Buffer.isBuffer(value) ? value : Buffer.from(value);
  return Buffer.concat([varint(number << 3 | 2), varint(payload.length), payload]);
}
function actorObservation() {
  return Buffer.concat([
    field(1, Buffer.from("actor-a")),
    field(2, Buffer.alloc(32, 1)),
    field(3, Buffer.from("eu")),
    Buffer.from([0x20, 0x01]),
    Buffer.from([0x38, 0x09]),
  ]);
}
function actorResponse() {
  return field(1, actorObservation());
}
function invokeResponse() {
  const header = Buffer.concat([field(1, Buffer.from("location")), field(2, Buffer.from("/actors/actor-a"))]);
  return Buffer.concat([Buffer.from([0x08, 0xc9, 0x01]), field(3, header)]);
}
function grpcFrame(payload) {
  const length = Buffer.alloc(4);
  length.writeUInt32BE(payload.length);
  return Buffer.concat([Buffer.from([0]), length, payload]);
}
function grpcHeaders(extra = {}) {
  return { ":status": 200, "content-type": "application/grpc", "grpc-encoding": "identity", ...extra };
}

function finish(inspectAborted) {
  if (closed) return;
  closed = true;
  process.stderr.write(JSON.stringify({ inspectStarted: inspectCount >= 2, inspectAborted }) + "\n");
  server.close(() => {
    rmSync(certDir, { recursive: true, force: true });
    process.exit(inspectAborted ? 0 : 2);
  });
  setTimeout(() => process.exit(inspectAborted ? 0 : 2), 2000).unref();
}

server.on("stream", (stream, headers) => {
  stream.on("error", () => {});
  const path = String(headers[":path"] ?? "");
  const authorization = String(headers.authorization ?? "");
  if (authorization !== `Bearer ${token}`) {
    stream.respond(grpcHeaders({ "grpc-status": "7", "grpc-message": "missing bearer" }));
    stream.end();
    return;
  }
  if (path.endsWith("/inspect")) {
    inspectCount += 1;
    if (inspectCount === 2) {
      const onAbort = () => finish(true);
      stream.once("aborted", onAbort);
      stream.once("close", () => {
        if (stream.aborted || stream.rstCode !== 0) onAbort();
      });
      return;
    }
  }
  stream.on("data", () => {});
  stream.on("end", () => {
    stream.respond(grpcHeaders({ "grpc-status": "0" }));
    stream.end(grpcFrame(path.endsWith("/invoke") ? invokeResponse() : actorResponse()));
  });
});

server.listen(0, "127.0.0.1", () => {
  const address = server.address();
  process.stdout.write(JSON.stringify({ endpoint: `https://localhost:${address.port}`, token }) + "\n");
});

