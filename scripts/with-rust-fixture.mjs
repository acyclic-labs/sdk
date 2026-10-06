#!/usr/bin/env node

// Run one consumer against a fresh Rust fixture process and the exact ordered
// request plan emitted by the Rust typed-manifest producer.  This is the
// qualification boundary: consumers receive request bytes and identities from
// Rust and must not synthesize setup calls or replace IDs with static values.

import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { createServer, Socket } from "node:net";
import { dirname, join, resolve } from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

const repo = resolve(fileURLToPath(new URL("..", import.meta.url)));
const windows = process.platform === "win32";
const defaultFixture = join(
  repo,
  "rust",
  "crates",
  "sdk-examples",
  "target",
  "debug",
  windows ? "fixture-server.exe" : "fixture-server",
);

function usage(message) {
  if (message) console.error(`error: ${message}`);
  console.error(
    "usage: node scripts/with-rust-fixture.mjs --manifest PATH --language NAME [--fixture PATH] [--receipt PATH] [--max-requests N] -- COMMAND [ARGS...]",
  );
  process.exit(2);
}

const separator = process.argv.indexOf("--");
const optionArgs = separator < 0 ? process.argv.slice(2) : process.argv.slice(2, separator);
const commandArgs = separator < 0 ? [] : process.argv.slice(separator + 1);
const options = new Map();
for (let index = 0; index < optionArgs.length; index += 1) {
  const value = optionArgs[index];
  if (!value.startsWith("--")) usage(`unexpected argument ${value}`);
  const key = value.slice(2);
  if (key === "require-source-binding") {
    options.set(key, true);
    continue;
  }
  const next = optionArgs[index + 1];
  if (!next || next.startsWith("--")) usage(`${value} requires a value`);
  options.set(key, next);
  index += 1;
}

const manifestPath = resolve(options.get("manifest") ?? "");
const language = options.get("language");
const fixturePath = resolve(options.get("fixture") ?? defaultFixture);
const receiptPath = resolve(options.get("receipt") ?? join(repo, "work", "rust-fixture-session.json"));
const buildReceiptPath = options.get("build-receipt") ? resolve(options.get("build-receipt")) : null;
const maxRequests = options.get("max-requests") ?? "512";
if (!options.get("manifest")) usage("--manifest is required");
if (!language) usage("--language is required");
if (!commandArgs.length) usage("a consumer command is required after --");
if (!existsSync(manifestPath)) usage(`manifest does not exist: ${manifestPath}`);

const manifestBytes = readFileSync(manifestPath);
const manifest = JSON.parse(manifestBytes.toString("utf8"));
if (manifest.complete !== true || !Array.isArray(manifest.execution_plan) || manifest.execution_plan.length === 0) {
  usage("manifest must contain a complete non-empty Rust execution_plan");
}
if (manifest.execution_plan_count !== manifest.execution_plan.length) {
  usage("manifest execution_plan_count does not match execution_plan length");
}
if (!existsSync(fixturePath)) usage(`fixture binary does not exist: ${fixturePath}`);
const fixtureBinarySha256 = `sha256:${createHash("sha256").update(readFileSync(fixturePath)).digest("hex")}`;
let buildReceipt = null;
if (buildReceiptPath) {
  if (!existsSync(buildReceiptPath)) usage(`build receipt does not exist: ${buildReceiptPath}`);
  buildReceipt = JSON.parse(readFileSync(buildReceiptPath, "utf8"));
  const boundRevision = buildReceipt.source_revision ?? buildReceipt.source?.revision;
  const boundBinary = buildReceipt.binary_sha256 ?? buildReceipt.fixture_binary_sha256;
  if (boundRevision !== manifest.source_revision) usage("build receipt source_revision does not match the Rust manifest");
  if (boundBinary !== fixtureBinarySha256) usage("build receipt binary_sha256 does not match the fixture executable");
} else if (options.has("require-source-binding")) {
  usage("--require-source-binding requires --build-receipt");
}

function hostPort(address) {
  return String(address).replace(/^https?:\/\//, "");
}

function writeReceipt(receipt) {
  mkdirSync(dirname(receiptPath), { recursive: true });
  writeFileSync(receiptPath, `${JSON.stringify(receipt, null, 2)}\n`, "utf8");
}

async function freePort(excluded = new Set()) {
  const server = createServer();
  await new Promise((resolvePort, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolvePort);
  });
  const port = server.address().port;
  await new Promise((resolveClose, reject) => server.close((error) => (error ? reject(error) : resolveClose())));
  if (excluded.has(port)) return freePort(excluded);
  return port;
}

async function waitForPort(port) {
  const deadline = Date.now() + 15000;
  while (Date.now() < deadline) {
    try {
      await new Promise((resolvePort, reject) => {
        const socket = new Socket();
        socket.setTimeout(250);
        socket.once("connect", () => {
          socket.destroy();
          resolvePort();
        });
        socket.once("timeout", () => {
          socket.destroy();
          reject(new Error("timeout"));
        });
        socket.once("error", reject);
        socket.connect(port, "127.0.0.1");
      });
      return;
    } catch {
      await new Promise((resolveDelay) => setTimeout(resolveDelay, 100));
    }
  }
  throw new Error(`fixture server did not open port ${port}`);
}

async function readReadiness(address) {
  const response = await fetch(`${address}/health`);
  if (!response.ok) {
    throw new Error(`fixture health probe failed with HTTP ${response.status}`);
  }
  const payload = await response.json();
  if (payload?.schema !== "acyclic.sdk.fixture-response.v1" || payload?.status !== "ready") {
    throw new Error("fixture health probe did not return the Rust readiness envelope");
  }
  return payload;
}

let fixture;
let consumer;
let readiness;
let fixtureStderr = "";
const startedAt = new Date().toISOString();
const finish = (exitCode) => {
  const receipt = {
    schema: "acyclic.sdk.rust-fixture-session.v1",
    status: exitCode === 0 ? "passed" : "failed",
    language,
    started_at: startedAt,
    finished_at: new Date().toISOString(),
    fixture_binary: fixturePath,
    fixture_binary_sha256: fixtureBinarySha256,
    build_receipt: buildReceiptPath,
    fixture_pid: fixture?.pid ?? null,
    consumer_command: commandArgs,
    consumer_exit_code: exitCode,
    readiness,
    manifest: {
      path: manifestPath,
      sha256: `sha256:${createHash("sha256").update(manifestBytes).digest("hex")}`,
      source_revision: manifest.source_revision ?? null,
      execution_plan_count: manifest.execution_plan_count,
      execution_plan_sha256: manifest.execution_plan_sha256 ?? null,
    },
    fixture_provenance: {
      source_revision: manifest.source_revision ?? null,
      source_sha256: readiness?.source?.sha256 ?? null,
      binary_sha256: fixtureBinarySha256,
      manifest_sha256: `sha256:${createHash("sha256").update(manifestBytes).digest("hex")}`,
      build_receipt: buildReceipt
        ? {
            source_revision: buildReceipt.source_revision ?? buildReceipt.source?.revision ?? null,
            binary_sha256: buildReceipt.binary_sha256 ?? buildReceipt.fixture_binary_sha256 ?? null,
          }
        : null,
      readiness: {
        source_sha256: readiness?.source?.sha256 ?? null,
        binary_sha256: fixtureBinarySha256,
      },
    },
    fixture_stderr: fixtureStderr.slice(-4000),
  };
  writeReceipt(receipt);
  console.log(JSON.stringify({ ...receipt, receipt: receiptPath }));
  return exitCode;
};

function stop(child) {
  if (child && !child.killed) child.kill();
}

process.once("SIGINT", () => {
  stop(consumer);
  stop(fixture);
  process.exit(130);
});
process.once("SIGTERM", () => {
  stop(consumer);
  stop(fixture);
  process.exit(143);
});

try {
  const httpPort = Number(options.get("port") ?? await freePort());
  const grpcPort = Number(options.get("grpc-port") ?? await freePort(new Set([httpPort])));
  if (!Number.isInteger(httpPort) || httpPort < 1 || httpPort > 65535) usage("--port must be a valid TCP port");
  if (!Number.isInteger(grpcPort) || grpcPort < 1 || grpcPort > 65535) usage("--grpc-port must be a valid TCP port");
  fixture = spawn(fixturePath, ["--bind-address", "127.0.0.1", "--port", String(httpPort), "--grpc-port", String(grpcPort), "--max-requests", maxRequests], {
    cwd: repo,
    // Pipe handles are rejected by some Windows runner sandboxes. The ports
    // are reserved above, so readiness can be established without consuming
    // the fixture's stdout as a control channel.
    stdio: "ignore",
    windowsHide: true,
  });
  await Promise.all([waitForPort(httpPort), waitForPort(grpcPort)]);
  readiness = {
    schema: "acyclic.sdk.fixture-server.v1",
    address: `http://127.0.0.1:${httpPort}`,
    grpc_address: `http://127.0.0.1:${grpcPort}`,
    max_requests: Number(maxRequests),
    source: null,
  };

  const grpcUrl = readiness.grpc_address;
  const httpUrl = readiness.address;
  const health = await readReadiness(httpUrl);
  readiness = {
    ...readiness,
    source: health.source ?? null,
    binary_sha256: fixtureBinarySha256,
  };
  const urlLanguage = new Set(["typescript", "csharp"]).has(language);
  const grpcAddress = urlLanguage ? grpcUrl : hostPort(grpcUrl);
  const env = {
    ...process.env,
    FIXTURE_GRPC_ADDRESS: grpcAddress,
    FIXTURE_HTTP_ADDRESS: httpUrl,
    ACYCLIC_RUST_FIXTURE_GRPC_ADDRESS: hostPort(grpcUrl),
    ACYCLIC_RUST_FIXTURE_GRPC_URL: grpcUrl,
    ACYCLIC_RUST_FIXTURE_HTTP_URL: httpUrl,
    ACYCLIC_RUST_FIXTURE_LANGUAGE: language,
    ACYCLIC_RUST_FIXTURE_FRESH: "1",
    ACYCLIC_RUST_FIXTURE_BINARY_SHA256: fixtureBinarySha256,
    ACYCLIC_RUST_FIXTURE_BUILD_RECEIPT: buildReceiptPath ?? "",
    ACYCLIC_RUST_TYPED_REQUEST_MANIFEST: manifestPath,
    ACYCLIC_RUST_CANONICAL_TYPED_REQUEST_MANIFEST: manifestPath,
  };
  consumer = spawn(commandArgs[0], commandArgs.slice(1), {
    cwd: repo,
    env,
    stdio: "inherit",
    windowsHide: true,
  });
  const code = await new Promise((resolveCode, reject) => {
    consumer.once("error", reject);
    consumer.once("exit", (exitCode, signal) => resolveCode(exitCode ?? (signal ? 1 : 0)));
  });
  stop(fixture);
  process.exitCode = finish(code);
} catch (error) {
  stop(consumer);
  stop(fixture);
  writeReceipt({
    schema: "acyclic.sdk.rust-fixture-session.v1",
    status: "failed_to_start",
    language,
    fixture_binary: fixturePath,
    fixture_binary_sha256: fixtureBinarySha256,
    build_receipt: buildReceiptPath,
    manifest: manifestPath,
    error: String(error?.stack ?? error),
  });
  console.error(String(error?.stack ?? error));
  process.exitCode = 1;
}
