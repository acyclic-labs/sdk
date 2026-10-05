import { createHash } from "node:crypto";
import { promises as fs } from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";

const connectionFile = process.env.MACHINES_CONNECTION_FILE;
if (connectionFile === undefined) throw new Error("MACHINES_CONNECTION_FILE is required");

const connectionText = await fs.readFile(connectionFile, "utf8");
const connectionLine = connectionText
  .split(/\r?\n/)
  .find((line) => line.startsWith("machines fixture connection "));
const connection = connectionLine === undefined
  ? JSON.parse(connectionText)
  : JSON.parse(connectionLine.slice("machines fixture connection ".length));

const consumerRequire = createRequire(path.join(process.cwd(), "package.json"));
const packageEntry = consumerRequire.resolve("@acyclic-labs/machines");
const packageRoot = path.dirname(path.dirname(packageEntry));
const generatedClient = await import(pathToFileURL(path.join(packageRoot, "dist", "generated-client.js")));
const { Machines, idempotencyKey } = await import(pathToFileURL(packageEntry));

async function packageDigest(root) {
  const files = [];
  async function visit(directory) {
    for (const entry of await fs.readdir(directory, { withFileTypes: true })) {
      const file = path.join(directory, entry.name);
      if (entry.isDirectory()) await visit(file);
      else if (entry.isFile()) files.push(file);
      else if (entry.isSymbolicLink()) throw new Error(`installed package contains a symlink: ${file}`);
    }
  }
  await visit(root);
  files.sort();
  const digest = createHash("sha256");
  for (const file of files) {
    digest.update(path.relative(root, file).replaceAll(path.sep, "/"));
    digest.update("\0");
    digest.update(await fs.readFile(file));
    digest.update("\0");
  }
  return `sha256:${digest.digest("hex")}`;
}

const tempRoot = await fs.mkdtemp(path.join(process.env.TEMP ?? process.cwd(), "acyclic-machines-live-"));
const files = {
  ca: path.join(tempRoot, "ca.pem"),
  certificate: path.join(tempRoot, "client.pem"),
  privateKey: path.join(tempRoot, "client-key.pem"),
};
await fs.writeFile(files.ca, connection.caCertificate, "utf8");
await fs.writeFile(files.certificate, connection.certificate, "utf8");
await fs.writeFile(files.privateKey, connection.privateKey, "utf8");

const expectedServiceMethods = [
  "cancel", "checkpoint", "create", "destroyCheckpoint", "destroyMachine", "events",
  "fork", "forkMachine", "inspectCheckpoint", "inspectMachine", "inspectOperation",
  "listMachines", "qualifyImage", "recover", "setSuspensionPolicy", "suspend", "usage",
  "wake", "watchOperation",
].sort();
const expectedRpcs = connection.expectedRpcs ?? [];
if (expectedRpcs.length !== 19) throw new Error(`Rust fixture must advertise 19 RPCs, got ${expectedRpcs.length}`);
let result;
try {
  process.env.ACYCLIC_MACHINES_ENDPOINT = connection.endpoint;
  process.env.ACYCLIC_MACHINES_CA_FILE = files.ca;
  process.env.ACYCLIC_MACHINES_CERT_FILE = files.certificate;
  process.env.ACYCLIC_MACHINES_KEY_FILE = files.privateKey;
  const machines = await Machines.fromEnv();
  const provider = machines.provider;
  const providerMethods = Object.getOwnPropertyNames(Object.getPrototypeOf(provider))
    .filter((name) => name !== "constructor")
    .sort();
  const serviceMethods = providerMethods.filter((name) => name !== "recoverOperation");
  if (JSON.stringify(serviceMethods) !== JSON.stringify(expectedServiceMethods)) {
    throw new Error(`native service method set mismatch: ${serviceMethods.join(",")}`);
  }
  const modeledRpcs = generatedClient.MACHINES_OPERATIONS === undefined
    ? []
    : Object.values(generatedClient.MACHINES_OPERATIONS).map((operation) => operation.rpc).sort();
  if (modeledRpcs.length !== 19 ||
      generatedClient.MACHINES_SOURCE?.modeledOperations !== 19 ||
      JSON.stringify(modeledRpcs) !== JSON.stringify([...expectedRpcs].sort())) {
    throw new Error("generated Machines operation metadata does not match the Rust fixture RPC identities");
  }

  const request = (key) => ({
    idempotencyKey: idempotencyKey(key),
    image: { kind: "custom", digestHex: "07".repeat(32) },
    compatibility: { kind: "best-effort" },
    suspension: { kind: "after-idle", milliseconds: 15_000 },
    expiration: { kind: "never" },
    networkPolicyDigestHex: "08".repeat(32),
    budgets: { spendMicros: 0n, concurrency: 0 },
  });
  const image = { kind: "custom", digestHex: "07".repeat(32) };
  const qualification = await provider.qualifyImage(image);
  const created = await provider.create(request("live-create"));
  if (created.kind !== "created") throw new Error(`create outcome mismatch: ${created.kind}`);
  const machineId = created.machine.id;
  const inspected = await provider.inspectMachine(machineId);
  const checkpointed = await provider.checkpoint(machineId, idempotencyKey("live-checkpoint"));
  if (checkpointed.kind !== "checkpointed") throw new Error(`checkpoint outcome mismatch: ${checkpointed.kind}`);
  const checkpointId = checkpointed.checkpoint.id;
  const checkpointInspection = await provider.inspectCheckpoint(checkpointId);
  const forked = await provider.fork(checkpointId, 1, idempotencyKey("live-fork"));
  if (forked.kind !== "forked") throw new Error(`fork outcome mismatch: ${forked.kind}`);
  const liveFork = await provider.forkMachine(machineId, 1, idempotencyKey("live-fork-machine"));
  if (liveFork.kind !== "machine-forked") throw new Error(`forkMachine outcome mismatch: ${liveFork.kind}`);
  const suspended = await provider.suspend(machineId, idempotencyKey("live-suspend"));
  if (suspended.kind !== "suspended") throw new Error(`suspend outcome mismatch: ${suspended.kind}`);
  const woken = await provider.wake(machineId, idempotencyKey("live-wake"));
  if (woken.kind !== "woken") throw new Error(`wake outcome mismatch: ${woken.kind}`);
  const policy = await provider.setSuspensionPolicy(machineId, { kind: "manual" }, idempotencyKey("live-policy"));
  if (policy.kind !== "suspension-policy-set") throw new Error(`policy outcome mismatch: ${policy.kind}`);
  const page = await provider.listMachines(null, 256);
  const events = await provider.events(machineId, null, 256);
  const usage = await provider.usage(machineId, 10, 20);
  const recovered = await provider.recover(idempotencyKey("live-create"));
  if (recovered.kind !== "created") throw new Error(`recover outcome mismatch: ${recovered.kind}`);
  const operationId = await provider.recoverOperation(idempotencyKey("live-create"));
  const pending = await provider.inspectOperation(operationId);
  const cancelled = await provider.cancel(operationId);
  const watchPhases = [];
  for await (const observation of provider.watchOperation(operationId)) watchPhases.push(String(observation.phase).toLowerCase());
  const childIds = [
    forked.machines?.[0]?.id,
    liveFork.children?.[0]?.id,
  ].filter((id) => id !== undefined);
  if (childIds.length === 0) throw new Error("Rust fixture did not return a child machine for destroyMachine qualification");
  await provider.destroyMachine(childIds[0], idempotencyKey("live-destroy-child"));
  const destroyedCheckpoint = await provider.destroyCheckpoint(checkpointId, idempotencyKey("live-destroy-checkpoint"));
  if (destroyedCheckpoint.kind !== "checkpoint-destroyed") throw new Error(`destroyCheckpoint outcome mismatch: ${destroyedCheckpoint.kind}`);

  const packageTreeDigest = await packageDigest(packageRoot);
  result = {
    schema: "acyclic.sdk.typescript.machines-installed-live.v2",
    packageEntry,
    packageVersion: (JSON.parse(await fs.readFile(path.join(packageRoot, "package.json"), "utf8"))).version,
    packageTreeDigest,
    packageInstallationRoot: packageRoot,
    rustFixture: {
      schema: connection.schema,
      endpoint: connection.endpoint,
      sourceSha256: connection.sourceSha256,
      buildTarget: connection.buildTarget,
      tls: connection.tls,
      expectedRpcs,
    },
    modeledOperations: Object.keys(generatedClient.MACHINES_OPERATIONS).length,
    nativeServiceMethods: serviceMethods,
    operationEvidence: {
      qualifyImage: qualification.image,
      create: created.kind,
      checkpoint: checkpointed.kind,
      fork: forked.kind,
      forkMachine: liveFork.kind,
      suspend: suspended.kind,
      wake: woken.kind,
      setSuspensionPolicy: policy.kind,
      destroyMachine: childIds.length > 0 ? "machine-destroyed" : "not-run-no-child",
      destroyCheckpoint: destroyedCheckpoint.kind,
      recover: recovered.kind,
      inspectMachine: inspected.state,
      inspectCheckpoint: checkpointInspection.id,
      listMachines: page.machines.length,
      events: events.events.length,
      usage: usage.receipt?.length ?? 0,
      cancel: cancelled.phase,
      inspectOperation: pending.phase,
      watchOperation: watchPhases,
    },
    operationId,
    packageAuth: "mutual-tls",
    streamedRecovery: watchPhases.length > 0,
  };
} finally {
  await fs.rm(tempRoot, { recursive: true, force: true });
}
console.log(JSON.stringify(result));


