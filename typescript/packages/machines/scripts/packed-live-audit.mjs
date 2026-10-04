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
const { Machines } = await import(pathToFileURL(packageEntry));

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
let result;
try {
  process.env.ACYCLIC_MACHINES_ENDPOINT = connection.endpoint;
  process.env.ACYCLIC_MACHINES_CA_FILE = files.ca;
  process.env.ACYCLIC_MACHINES_CERT_FILE = files.certificate;
  process.env.ACYCLIC_MACHINES_KEY_FILE = files.privateKey;
  const machines = await Machines.fromEnv();
  const providerMethods = Object.getOwnPropertyNames(Object.getPrototypeOf(machines.provider))
    .filter((name) => name !== "constructor")
    .sort();
  const serviceMethods = providerMethods.filter((name) => name !== "recoverOperation");
  if (JSON.stringify(serviceMethods) !== JSON.stringify(expectedServiceMethods)) {
    throw new Error(`native service method set mismatch: ${serviceMethods.join(",")}`);
  }
  if (generatedClient.MACHINES_OPERATIONS === undefined ||
      Object.keys(generatedClient.MACHINES_OPERATIONS).length !== 19 ||
      generatedClient.MACHINES_SOURCE?.modeledOperations !== 19) {
    throw new Error("generated Machines operation metadata does not contain 19 operations");
  }

  const machine = machines.machine(connection.machineId);
  const inspect = await machine.inspect();
  const events = await machine.events();
  const usage = await machine.usage(10, 20);
  const operation = machines.operation(connection.operationId);
  const pending = await operation.inspect();
  const cancelled = await operation.cancel();
  const watchPhases = [];
  for await (const observation of operation.watch()) watchPhases.push(String(observation.phase).toLowerCase());
  if (String(inspect.state).toLowerCase() !== "running") throw new Error("inspect state mismatch");
  if (events.events?.[0]?.sequence !== 1) throw new Error("event sequence mismatch");
  if (usage.receipt === undefined) throw new Error("usage receipt missing");
  if (String(pending.phase).toLowerCase() !== "pending") throw new Error("pending operation mismatch");
  if (String(cancelled.phase).toLowerCase() !== "cancelled") throw new Error("cancelled operation mismatch");
  if (watchPhases.join(",") !== "pending,succeeded") throw new Error("watch stream mismatch");
  result = {
    packageEntry,
    modeledOperations: Object.keys(generatedClient.MACHINES_OPERATIONS).length,
    nativeServiceMethods: serviceMethods,
    nativeConvenienceMethods: providerMethods.filter((name) => !expectedServiceMethods.includes(name)),
    inspectState: inspect.state,
    eventSequence: events.events[0].sequence,
    usageReceiptBytes: usage.receipt.length,
    pendingPhase: pending.phase,
    cancelledPhase: cancelled.phase,
    watchPhases,
    outstandingFixtureRoutes: expectedServiceMethods.filter((name) =>
      !["cancel", "events", "inspectMachine", "inspectOperation", "usage", "watchOperation"].includes(name)),
  };
} finally {
  await fs.rm(tempRoot, { recursive: true, force: true });
}
console.log(JSON.stringify(result));
