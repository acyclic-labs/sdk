import {
  Machines, HttpMachinesProvider, machineId, idempotencyKey, checkpointId, operationId, managedOci,
  type Budgets, type Capability, type CompatibilityPolicy, type CreateMachine, type ExpirationPolicy,
  type Image, type MachineEvent, type MachinePage, type MutationOutcome, type SuspensionPolicy,
} from "@acyclic-labs/machines";
const mid = machineId("machine-1");
const key = idempotencyKey("request-1");
const cid = checkpointId("checkpoint-1");
const oid = operationId("operation-1");
const image: Image = { kind: "custom", digestHex: "07".repeat(64) };
const capability: Capability = "live-checkpoint";
const compatibility: CompatibilityPolicy = { kind: "require", capabilities: [capability] };
const suspension: SuspensionPolicy = { kind: "after-idle", milliseconds: 1000 };
const expiration: ExpirationPolicy = { kind: "max-age", milliseconds: 2000 };
const budgets: Budgets = { spendMicros: 0n, concurrency: 1 };
const create: CreateMachine = { idempotencyKey: key, image, compatibility, suspension, expiration, networkPolicyDigestHex: "08".repeat(64), budgets };
const page: MachinePage = { machines: [], next: mid };
const outcome: MutationOutcome = { kind: "machine-destroyed", machineId: mid };
const event: MachineEvent = { machine: mid, sequence: 1, observedAtUnixMs: 2, fact: { kind: "state", state: "running" } };
const provider = new HttpMachinesProvider({ endpoint: "https://example.test/api", token: "token" });
const facade = Machines.fromEnv({ endpoint: "https://example.test", token: "token" });
const managed: Image = managedOci("registry.example/image@sha256:" + "0a".repeat(32));
void [cid, oid, create, page, outcome, event, provider, facade, managed];
