// Generated from Rust scenario machines/typescript-consumer.
// Rust output SHA256: sha256:da352173c5e5a3795dc8bc41ef0a712c99992531e557b81046a43331a222fe6e
import { SimulatedMachines, idempotencyKey, managedOci, type CreateMachine } from "@acyclic-labs/machines";

const request = {
  idempotencyKey: idempotencyKey("11111111-1111-4111-8111-111111111111"),
  image: managedOci("registry.example/generated@sha256:1111111111111111111111111111111111111111111111111111111111111111"),
  compatibility: { kind: "best-effort" },
  suspension: { kind: "after-idle", milliseconds: 15000 },
  expiration: { kind: "never" },
  networkPolicyDigestHex: "2222222222222222222222222222222222222222222222222222222222222222",
  budgets: { spendMicros: BigInt("0"), concurrency: 0 },
} satisfies CreateMachine;

const provider = new SimulatedMachines();
const outcome = await provider.create(request);
if (outcome.kind !== "created") throw new Error(`expected created outcome, received ${outcome.kind}`);
const page = await provider.listMachines(null, 256);
const firstMachine = page.machines[0];
if (page.machines.length !== 1 || firstMachine === undefined || firstMachine.contract.suspension.kind !== "after-idle") throw new Error("Rust scenario parity failed");
console.log(JSON.stringify({ kind: outcome.kind, pageSize: page.machines.length, suspension: firstMachine.contract.suspension.kind }));
