// Generated from Rust scenario machines/typescript-consumer.
// Rust output SHA256: sha256:171f70b33b314988c6baf45e574f5abc2a6a008e7ba033a04245d16b3f80c69c
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
if (page.machines.length !== 1 || page.machines[0].contract.suspension.kind !== "after-idle") throw new Error("Rust scenario parity failed");
console.log(JSON.stringify({ kind: outcome.kind, pageSize: page.machines.length, suspension: page.machines[0].contract.suspension.kind }));
