import type { ExpirationPolicy, Image, MachineId, MachinePage, MutationOutcome, SuspensionPolicy } from "@acyclic-labs/machines";
const plainMachine: MachineId = "machine-1";
const page: MachinePage = { machines: [], next: null };
page.machines = [];
const badImage: Image = { kind: "latest", digestHex: "07".repeat(64) };
const badSuspension: SuspensionPolicy = { kind: "invalid" };
const badExpiration: ExpirationPolicy = { kind: "invalid" };
const badOutcome: MutationOutcome = { kind: "made-up" };
void [plainMachine, page, badImage, badSuspension, badExpiration, badOutcome];
