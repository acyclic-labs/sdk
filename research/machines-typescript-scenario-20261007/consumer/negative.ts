import type { ExpirationPolicy, Image, MachineId, MachinePage, MutationOutcome, SuspensionPolicy } from "@acyclic-labs/machines";
// @ts-expect-error branded IDs reject plain strings
const plainMachine: MachineId = "machine-1";
const page: MachinePage = { machines: [], next: null };
// @ts-expect-error generated DTO projections are readonly
page.machines = [];
// @ts-expect-error generated image union rejects unknown discriminants
const badImage: Image = { kind: "latest", digestHex: "07".repeat(64) };
// @ts-expect-error policies derive their finite Rust unions
const badSuspension: SuspensionPolicy = { kind: "invalid" };
// @ts-expect-error policies derive their finite Rust unions
const badExpiration: ExpirationPolicy = { kind: "invalid" };
// @ts-expect-error mutation outcomes preserve Rust union discriminants
const badOutcome: MutationOutcome = { kind: "made-up" };
void [plainMachine, page, badImage, badSuspension, badExpiration, badOutcome];
