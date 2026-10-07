import { Machines, managedOci, machineId, idempotencyKey, checkpointId, operationId } from "@acyclic-labs/machines";
const ids = [machineId("machine-1"), idempotencyKey("request-1"), checkpointId("checkpoint-1"), operationId("operation-1")];
const image = managedOci(`registry.example/image@sha256:${"0a".repeat(32)}`);
const facade = Machines.fromEnv({ endpoint: "https://example.test", token: "token" });
if (ids.some((value) => typeof value !== "string") || image.kind !== "managed-oci" || facade.provider === undefined) throw new Error("installed Machines package smoke failed");
console.log("installed-machines-runtime-ok");
