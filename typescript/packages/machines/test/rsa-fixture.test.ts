import { readFile } from "node:fs/promises";
import { create } from "@bufbuild/protobuf";
import { expect, test } from "bun:test";
import { createMachinesGrpcClient } from "../src/generated-client.js";
import { createMachinesGrpcInvoker } from "../src/grpc.js";
import {
  CheckpointMachineRequestSchema,
  CheckpointMutationRequestSchema,
  CreateMachineRequestSchema,
  EventsRequestSchema,
  ForkCheckpointRequestSchema,
  ForkMachineRequestSchema,
  InspectCheckpointRequestSchema,
  InspectMachineRequestSchema,
  ListMachinesRequestSchema,
  MachineMutationRequestSchema,
  OperationRequestSchema,
  QualifyImageRequestSchema,
  RecoverRequestSchema,
  SetSuspensionPolicyRequestSchema,
  UsageRequestSchema,
} from "../generated/proto/machines/v1/machines_pb.js";

interface RsaFixtureConnection {
  readonly endpoint: string;
  readonly caCertificate: string;
  readonly certificate: string;
  readonly privateKey: string;
}

test("executes every modeled Machines RPC against the RSA mTLS fixture when configured", async () => {
  const path = process.env.ACYCLIC_MACHINES_RSA_FIXTURE_FILE;
  if (path === undefined) return;
  const connection = JSON.parse(await readFile(path, "utf8")) as RsaFixtureConnection;
  const client = createMachinesGrpcClient(createMachinesGrpcInvoker(connection));
  const calls = [
    client.qualifyImage(create(QualifyImageRequestSchema)),
    client.create(create(CreateMachineRequestSchema)),
    client.checkpoint(create(CheckpointMachineRequestSchema)),
    client.fork(create(ForkCheckpointRequestSchema)),
    client.forkMachine(create(ForkMachineRequestSchema)),
    client.suspend(create(MachineMutationRequestSchema)),
    client.wake(create(MachineMutationRequestSchema)),
    client.setSuspensionPolicy(create(SetSuspensionPolicyRequestSchema)),
    client.destroyMachine(create(MachineMutationRequestSchema)),
    client.destroyCheckpoint(create(CheckpointMutationRequestSchema)),
    client.recover(create(RecoverRequestSchema)),
    client.inspectMachine(create(InspectMachineRequestSchema)),
    client.inspectCheckpoint(create(InspectCheckpointRequestSchema)),
    client.listMachines(create(ListMachinesRequestSchema)),
    client.events(create(EventsRequestSchema)),
    client.usage(create(UsageRequestSchema)),
    client.cancel(create(OperationRequestSchema)),
    client.inspectOperation(create(OperationRequestSchema)),
  ];
  await Promise.all(calls);
  const watched = [];
  for await (const state of client.watchOperation(create(OperationRequestSchema))) watched.push(state.status);
  expect(calls).toHaveLength(18);
  expect(watched).toHaveLength(2);
});
