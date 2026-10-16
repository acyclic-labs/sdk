import { describe, expect, test } from "bun:test";
import { create } from "@bufbuild/protobuf";
import { Code, ConnectError } from "@connectrpc/connect";
import { connectNodeAdapter } from "@connectrpc/connect-node";
import { once } from "node:events";
import { readFileSync } from "node:fs";
import { createSecureServer } from "node:http2";
import {
  createMachine, HttpMachinesProvider, idempotencyKey, MachinesTransportError,
  SimulatedMachines, type CreateMachine,
} from "../src/index.ts";
import { createMachinesGrpcClient } from "../src/grpc.ts";
import * as wire from "../generated/proto/machines/v1/machines_pb.js";
import { ownFixtureServer } from "../../../../scripts/fixture-server.mjs";

type Outcome = "accepted" | "rejected" | "uncertain";
const protocol = { major: 1, minor: 1 };
const caCertificate = readFileSync(new URL("./fixtures/server.pem", import.meta.url), "utf8");
const key = readFileSync(new URL("./fixtures/server-key.pem", import.meta.url), "utf8");

function bytes(identity: string): Uint8Array {
  return Uint8Array.from(Buffer.from(identity.replaceAll("-", ""), "hex"));
}

function request(): CreateMachine {
  return {
    idempotencyKey: idempotencyKey("00000000-0000-4000-8000-000000000012"),
    image: { kind: "custom", digestHex: "07".repeat(32) },
    compatibility: { kind: "best-effort" },
    suspension: { kind: "manual" },
    expiration: { kind: "never" },
    networkPolicyDigestHex: "08".repeat(32),
    budgets: { spendMicros: 0n, concurrency: 0 },
  };
}

function wireRequest(input: CreateMachine) {
  return create(wire.CreateMachineRequestSchema, {
    protocol,
    idempotencyKey: { value: bytes(input.idempotencyKey) },
    image: { kind: wire.ImageKind.CUSTOM, immutableReference: { case: "customDigest", value: new Uint8Array(32).fill(7) } },
    compatibility: { mode: wire.CompatibilityMode.BEST_EFFORT },
    suspension: { policy: { case: "manual", value: true } },
    expiration: { kind: wire.ExpirationKind.NEVER },
    networkPolicyDigest: new Uint8Array(32).fill(8),
    budgets: { spendMicros: 0n, concurrency: 0 },
  });
}

function encode(value: unknown): string {
  return JSON.stringify(value, (_key, item: unknown) => {
    if (typeof item === "bigint") return { $bigint: item.toString() };
    if (item instanceof Uint8Array) return { $bytes: Buffer.from(item).toString("base64") };
    return item;
  });
}

async function attempt<T>(invoke: () => Promise<T>) {
  try { return { accepted: true, value: await invoke() } as const; }
  catch (error) { return { accepted: false, error } as const; }
}

async function fixture(outcome: Outcome) {
  const expected = request();
  const simulated = new SimulatedMachines();
  const calls = { http: [] as string[], grpc: [] as string[] };
  const admission = async () => {
    const created = await simulated.recover(expected.idempotencyKey);
    if (created.kind !== "created") throw new Error("fixture did not create a machine");
    return {
      machine: { value: bytes(created.machine.id) },
      operation: { value: bytes(await simulated.recoverOperation(expected.idempotencyKey)) },
    };
  };
  const server = createSecureServer({ cert: caCertificate, key }, connectNodeAdapter({
    routes(router) {
      router.service(wire.MachinesService, {
        async create(query, context) {
          expect(context.requestHeader.get("authorization")).toBe("Bearer conformance");
          expect(query).toEqual(wireRequest(expected));
          calls.grpc.push("create");
          if (outcome === "rejected") throw new ConnectError("not admitted", Code.InvalidArgument);
          await simulated.create(expected);
          const accepted = await admission();
          if (outcome === "uncertain") throw new ConnectError("admission reply lost", Code.Unavailable);
          return accepted;
        },
        async recover(query, context) {
          expect(context.requestHeader.get("authorization")).toBe("Bearer conformance");
          expect(query.idempotencyKey?.value).toEqual(bytes(expected.idempotencyKey));
          calls.grpc.push("recover");
          if (outcome === "rejected") throw new ConnectError("no admission", Code.NotFound);
          const recovered = await admission();
          return { operation: recovered.operation, result: { case: "create", value: recovered } };
        },
        async *exec(query, context) {
          expect(context.requestHeader.get("authorization")).toBe("Bearer conformance");
          expect(query.argv).toEqual(["echo", "hi"]);
          const original = await admission();
          expect(query.machine?.value).toEqual(original.machine.value);
          calls.grpc.push("exec");
          yield { machine: original.machine, output: { case: "data", value: new TextEncoder().encode("partial") } };
          throw new ConnectError("exec reply lost after dispatch", Code.Unavailable);
        },
      });
    },
  }));
  const closeServer = ownFixtureServer(server);
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("missing fixture port");
  const grpc = createMachinesGrpcClient({ endpoint: `https://127.0.0.1:${address.port}`, token: "conformance", caCertificate });
  const http = new HttpMachinesProvider({
    endpoint: "https://conformance.invalid/", token: "conformance",
    fetcher: Object.assign(async (input: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) => {
      const headers = new Headers(init?.headers);
      expect(headers.get("authorization")).toBe("Bearer conformance");
      const path = new URL(String(input)).pathname;
      const query: unknown = JSON.parse(String(init?.body));
      expect(query).toMatchObject({ idempotencyKey: expected.idempotencyKey });
      if (path.endsWith("/machines/create")) {
        calls.http.push("create");
        if (outcome === "rejected") return new Response("not admitted", { status: 400 });
        const created = await simulated.create(expected);
        if (outcome === "uncertain") return new Response("admission reply lost", { status: 503 });
        return new Response(encode(created));
      }
      if (path.endsWith("/operations/recover")) {
        calls.http.push("recover");
        if (outcome === "rejected") return new Response("no admission", { status: 404 });
        return new Response(encode(await simulated.recover(expected.idempotencyKey)));
      }
      throw new Error(`unexpected HTTP fixture request ${path}`);
    }, { preconnect() { throw new Error("unexpected provider network preconnect"); } }),
  });
  return {
    http, grpc, calls, expected, simulated,
    async close() { grpc.close(); await closeServer(); },
  };
}

describe("Machines HTTP/gRPC admission and recovery conformance", () => {
  for (const outcome of ["accepted", "rejected", "uncertain"] as const) {
    test(`${outcome} admission is explicit and never silently redispatched`, async () => {
      const f = await fixture(outcome);
      try {
        const http = await attempt(() => createMachine(f.http, f.expected));
        const grpc = await attempt(() => f.grpc.machines.create(wireRequest(f.expected)));
        expect(http.accepted).toBe(outcome === "accepted");
        expect(grpc.accepted).toBe(outcome === "accepted");
        expect(f.calls).toEqual({ http: ["create"], grpc: ["create"] });
        const page = await f.simulated.listMachines(null, 10);
        expect(page.machines).toHaveLength(outcome === "rejected" ? 0 : 1);
        if (http.accepted && grpc.accepted) {
          expect(grpc.value.machine?.value).toEqual(bytes(http.value));
          expect(grpc.value.operation?.value).toEqual(bytes(await f.simulated.recoverOperation(f.expected.idempotencyKey)));
        } else if (!http.accepted && !grpc.accepted) {
          expect(http.error).toBeInstanceOf(MachinesTransportError);
          expect(http.error).toMatchObject({ status: outcome === "rejected" ? 400 : 503 });
          expect(grpc.error).toBeInstanceOf(ConnectError);
          expect(grpc.error).toMatchObject({ code: outcome === "rejected" ? Code.InvalidArgument : Code.Unavailable });
        } else throw new Error("HTTP and gRPC disagreed on admission");

        const recoveredHttp = await attempt(() => f.http.recover(f.expected.idempotencyKey));
        const recoveredGrpc = await attempt(() => f.grpc.machines.recover({ protocol, idempotencyKey: { value: bytes(f.expected.idempotencyKey) } }));
        if (outcome === "rejected") {
          expect(recoveredHttp.accepted).toBe(false);
          expect(recoveredGrpc.accepted).toBe(false);
          if (!recoveredHttp.accepted) expect(recoveredHttp.error).toMatchObject({ status: 404 });
          if (!recoveredGrpc.accepted) expect(recoveredGrpc.error).toMatchObject({ code: Code.NotFound });
        } else {
          if (!recoveredHttp.accepted || !recoveredGrpc.accepted) throw new Error("original admission was not recoverable");
          if (recoveredHttp.value.kind !== "created" || recoveredGrpc.value.result.case !== "create") throw new Error("recovery substituted another mutation");
          expect(recoveredGrpc.value.result.value.machine?.value).toEqual(bytes(recoveredHttp.value.machine.id));
          const operation = bytes(await f.simulated.recoverOperation(f.expected.idempotencyKey));
          expect(recoveredGrpc.value.operation?.value).toEqual(operation);
          expect(recoveredGrpc.value.result.value.operation?.value).toEqual(operation);
        }
        expect(f.calls).toEqual({ http: ["create", "recover"], grpc: ["create", "recover"] });
        expect((await f.simulated.listMachines(null, 10)).machines).toHaveLength(outcome === "rejected" ? 0 : 1);
      } finally { await f.close(); }
    });
  }

  test("non-idempotent exec is not redispatched after a partial response", async () => {
    const f = await fixture("accepted");
    try {
      const admission = await f.grpc.machines.create(wireRequest(f.expected));
      const data: string[] = [];
      const result = await attempt(async () => {
        for await (const frame of f.grpc.machines.exec({
          protocol, machine: admission.machine, argv: ["echo", "hi"], timeoutMs: 1_000,
        })) {
          if (frame.output.case !== "data") throw new Error("an uncertain exec reported a terminal exit");
          data.push(new TextDecoder().decode(frame.output.value));
        }
      });
      expect(result.accepted).toBe(false);
      if (!result.accepted) expect(result.error).toMatchObject({ code: Code.Unavailable });
      expect(data).toEqual(["partial"]);
      expect(f.calls).toEqual({ http: [], grpc: ["create", "exec"] });
      expect((await f.simulated.listMachines(null, 10)).machines).toHaveLength(1);
    } finally { await f.close(); }
  });
});
