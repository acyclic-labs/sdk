import { expect, test } from "bun:test";
import { createSecureServer } from "node:http2";
import { readFileSync } from "node:fs";
import { once } from "node:events";
import { Code, ConnectError } from "@connectrpc/connect";
import { connectNodeAdapter } from "@connectrpc/connect-node";
import { CredentialCache, CredentialSnapshot, resolveCredential } from "@acyclic-labs/auth/credential";
import { createMachinesGrpcClient } from "../src/grpc.js";
import * as wire from "../generated/proto/machines/v1/machines_pb.js";
import { ownFixtureServer } from "../../../../scripts/fixture-server.mjs";

test("Machines retains exact leases through original RPC and duplex body owners", async () => {
  let revision = "account-a/audience-a/grant-a";
  let now = 1;
  let refreshes = 0;
  let calls = 0;
  let mode = "ok";
  let release = Promise.withResolvers<void>();
  let entered = Promise.withResolvers<void>();
  let retired = Promise.withResolvers<void>();
  const cache = new CredentialCache(async () => {
    refreshes++;
    return new CredentialSnapshot(`synthetic-test-only-${refreshes}`, revision, now + 10);
  }, () => ({ revision, nowUnixSeconds: now }));
  const server = createSecureServer({
    cert: readFileSync(new URL("./fixtures/server.pem", import.meta.url)),
    key: readFileSync(new URL("./fixtures/server-key.pem", import.meta.url)),
  }, connectNodeAdapter({ routes(router) {
    router.service(wire.MachinesService, {
      async create(_request, context) {
        calls++;
        expect(context.requestHeader.get("authorization")).toMatch(/^Bearer synthetic-test-only-[1-9]\d*$/);
        const selected = mode;
        if (selected === "late401") {
          entered.resolve();
          await release.promise;
          throw new ConnectError("original attempt rejected", Code.Unauthenticated);
        }
        if (selected === "deny") throw new ConnectError("policy denied", Code.PermissionDenied);
        if (selected === "401") throw new ConnectError("original attempt rejected", Code.Unauthenticated);
        return {};
      },
      async *exec(input, context) {
        calls++;
        const aborted = Promise.withResolvers<void>();
        const stop = () => aborted.resolve();
        context.signal.addEventListener("abort", stop, { once: true });
        if (context.signal.aborted) stop();
        try {
          let machine;
          for await (const frame of input) if (frame.input.case === "start") machine = frame.input.value.machine;
          entered.resolve();
          yield { machine, output: { case: "data", value: { stream: wire.ExecStream.STDOUT, bytes: new Uint8Array([1]) } } };
          if (mode === "quiet") await aborted.promise;
          else {
            await release.promise;
            yield { machine, output: { case: "exitCode", value: 0 } };
          }
        } finally {
          context.signal.removeEventListener("abort", stop);
          retired.resolve();
        }
      },
    });
  } }));
  const closeServer = ownFixtureServer(server);
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("TCP fixture did not retain its listener");
  const client = createMachinesGrpcClient({
    endpoint: `https://127.0.0.1:${address.port}`, credential: cache.provider,
    caCertificate: readFileSync(new URL("./fixtures/server.pem", import.meta.url), "utf8"),
  });
  const outcome = <T>(value: Promise<T>) => value.then(value => ({ value, error: undefined }), error => ({ value: undefined, error }));
  const request = { protocol: { major: 1, minor: 3 } };
  const command = { ...request, machine: { value: new Uint8Array(16).fill(1) }, argv: ["synthetic-command"], timeoutMs: 60_000n };
  try {
    for (const count of [1, 100, 1000]) await Promise.all(Array.from({ length: count }, () => client.machines.create(request)));
    expect(calls).toBe(1101);
    expect(refreshes).toBe(1);
    for (const count of [1, 100, 1000]) {
      now += 10;
      await Promise.all(Array.from({ length: count }, () => client.machines.create(request)));
    }
    expect(calls).toBe(2202);
    expect(refreshes).toBe(4);

    mode = "late401";
    const late = outcome(client.machines.create(request));
    await Promise.race([entered.promise, late.then(() => { throw new Error("held RPC ended before its admission event"); })]);
    revision = "account-b/audience-b/grant-narrowed";
    // Admit a newer exact lease without delivering a source event to the held
    // old attempt, then let that old server rejection arrive.
    const current = await resolveCredential(cache.provider);
    release.resolve();
    expect((await late).error?.code).toBe(Code.Unauthenticated);
    current.check();
    mode = "deny";
    expect((await outcome(client.machines.create(request))).error?.code).toBe(Code.PermissionDenied);
    current.check();
    mode = "401";
    expect((await outcome(client.machines.create(request))).error?.code).toBe(Code.Unauthenticated);
    expect(() => current.check()).toThrow();

    for (const changed of ["source", "owner", "expiry", "cancel"]) {
      mode = changed === "expiry" ? "released" : "quiet";
      release = Promise.withResolvers<void>(); entered = Promise.withResolvers<void>(); retired = Promise.withResolvers<void>();
      const parent = new AbortController();
      const stream = client.machines.exec(command, { signal: parent.signal });
      expect((await stream.next()).value?.output.case).toBe("data");
      await entered.promise;
      const pending = outcome(stream.next());
      if (changed === "expiry") { now += 10; release.resolve(); }
      else if (changed === "cancel") parent.abort();
      else { revision = changed === "owner" ? "" : "account-c/audience-c/grant-c"; cache.invalidate(); }
      const refused = await pending;
      expect(refused.error).toBeDefined();
      await stream.return();
      await retired.promise;
      if (changed === "owner") revision = "account-c/audience-c/grant-c";
    }
    mode = "ok";
    await client.machines.create(request);
  } finally {
    release.resolve(); client.close();
    await closeServer();
  }
});
