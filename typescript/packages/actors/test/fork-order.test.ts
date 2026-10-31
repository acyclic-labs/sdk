import { test, expect } from "bun:test";
import { createServer } from "node:http2";
import { createClient, Code, ConnectError } from "@connectrpc/connect";
import { connectNodeAdapter, createGrpcTransport } from "@connectrpc/connect-node";
import { ownFixtureServer } from "../../../../scripts/fixture-server.mjs";
import { ActorsService } from "../generated/proto/actors/v1/actors_pb.js";
import { ActorNamespaceBinding } from "../src/namespace.js";

test("fork retains every earlier HTTP2 completion and refuses failure or cancellation", async () => {
  const gates = new Map<string, ReturnType<typeof Promise.withResolvers<void>>>();
  const entered = new Map<string, ReturnType<typeof Promise.withResolvers<void>>>();
  const forks: string[] = [];
  for (const name of ["success", "failed", "cancelled", "cancel-fork"]) {
    gates.set(name, Promise.withResolvers<void>());
    entered.set(name, Promise.withResolvers<void>());
  }
  const server = createServer(connectNodeAdapter({ routes(router) {
    router.service(ActorsService, {
      async invokeActor(request) {
        if (request.rpcMethod === "slow") {
          entered.get(request.actorId)!.resolve();
          await gates.get(request.actorId)!.promise;
        }
        return { status: 200, headers: [], body: request.body };
      },
      async forkActor(request) {
        forks.push(request.actorId);
        return { actor: { actorId: request.childActorId } };
      },
    });
  }}));
  const close = ownFixtureServer(server);
  try {
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (address === null || typeof address === "string") throw new Error("missing HTTP2 address");
    const transport = createClient(ActorsService, createGrpcTransport({ baseUrl: `http://127.0.0.1:${address.port}` }));
    const namespace = new ActorNamespaceBinding(new TextEncoder().encode("account"), "ClassOne", transport as never, "OBJECTS");
    const barrier = () => namespace.getByName("barrier").rpc("barrier", new Uint8Array());
    const child = namespace.idFromName("child");

    const success = namespace.getByName("success");
    const slow = success.rpc("slow", new Uint8Array());
    const fast = success.rpc("fast", new Uint8Array());
    await entered.get("success")!.promise;
    await fast; // The second real HTTP2 response overtakes the first.
    const forked = success.fork(child, [], { idempotencyKey: "success" });
    await barrier();
    expect(forks).toEqual([]);
    gates.get("success")!.resolve();
    await slow;
    expect((await forked).equals(child.reference)).toBe(true);
    expect(forks).toEqual(["success"]);

    for (const name of ["failed", "cancelled"]) {
      const stub = namespace.getByName(name);
      const cancellation = new AbortController();
      const first = stub.rpc("slow", new Uint8Array(), { signal: cancellation.signal });
      const firstResult = first.then(() => undefined, error => error);
      await entered.get(name)!.promise;
      await stub.rpc("fast", new Uint8Array());
      const pending = stub.fork(child, [], { idempotencyKey: name });
      const outcome = pending.then(() => undefined, error => error);
      await barrier();
      expect(forks).toEqual(["success"]);
      if (name === "failed") gates.get(name)!.reject(new ConnectError("original call failed", Code.Aborted));
      else cancellation.abort(new Error("original call cancelled"));
      const failure = await firstResult;
      expect(failure).toBeInstanceOf(Error);
      expect(await outcome).toBe(failure);
      gates.get(name)!.resolve();
      await barrier();
      expect(forks).toEqual(["success"]);
      await expect(stub.rpc("forbidden", new Uint8Array())).rejects.toThrow("broken");
    }

    // Cancelling only the fork wait must leave its preceding call alive.
    const stub = namespace.getByName("cancel-fork");
    const first = stub.rpc("slow", new Uint8Array());
    await entered.get("cancel-fork")!.promise;
    const cancellation = new AbortController();
    const pending = stub.fork(child, [], { signal: cancellation.signal });
    const outcome = pending.then(() => undefined, error => error);
    const reason = new Error("fork caller cancelled");
    cancellation.abort(reason);
    expect(await outcome).toBe(reason);
    expect(forks).toEqual(["success"]);
    gates.get("cancel-fork")!.resolve();
    await first;

    const invokeOnly = new ActorNamespaceBinding(new TextEncoder().encode("account"), "ClassOne", { invokeActor: transport.invokeActor } as never, "OBJECTS");
    await expect(invokeOnly.getByName("success").fork(child, [])).rejects.toThrow("cannot fork");
    const outside = new ActorNamespaceBinding(new TextEncoder().encode("account"), "ClassTwo", transport as never, "OBJECTS").idFromName("child");
    await expect(success.fork(outside, [])).rejects.toThrow("outside");
    await expect(success.fork(namespace.idFromName("success"), [])).rejects.toThrow("itself");
  } finally {
    for (const gate of gates.values()) gate.resolve();
    await close();
  }
});
