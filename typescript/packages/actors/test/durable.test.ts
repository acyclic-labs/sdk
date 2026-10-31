import { expect, test } from "bun:test";
import { ActorReference } from "@acyclic-labs/auth/addressing";
import { ActorNamespaceBinding } from "../src/namespace.js";
import { DurableObjectActivation, DurableObjectState, alarmInfo, type DurableHost } from "../src/durable.js";
const bytes = (s: string) => new TextEncoder().encode(s);
const reference = () => new ActorReference(bytes("account"), bytes("ClassOne"), bytes("same"));

test("namespace get is local, references are scoped, and stub calls stay ordered and terminal on failure", async () => {
  const calls: string[] = [];
  let fail = false;
  const namespace = new ActorNamespaceBinding(bytes("account"), "ClassOne", { async invokeActor(request) {
    expect(request.accountId).toEqual(bytes("account")); expect(request.namespace).toBe("ClassOne");
    expect(request.bindingName).toBe("OBJECTS"); calls.push(request.rpcMethod!);
    if (fail) throw new Error("rejected");
    return { status: 200, body: request.body, headers: [] };
  } }, "OBJECTS");
  const named = namespace.idFromName("same"), parsed = namespace.idFromString("same");
  expect(named.equals(parsed)).toBe(true); expect(named.name).toBe("same"); expect(parsed.name).toBeUndefined();
  expect(namespace.newUniqueId().toString()).toMatch(/^[a-f0-9]{32}$/);
  expect(() => namespace.idFromReference(new ActorReference(bytes("account"), bytes("ClassTwo"), bytes("same")))).toThrow();
  expect(() => namespace.idFromReference(new ActorReference(bytes("other"), bytes("ClassOne"), bytes("same")))).toThrow();
  const stub = namespace.get(named, { locationHint: "weur" }); expect(calls).toEqual([]);
  expect(() => namespace.get(named, { locationHint: "wrong" as "weur" })).toThrow();
  await Promise.all([stub.rpc("first", bytes("1")), stub.rpc("second", bytes("2"))]);
  expect(calls).toEqual(["first", "second"]);
  fail = true; await expect(stub.rpc("fail", new Uint8Array())).rejects.toThrow("rejected"); fail = false;
  await expect(stub.rpc("forbidden", new Uint8Array())).rejects.toThrow("broken");
  expect(calls).toEqual(["first", "second", "fail"]);
  await namespace.get(parsed).rpc("fresh", new Uint8Array()); // Negative control: fresh stub, same address.
  expect(calls.at(-1)).toBe("fresh");
});

test("fork waits for the stub's earlier calls, names the child in the parent's namespace and returns its plain reference", async () => {
  const events: string[] = [];
  let release!: () => void;
  const held = new Promise<void>(resolve => { release = resolve; });
  const transport = {
    async invokeActor(request: { rpcMethod?: string }) { events.push(`invoke ${request.rpcMethod}`); await held; events.push("invoked"); return { status: 200, body: new Uint8Array(), headers: [] }; },
    async forkActor(request: { actorId: string; childActorId: string; namespace?: string; idempotencyKey: string; subscriptions: readonly { subscriptionId: string }[] }) {
      events.push(`fork ${request.actorId}->${request.childActorId} ${request.namespace} ${request.idempotencyKey} ${request.subscriptions.map(s => s.subscriptionId)}`);
      return {};
    },
  };
  const namespace = new ActorNamespaceBinding(bytes("account"), "ClassOne", transport as never, "OBJECTS");
  const stub = namespace.getByName("same"), child = namespace.idFromName("child");
  const call = stub.rpc("set", new Uint8Array());
  const forked = stub.fork(child, [{ subscriptionId: "inbox", streamPath: "child/inbox", start: { currentHead: true }, placementAnchor: false } as never], { idempotencyKey: "k" });
  release(); await call;
  const reference = await forked;
  expect(events).toEqual(["invoke set", "invoked", "fork same->child ClassOne k inbox"]);
  expect(reference.equals(child.reference)).toBe(true);
  // Negative controls: no fork capability, a child outside the namespace, and forking into itself.
  const invokeOnly = new ActorNamespaceBinding(bytes("account"), "ClassOne", { invokeActor: transport.invokeActor } as never, "OBJECTS");
  await expect(invokeOnly.getByName("same").fork(child, [])).rejects.toThrow("cannot fork");
  const outside = new ActorNamespaceBinding(bytes("account"), "ClassTwo", transport as never, "OBJECTS").idFromName("child");
  await expect(stub.fork(outside, [])).rejects.toThrow("outside");
  await expect(stub.fork(namespace.idFromName("same"), [])).rejects.toThrow("itself");
  expect(events.length).toBe(3);
});

test("slow request bodies cannot reorder calls on one stub", async () => {
  const calls: string[] = [];
  const namespace = new ActorNamespaceBinding(bytes("account"), "ClassOne", { async invokeActor(request) {
    calls.push(request.rpcMethod ?? new TextDecoder().decode(Uint8Array.from(request.body)));
    return {status:200,body:request.body,headers:[]};
  } }, "OBJECTS");
  const id = namespace.idFromName("same"), stub = namespace.get(id);
  let controller!: ReadableStreamDefaultController<Uint8Array>;
  let reading!: () => void;
  const read = new Promise<void>(resolve => { reading = resolve; });
  const body = new ReadableStream<Uint8Array>({start(value) {controller = value;}, pull() {reading();}});
  const first = stub.fetch("https://example.com/", {method:"POST",body,duplex:"half"} as RequestInit);
  const second = stub.fetch("https://example.com/", {method:"POST",body:"second"});
  await read;
  // A separate stub is a microtask barrier and the negative control for global serialization.
  await namespace.get(id).rpc("barrier", new Uint8Array());
  expect(calls).toEqual(["barrier"]);
  controller.enqueue(bytes("first")); controller.close();
  await Promise.all([first,second]);
  expect(calls).toEqual(["barrier","first","second"]);
});

test("cancellation breaks only the affected stub without replay", async () => {
  let calls = 0, entered!: () => void;
  const started = new Promise<void>(resolve => { entered = resolve; });
  const namespace = new ActorNamespaceBinding(bytes("account"), "ClassOne", { async invokeActor(_request, options) {
    calls++; entered();
    return new Promise<never>((_resolve, reject) => {
      options!.signal!.addEventListener("abort", () => {reject(options!.signal!.reason);}, {once:true});
    });
  } }, "OBJECTS");
  const stub = namespace.get(namespace.idFromName("same"));
  const cancellation = new AbortController();
  const pending = stub.rpc("first",new Uint8Array(),{signal:cancellation.signal});
  await started; cancellation.abort(new Error("cancelled"));
  await expect(pending).rejects.toThrow("cancelled");
  await expect(stub.rpc("second",new Uint8Array())).rejects.toThrow("broken");
  expect(calls).toBe(1);
});

test("one stub admits interleaved responses and breaks every in-flight call on failure", async () => {
  let release!: () => void;
  const held = new Promise<void>(resolve => {release = resolve;});
  const orders: semanticOrder[] = [];
  type semanticOrder = {readonly session:string;readonly sequence:bigint};
  const namespace = new ActorNamespaceBinding(bytes("account"), "ClassOne", { async invokeActor(request) {
    orders.push(request.invocationOrder!);
    if (request.rpcMethod === "first") await held;
    return {status:200,body:request.body,headers:[]};
  } }, "OBJECTS");
  const stub = namespace.getByName("same"); expect(stub.name).toBe("same");
  const first = stub.rpc("first",new Uint8Array()), second = stub.rpc("second",new Uint8Array());
  try {
    await namespace.getByName("same").rpc("barrier",new Uint8Array());
    const ordered = orders.filter(order => order.session === orders[0]!.session);
    // Negative control fails immediately if admission waits for the first response.
    expect(ordered.map(order => order.sequence)).toEqual([0n,1n]);
    expect(ordered[0]!.session).toMatch(/^[a-f0-9]{32}$/);
    await second;
  } finally {release(); await first;}

  let fail!: (reason: Error) => void;
  const failed = new Promise<never>((_resolve, reject) => {fail = reject;});
  let releaseHeld!: (reason: Error) => void;
  const heldCall = new Promise<never>((_resolve, reject) => {releaseHeld = reject;});
  const failingNamespace = new ActorNamespaceBinding(bytes("account"), "ClassOne", { async invokeActor(request) {
    if (request.rpcMethod === "fail") return failed;
    if (request.rpcMethod === "hold") return heldCall;
    return {status:200,body:request.body,headers:[]};
  } }, "OBJECTS");
  const failing = failingNamespace.getByName("same");
  const a = failing.rpc("hold",new Uint8Array()), b = failing.rpc("fail",new Uint8Array());
  let rejectedInFlight = false;
  void a.catch(() => {rejectedInFlight = true;});
  fail(new Error("rejected")); await expect(b).rejects.toThrow("rejected");
  await failingNamespace.getByName("same").rpc("barrier",new Uint8Array());
  const propagated = rejectedInFlight;
  releaseHeld(new Error("fixture cleanup")); await a.catch(() => undefined);
  expect(propagated).toBe(true);
  await expect(failing.rpc("future",new Uint8Array())).rejects.toThrow("broken");
});

test("cancelling a blocked body reader cancels the stream and rejects queued calls", async () => {
  let read!: () => void, cancelled = 0, invocations = 0;
  const reading = new Promise<void>(resolve => {read = resolve;});
  const body = new ReadableStream<Uint8Array>({pull() {read();},cancel() {cancelled++;}});
  const namespace = new ActorNamespaceBinding(bytes("account"), "ClassOne", { async invokeActor() {
    invocations++; return {status:200,body:new Uint8Array(),headers:[]};
  } }, "OBJECTS");
  const stub = namespace.getByName("same"), abort = new AbortController();
  const first = stub.fetch("https://example.com/",{method:"POST",body,duplex:"half"} as RequestInit,{signal:abort.signal});
  const second = stub.rpc("queued",new Uint8Array());
  const settled = Promise.allSettled([first,second]);
  await reading; abort.abort(new Error("cancelled"));
  expect(cancelled).toBe(1);
  expect((await settled).map(result => result.status)).toEqual(["rejected","rejected"]);
  expect(invocations).toBe(0);
});

test("durable host receives the exact triple; validation rejects bounds before any host effect", async () => {
  const seen: string[] = [];
  const host = new Proxy({}, {get(_target, property) { return (address: Uint8Array) => {
    expect(ActorReference.decode(address).equals(reference())).toBe(true); seen.push(String(property));
    return property === "getWebSockets" ? [] : undefined;
  }; }}) as DurableHost;
  const state = new DurableObjectState(reference(), host);
  const socket = {} as WebSocket;
  state.acceptWebSocket(socket, Array(10).fill("λ".repeat(256)));
  expect(() => state.acceptWebSocket(socket, Array(11).fill(""))).toThrow();
  expect(() => state.acceptWebSocket(socket, ["λ".repeat(257)])).toThrow();
  state.serializeAttachment(socket, new Uint8Array(2048));
  expect(() => state.serializeAttachment(socket, new Uint8Array(2049))).toThrow();
  state.setWebSocketAutoResponse({request:"λ".repeat(2048),response:"ok"});
  expect(() => state.setWebSocketAutoResponse({request:"",response:"λ".repeat(2049)})).toThrow();
  expect(() => alarmInfo(0,true)).toThrow(); expect(alarmInfo(1)).toEqual({retryCount:1,isRetry:true});
  expect(seen).toEqual(["acceptWebSocket","serializeAttachment","setWebSocketAutoResponse"]);
  const delivered: string[] = [];
  const actor = new DurableObjectActivation(state, {
    initialize() { delivered.push("initialize"); return 1; },
    fetch() { delivered.push("fetch"); return new Response("ok"); },
    rpc() { delivered.push("rpc"); return new Uint8Array(); },
    alarm() { delivered.push("alarm"); },
    webSocketMessage() { delivered.push("message"); },
    webSocketClose() { delivered.push("close"); },
    webSocketError() { delivered.push("error"); },
  });
  await Promise.all([actor.fetch(new Request("https://example.com/")),actor.rpc("method",new Uint8Array()),actor.alarm(alarmInfo(0)),actor.webSocketMessage(socket,"data"),actor.webSocketClose(socket,1000,"",true),actor.webSocketError(socket,new Error())]);
  expect(delivered).toEqual(["initialize","fetch","rpc","alarm","message","close","error"]);
  await state.deleteAll(); expect(seen.at(-1)).toBe("deleteAll");
});
