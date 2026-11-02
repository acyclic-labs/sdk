import { expect, test } from "bun:test";
import { ActorReference } from "@acyclic-labs/auth/addressing";
import { ActorNamespaceBinding } from "../src/namespace.js";
import { DurableObjectActivation, DurableObjectState, type DurableHost } from "../src/durable.js";
const bytes = (s: string) => new TextEncoder().encode(s);
const reference = () => new ActorReference(bytes("account"), bytes("ClassOne"), bytes("same"));

test("namespace get is local, references are scoped, and stub calls stay ordered and terminal on failure", async () => {
  const calls: string[] = [];
  let fail = false;
  const account = bytes("account");
  const namespace = new ActorNamespaceBinding(account, "ClassOne", { async invokeActor(request) {
    expect(request.accountId).toEqual(bytes("account")); expect(request.namespace).toBe("ClassOne");
    expect(request.actorId).toBe("same");
    if (request.rpcMethod === "first" || request.rpcMethod === "second") expect(request.locationHint).toBe("weur");
    // A transport may mutate its owned request bytes, never the retained scope.
    if (request.accountId instanceof Uint8Array) request.accountId.fill(120);
    expect(request.bindingName).toBe("OBJECTS"); calls.push(request.rpcMethod!);
    if (fail) throw new Error("rejected");
    return { status: 200, body: request.body, headers: [] };
  } }, "OBJECTS");
  account.fill(120);
  const named = namespace.idFromName("same"), parsed = namespace.idFromString("same");
  expect(named.equals(parsed)).toBe(true); expect(named.name).toBe("same"); expect(parsed.name).toBeUndefined();
  expect(namespace.newUniqueId().toString()).toMatch(/^[a-f0-9]{32}$/);
  expect(() => namespace.idFromReference(new ActorReference(bytes("account"), bytes("ClassTwo"), bytes("same")))).toThrow();
  expect(() => namespace.idFromReference(new ActorReference(bytes("other"), bytes("ClassOne"), bytes("same")))).toThrow();
  const substituted = reference(), foreign = new ActorReference(bytes("other"), bytes("ClassOne"), bytes("same"));
  substituted.encode = () => foreign.encode();
  expect(() => namespace.idFromReference(substituted)).toThrow();
  const selected = named.reference;
  let referenceReads = 0;
  Object.defineProperty(named, "reference", {get() { return ++referenceReads === 1 ? selected : foreign; }});
  const fields = ["account", "namespace"] as const;
  const descriptors = fields.map(field => Object.getOwnPropertyDescriptor(ActorReference.prototype,field)!);
  const projections = {account:0,namespace:0};
  let hintReads = 0, stub!: ReturnType<ActorNamespaceBinding["get"]>;
  try {
    for (const [index,field] of fields.entries()) {
      const descriptor = descriptors[index]!;
      Object.defineProperty(ActorReference.prototype,field,{...descriptor,get() {
        projections[field]++;
        return descriptor.get!.call(this);
      }});
    }
    stub = namespace.get(named, {get locationHint() {
      return hintReads++ === 0 ? "weur" : "wrong" as "weur";
    }});
    expect(hintReads).toBe(1);
    expect(referenceReads).toBe(0);
    // Only the stub projects its canonical fields; the namespace retains its scope.
    expect(projections).toEqual({account:1,namespace:1});
  } finally {
    for (const [index,field] of fields.entries()) Object.defineProperty(ActorReference.prototype,field,descriptors[index]!);
  }
  expect(calls).toEqual([]);
  expect(() => namespace.get(named, { locationHint: "wrong" as "weur" })).toThrow();
  const encode = ActorReference.prototype.encode;
  let encodes = 0;
  ActorReference.prototype.encode = function () { encodes++; return encode.call(this); };
  try {
    await Promise.all([stub.rpc("first", bytes("1")), stub.rpc("second", bytes("2"))]);
    // Negative control: rebuilding the same immutable reference per call crosses this boundary.
    expect(encodes).toBe(0);
  } finally { ActorReference.prototype.encode = encode; }
  expect(calls).toEqual(["first", "second"]);
  fail = true; await expect(stub.rpc("fail", new Uint8Array())).rejects.toThrow("rejected"); fail = false;
  await expect(stub.rpc("forbidden", new Uint8Array())).rejects.toThrow("broken");
  expect(calls).toEqual(["first", "second", "fail"]);
  await namespace.get(parsed).rpc("fresh", new Uint8Array()); // Negative control: fresh stub, same address.
  expect(calls.at(-1)).toBe("fresh");
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

test("durable host receives the exact triple and complete socket metadata", async () => {
  const seen: string[] = [];
  const host = new Proxy({}, {get(_target, property) { return (address: Uint8Array) => {
    expect(ActorReference.decode(address).equals(reference())).toBe(true); seen.push(String(property));
    return property === "getWebSockets" ? [] : undefined;
  }; }}) as DurableHost;
  const state = new DurableObjectState(reference(), host);
  const socket = {} as WebSocket;
  state.acceptWebSocket(socket, Array(11).fill("λ".repeat(257)));
  state.serializeAttachment(socket, new Uint8Array(2049));
  state.setWebSocketAutoResponse({request:"λ".repeat(2049),response:"ok"});
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
  await Promise.all([actor.fetch(new Request("https://example.com/")),actor.rpc("method",new Uint8Array()),actor.alarm(),actor.webSocketMessage(socket,"data"),actor.webSocketClose(socket,1000,"",true),actor.webSocketError(socket,new Error())]);
  expect(delivered).toEqual(["initialize","fetch","rpc","alarm","message","close","error"]);
  await state.deleteAll(); expect(seen.at(-1)).toBe("deleteAll");
});
