import { expect, test } from "bun:test";
import { ActorReference } from "@acyclic-labs/auth/addressing";
import { ActorNamespaceBinding } from "../src/namespace.js";
import { ActorActivation, DurableObjectActivation, DurableObjectState, alarmInfo, type DurableHost } from "../src/durable.js";
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

test("initialization and exclusive sections gate every handler; failed or fenced heaps never revive", async () => {
  let release!: () => void; const held = new Promise<void>(resolve => { release = resolve; });
  let initializations = 0, deliveries = 0;
  const activation = new ActorActivation(async () => { initializations++; await held; return 42; });
  const a = activation.deliver(state => { deliveries++; return state; });
  const b = activation.deliver(state => { deliveries++; return state; });
  expect(deliveries).toBe(0); release(); expect(await Promise.all([a,b])).toEqual([42,42]);
  expect(initializations).toBe(1);
  await expect(activation.blockConcurrencyWhile(() => { throw new Error("initialization failed"); })).rejects.toThrow();
  await expect(activation.deliver(() => { deliveries++; })).rejects.toThrow(); expect(deliveries).toBe(2);
  let finish!: () => void; const gate = new Promise<void>(resolve => { finish = resolve; });
  const fenced = new ActorActivation(async () => { await gate; return 1; });
  const pending = fenced.deliver(() => { deliveries++; }); fenced.fence(); finish();
  await expect(pending).rejects.toThrow(); await expect(fenced.deliver(() => { deliveries++; })).rejects.toThrow();
  expect(deliveries).toBe(2);
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
