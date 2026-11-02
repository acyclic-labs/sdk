import { expect, test } from "bun:test";
import { materializeBindings } from "../src/bindings.js";
import { ActorNamespaceBinding, type semantic } from "@acyclic-labs/actors";
import { WorkerServiceBinding } from "../src/service.js";
import { ResourceIdentity } from "@acyclic-labs/auth/addressing";
const account = new TextEncoder().encode("account");
const namespace: semantic.Binding = {name:"OBJECTS",capability:"grant",resource:"actors:/ClassOne",targetAccount:account,actorNamespace:"ClassOne"};
const service: semantic.Binding = {name:"SERVICE",capability:"grant",resource:"workers:/worker",targetAccount:account,workerService:"worker"};

test("deployment declarations become scoped namespace and private service handles after Rust admission", async () => {
  let factories = 0;
  const requests: unknown[] = [];
  const record = (request: semantic.InvokeActorRequest | semantic.InvokeWorkerRequest) => {
    expect(request.accountId).toEqual(account);
    if (!(request.deploymentEnvelope instanceof Uint8Array)) throw new TypeError("binding envelope must own transferable bytes");
    // Transfer this request's envelope exactly as an IPC adapter can. A shared
    // empty backing buffer would leave the next request detached and unclonable.
    const received = structuredClone(request, {transfer:[request.deploymentEnvelope.buffer]});
    expect(received.deploymentEnvelope).toEqual(new Uint8Array());
    requests.push(received);
    // A transport owns these request bytes, never the handle's retained scope.
    if (request.accountId instanceof Uint8Array) request.accountId.fill(120);
    return {status:200,headers:[],body:received.body};
  };
  let actorReads = 0, workerReads = 0;
  const actorTransport = {get invokeActor() {
    actorReads++;
    return async function(this: unknown, request: semantic.InvokeActorRequest) {
      expect(this).toBe(actorTransport);
      return record(request);
    };
  }};
  const workerTransport = {get invokeWorker() {
    workerReads++;
    return async function(this: unknown, request: semantic.InvokeWorkerRequest) {
      expect(this).toBe(workerTransport);
      return record(request);
    };
  }};
  const authority = {
    namespace() {
      expect(this).toBe(authority);
      // Earlier effects cannot substitute a later producer after preflight.
      authority.service = () => { throw new Error("substituted service producer"); };
      factories++;
      return actorTransport;
    },
    service() { expect(this).toBe(authority); factories++; return workerTransport; },
    get objects(): never { throw new Error("unselected Objects producer"); },
  };
  const env = await materializeBindings([namespace,service], authority);
  expect(Object.isFrozen(env)).toBe(true); expect(factories).toBe(2);
  expect(env.OBJECTS).toBeInstanceOf(ActorNamespaceBinding); expect(env.SERVICE).toBeInstanceOf(WorkerServiceBinding);
  const objects = env.OBJECTS as ActorNamespaceBinding;
  const worker = env.SERVICE as WorkerServiceBinding;
  const exposed = worker.identity.toFields();
  exposed.account.fill(121); exposed.id.fill(122);
  const actor = objects.get(objects.idFromName("same"));
  actor.id.toString = () => { throw new Error("substituted actor display method"); };
  Object.defineProperty(actor.id,"reference", {get() { throw new Error("substituted actor reference"); }});
  expect(actor.reference.toFields()).toEqual({account,namespace:new TextEncoder().encode("ClassOne"),id:new TextEncoder().encode("same")});
  expect([actorReads,workerReads]).toEqual([1,1]);
  // Existing handles retain the original callable and receiver across a wait.
  for (const [transport, method] of [[actorTransport,"invokeActor"],[workerTransport,"invokeWorker"]] as const) {
    Object.defineProperty(transport, method, {get() { throw new Error("substituted invocation producer"); }});
  }
  const accountField = Object.getOwnPropertyDescriptor(ResourceIdentity.prototype,"account")!;
  let projections = 0;
  Object.defineProperty(ResourceIdentity.prototype,"account", {...accountField, get() {
    projections++;
    return accountField.get!.call(this);
  }});
  try {
    expect(await actor.rpc("actorCall", new Uint8Array([1]))).toEqual(new Uint8Array([1]));
    expect(await worker.rpc("workerCall", new Uint8Array([2]))).toEqual(new Uint8Array([2]));
    expect(await actor.rpc("actorWarm", new Uint8Array([3]))).toEqual(new Uint8Array([3]));
    expect(await worker.rpc("workerWarm", new Uint8Array([4]))).toEqual(new Uint8Array([4]));
    // Negative control: the former getter repeats the real Rust identity decode.
    expect(projections).toBe(0);
  } finally { Object.defineProperty(ResourceIdentity.prototype,"account", accountField); }
  expect([actorReads,workerReads]).toEqual([1,1]);
  expect(requests).toHaveLength(4);
  expect(requests.slice(0,2)).toMatchObject([{accountId:account,namespace:"ClassOne",actorId:"same",bindingName:"OBJECTS"},{accountId:account,workerName:"worker",bindingName:"SERVICE"}]);
  for (const handle of [actor,worker]) {
    const expected = new Uint8Array([29,31,37]);
    const backing = new Uint8Array(1024 * 1024);
    backing.set(expected,17);
    const payload = backing.subarray(17,20);
    // Negative control: the former slice call substitutes the admitted bytes.
    Object.defineProperty(payload,"slice",{value:() => new Uint8Array([255])});
    const pending = handle.rpc("selected",payload);
    backing.fill(0,17,20);
    expect(await pending).toEqual(expected);
    const admitted = requests.length;
    await expect(handle.rpc("refused",new Uint8Array(new SharedArrayBuffer(3)))).rejects.toBeInstanceOf(TypeError);
    expect(requests).toHaveLength(admitted);
    // A boundary refusal has no invocation and leaves the original handle live.
    expect(await handle.rpc("afterRefusal",expected)).toEqual(expected);
    const body = new ReadableStream<Uint8Array>({start(controller) {controller.enqueue(expected);controller.close();}});
    const response = await handle.fetch("https://example.com/",{method:"POST",body,duplex:"half"} as RequestInit);
    expect(new Uint8Array(await response.arrayBuffer())).toEqual(expected);
  }
  factories = 0;
  for (const invalid of [
    {...namespace,resource:"actors:/Other"}, {...namespace,targetAccount:new Uint8Array()},
    {...namespace,workerService:"worker"}, {...namespace,capability:""},
    {...service,workerService:"Upper"}, {...namespace,actorNamespace:undefined},
    {...namespace,targetAccount:[256]}, {...namespace,targetAccount:[0.5]},
  ]) {
    await expect(materializeBindings([namespace, {...invalid,name:"BAD"}],authority)).rejects.toThrow();
  }
  await expect(materializeBindings([namespace,namespace],authority)).rejects.toThrow();
  // Negative control: without complete admission, even an earlier valid entry could cause host effects.
  expect(factories).toBe(0);
});


test("binding admission snapshots caller bytes and scope before awaiting Rust", async () => {
  const target = account.slice();
  const declaration = {...namespace,targetAccount:target};
  const secondTarget = account.slice();
  const second = {...namespace,name:"SECOND",targetAccount:secondTarget};
  const admitted: number[][] = [];
  const pending = materializeBindings([declaration,second], {
    namespace(binding) {
      admitted.push(Array.from(binding.targetAccount));
      return {async invokeActor(request) {
        expect(request.accountId).toEqual(account);
        expect(request.namespace).toBe("ClassOne");
        return {status:200,headers:[],body:request.body};
      }};
    },
    service() { throw new Error("wrong binding kind"); },
  });
  // Negative control: retaining caller backing bytes changes the admitted scope.
  target.fill(120); declaration.actorNamespace = "ClassTwo";
  secondTarget.fill(121); second.actorNamespace = "ClassThree";
  const env = await pending;
  expect(admitted).toEqual([Array.from(account),Array.from(account)]);
  for (const name of ["OBJECTS","SECOND"]) {
    const binding = env[name] as ActorNamespaceBinding;
    await binding.getByName("same").rpc("call",new Uint8Array());
  }
});

// Exercise both public wrappers through the same Fetch contract controls.
for (const kind of ["actor","worker"] as const) {
  const handle = (transport: {invoke(request: semantic.InvokeActorRequest | semantic.InvokeWorkerRequest, options?: import("@acyclic-labs/actors").ActorsCallOptions): Promise<semantic.InvokeActorResponse>}) => kind === "actor"
    ? new ActorNamespaceBinding(account,"ClassOne",{invokeActor:transport.invoke.bind(transport)},"OBJECTS").getByName("same")
    : new WorkerServiceBinding(account,"worker",{invokeWorker:transport.invoke.bind(transport)},"SERVICE");
  test(`${kind} fetch forwards Request, init and SDK cancellation to transport`, async () => {
    for (const source of ["request","init","sdk","both"] as const) {
      let entered!: () => void; const started = new Promise<void>(resolve => {entered = resolve;});
      let received: AbortSignal | undefined, finish!: (reason: Error) => void;
      const target = handle({async invoke(_request,options) {
        received = options?.signal; entered();
        return new Promise<never>((_resolve,reject) => {finish = reject;received?.addEventListener("abort",() => reject(received?.reason),{once:true});});
      }});
      const abort = new AbortController(), sdk = new AbortController();
      const request = new Request("https://example.com/",source === "request" ? {signal:abort.signal} : {});
      const pending = target.fetch(request,source === "init" || source === "both" ? {signal:abort.signal} : {},source === "sdk" || source === "both" ? {signal:source === "sdk" ? abort.signal : sdk.signal} : {});
      const settled = pending.catch(error => error);
      await started; abort.abort(new Error("cancelled"));
      try { expect(received?.aborted).toBe(true); } finally { sdk.abort(new Error("cleanup")); finish(new Error("cleanup")); }
      expect((await settled).message).toBe("cancelled");
    }
  });
}


test("pre-aborted and queued actor Fetch bodies are released before transport", async () => {
  for (const preAborted of [false,true]) {
    let cancelled = 0, calls = 0;
    const body = () => new ReadableStream<Uint8Array>({cancel() {cancelled++;}},{highWaterMark:0});
    const stub = new ActorNamespaceBinding(account,"ClassOne",{async invokeActor() {calls++;throw new Error("unexpected transport");}},"OBJECTS").getByName("same");
    const abort = new AbortController();
    if (preAborted) abort.abort(new Error("cancelled"));
    const first = preAborted ? undefined : stub.fetch("https://example.com/",{method:"POST",body:body(),duplex:"half"} as RequestInit);
    const pending = stub.fetch("https://example.com/",{method:"POST",body:body(),duplex:"half",signal:abort.signal} as RequestInit);
    const settled = Promise.allSettled(first ? [first,pending] : [pending]);
    if (!preAborted) abort.abort(new Error("cancelled"));
    expect(cancelled).toBe(preAborted ? 1 : 2);
    expect((await settled).every(result => result.status === "rejected")).toBe(true);
    expect(calls).toBe(0);
  }
});
