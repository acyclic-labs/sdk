import { expect, test } from "bun:test";
import { materializeBindings } from "../src/bindings.js";
import { ActorNamespaceBinding, type semantic } from "@acyclic-labs/actors";
import { WorkerServiceBinding } from "../src/service.js";
const account = new TextEncoder().encode("account");
const namespace: semantic.Binding = {name:"OBJECTS",capability:"grant",resource:"actors:/ClassOne",targetAccount:account,actorNamespace:"ClassOne"};
const service: semantic.Binding = {name:"SERVICE",capability:"grant",resource:"workers:/worker",targetAccount:account,workerService:"worker"};

test("deployment declarations become scoped namespace and private service handles after Rust admission", async () => {
  let factories = 0;
  const requests: unknown[] = [];
  const authority = {
    namespace() { factories++; return {async invokeActor(request: semantic.InvokeActorRequest) {requests.push(request); return {status:200,headers:[],body:request.body};}}; },
    service() { factories++; return {async invokeWorker(request: semantic.InvokeWorkerRequest) {requests.push(request); return {status:200,headers:[],body:request.body};}}; },
  };
  const env = await materializeBindings([namespace,service], authority);
  expect(Object.isFrozen(env)).toBe(true); expect(factories).toBe(2);
  expect(env.OBJECTS).toBeInstanceOf(ActorNamespaceBinding); expect(env.SERVICE).toBeInstanceOf(WorkerServiceBinding);
  const objects = env.OBJECTS as ActorNamespaceBinding;
  const worker = env.SERVICE as WorkerServiceBinding;
  await objects.get(objects.idFromName("same")).rpc("actorCall", new Uint8Array([1]));
  await worker.rpc("workerCall", new Uint8Array([2]));
  expect(requests).toMatchObject([{accountId:account,namespace:"ClassOne",actorId:"same",bindingName:"OBJECTS"},{accountId:account,workerName:"worker",bindingName:"SERVICE"}]);
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
  let admitted: number[] = [];
  const pending = materializeBindings([declaration], {
    namespace(binding) {
      admitted = Array.from(binding.targetAccount);
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
  const env = await pending;
  expect(admitted).toEqual(Array.from(account));
  const binding = env.OBJECTS as ActorNamespaceBinding;
  await binding.getByName("same").rpc("call",new Uint8Array());
});

// Exercise both public wrappers through the same Fetch contract controls.
for (const kind of ["actor","worker"] as const) {
  const handle = (transport: {invoke(request: semantic.InvokeActorRequest | semantic.InvokeWorkerRequest, options?: import("@acyclic-labs/actors").ActorsCallOptions): Promise<semantic.InvokeActorResponse>}) => kind === "actor"
    ? new ActorNamespaceBinding(account,"ClassOne",{invokeActor:transport.invoke.bind(transport)},"OBJECTS").getByName("same")
    : new WorkerServiceBinding(account,"worker",{invokeWorker:transport.invoke.bind(transport)},"SERVICE");
  test(`${kind} fetch preserves bodyless statuses and HEAD responses`, async () => {
    for (const status of [204,205,304,200]) {
      const target = handle({async invoke() {return {status,headers:[],body:new Uint8Array()};}});
      const response = await target.fetch("https://example.com/",{method:status === 200 ? "HEAD" : "GET"});
      expect(response.status).toBe(status); expect(response.body).toBeNull(); expect(await response.text()).toBe("");
    }
  });
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
  test(`${kind} standard Fetch cancellation cancels a blocked body before transport`, async () => {
    for (const source of ["request","init"] as const) {
      let read!: () => void, controller!: ReadableStreamDefaultController<Uint8Array>, cancelled = 0, calls = 0;
      const reading = new Promise<void>(resolve => {read = resolve;});
      const body = new ReadableStream<Uint8Array>({start(value) {controller = value;},pull() {read();},cancel() {cancelled++;}},{highWaterMark:0});
      const target = handle({async invoke() {calls++;return {status:200,body:new Uint8Array(),headers:[]};}});
      const abort = new AbortController();
      const init = {method:"POST",body,duplex:"half",signal:abort.signal} as RequestInit;
      const pending = source === "request" ? target.fetch(new Request("https://example.com/",init)) : target.fetch("https://example.com/",init);
      const settled = pending.then(() => "fulfilled",() => "rejected");
      await reading; abort.abort(new Error("cancelled"));
      try {expect(cancelled).toBe(1);expect(calls).toBe(0);} finally {if (!cancelled) controller.close();}
      expect(await settled).toBe("rejected");
    }
  });
}
