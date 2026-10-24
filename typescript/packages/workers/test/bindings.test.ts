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
