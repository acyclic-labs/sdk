import { expect, test } from "bun:test";
import { bindingRequest, bindingResponse, bindingSignal, readBindingRequestBody } from "../src/binding-fetch.js";

test("Fetch binding owns only selected bytes without a single-chunk assembly copy", async () => {
  for (const count of [1,2]) {
    const U8 = Uint8Array, expected = new U8([7,11,13,17,19,23].slice(0,3 * count));
    const backing = new U8(1024 * 1024);
    backing.set(expected,17);
    let index = 0, allocations = 0;
    const body = new ReadableStream<Uint8Array>({pull(controller) {
      if (index === count) { backing.fill(0,17,17 + expected.byteLength); controller.close(); return; }
      const start = 17 + 3 * index++;
      const chunk = backing.subarray(start,start + 3);
      Object.defineProperty(chunk,"slice",{value() {throw new Error("caller slice invoked");}});
      controller.enqueue(chunk);
    }},{highWaterMark:0});
    const request = bindingRequest("https://example.com/",{method:"POST",body,duplex:"half"} as RequestInit);
    globalThis.Uint8Array = new Proxy(U8,{construct(target,args,newTarget) {
      // snapshotBytes captures its intrinsic constructor. Observe only the
      // collector's final selected-size allocation, before response handling.
      if (args[0] === expected.byteLength) allocations++;
      return Reflect.construct(target,args,newTarget);
    }});
    try {
      // Negative control: an injected collector copy is visible to the counter.
      new Uint8Array(expected.byteLength);
      expect(allocations).toBe(1); allocations = 0;
      const owned = await readBindingRequestBody(request,bindingSignal(request));
      expect(allocations).toBe(count === 1 ? 0 : 1);
      expect(owned).toEqual(expected);
      expect(owned.buffer.byteLength).toBe(expected.byteLength);
      expect(request.body!.locked).toBe(false);
    } finally { globalThis.Uint8Array = U8; }
  }
});

test("Fetch binding retains the original status for response body disclosure", async () => {
  for (const [status,method] of [[204,"GET"],[205,"GET"],[304,"GET"],[200,"HEAD"],[200,"GET"]] as const) {
    let statusReads = 0, bodyReads = 0;
    const bodyless = method === "HEAD" || status !== 200;
    const response = bindingResponse({
      // Negative control: a repeated read substitutes a different status.
      get status() {return statusReads++ === 0 ? status : 418;},
      get body() {
        bodyReads++;
        if (bodyless) throw new Error("body disclosed for a bodyless response");
        return new Uint8Array([73]);
      },headers:[],
    },method);
    expect(response.status).toBe(status);
    expect(statusReads).toBe(1);
    expect(bodyReads).toBe(bodyless ? 0 : 1);
    expect(response.body === null).toBe(bodyless);
    expect(await response.text()).toBe(bodyless ? "" : "I");
  }
});

test("Fetch binding abort, refusal and producer loss release the original reader", async () => {
  for (const input of ["request","init"]) for (const mode of ["abort","shared","detached","foreign-kind","producer-error"]) {
    let read!: () => void, release!: () => void, cancelled = 0, outcome = "pending";
    let failure: unknown;
    const reading = new Promise<void>(resolve => {read = resolve;});
    const cleanup = new Promise<void>(resolve => {release = resolve;});
    const reason = new Error("original producer stopped");
    const body = new ReadableStream<Uint8Array>({pull(controller) {
      read();
      if (mode === "shared") controller.enqueue(new Uint8Array(new SharedArrayBuffer(3)));
      if (mode === "detached") {
        const chunk = new Uint8Array(3);
        structuredClone(chunk.buffer,{transfer:[chunk.buffer]});
        controller.enqueue(chunk);
      }
      if (mode === "foreign-kind") controller.enqueue(new Uint16Array(3) as unknown as Uint8Array);
      if (mode === "producer-error") controller.error(reason);
    },cancel() {cancelled++;return cleanup;}},{highWaterMark:0});
    const abort = new AbortController();
    const init = {method:"POST",body,duplex:"half",signal:abort.signal} as RequestInit;
    const request = input === "request" ? bindingRequest(new Request("https://example.com/",init)) : bindingRequest("https://example.com/",init);
    const pending = readBindingRequestBody(request,bindingSignal(request)).then(() => {outcome="fulfilled";},error => {outcome="rejected";failure=error;});
    try {
      // Early failure or completion must fail setup rather than leave it
      // waiting for a pull whose producer has already stopped.
      expect(await Promise.race([reading.then(() => true),pending.then(() => false)])).toBe(true);
      if (mode === "abort") abort.abort(reason);
      // Drain runnable promise jobs once while source cleanup remains held. A
      // pending result would prove an unwanted dependency on that cleanup.
      await new Promise<void>(resolve => {setImmediate(resolve);});
      expect(outcome).toBe("rejected");
      expect(request.body!.locked).toBe(false);
      expect(cancelled).toBe(mode === "producer-error" ? 0 : 1);
      if (mode === "abort" || mode === "producer-error") expect(failure).toBe(reason);
      else expect(failure).toBeInstanceOf(TypeError);
    } finally {release();await pending;}
  }
});
