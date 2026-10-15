import { create, toBinary } from "@bufbuild/protobuf";

// Test-only platform peer. Each source has at most one outstanding record;
// credits wake only the sources that can progress, without a timer.
export function sessionService(wire, follow) {
  return async function* (requests, context) {
    const routes = new Map();
    const output = [];
    const waiters = new Set();
    let wakeOutput;
    let ended = false;
    let failure;
    const credit = { bytes: 0n, frames: 0 };
    const wake = () => { for (const resolve of waiters) resolve(); waiters.clear(); };
    const emit = frame => { output.push(frame); wakeOutput?.(); };
    const frame = (channelId, generation, body) => create(wire.SessionFrameSchema, { channelId, generation, body });
    const stop = () => {
      ended = true;
      for (const route of routes.values()) route.controller.abort();
      routes.clear();
      wake();
      wakeOutput?.();
    };
    context.signal.addEventListener("abort", stop, { once: true });
    const source = async route => {
      try {
        for await (const item of follow(route.subscription, route.controller.signal)) {
          const data = frame(route.id, route.generation, { case: "data", value: create(wire.SessionDataSchema, {
            after: { next: item.record.sequence + 1n }, accepted: item.record.sequence + 1n,
            item: item.invalid ? undefined : { case: "record", value: item.record },
          }) });
          const bytes = BigInt(toBinary(wire.SessionFrameSchema, data).length);
          while (!route.controller.signal.aborted && (credit.frames === 0 || credit.bytes < bytes || route.credit.frames === 0 || route.credit.bytes < bytes)) {
            await new Promise(resolve => waiters.add(resolve));
          }
          if (route.controller.signal.aborted) return;
          credit.bytes -= bytes; credit.frames--;
          route.credit.bytes -= bytes; route.credit.frames--;
          emit(data);
        }
      } catch (error) {
        if (!route.controller.signal.aborted) emit(frame(route.id, route.generation, { case: "closed", value: create(wire.SessionClosedSchema, { code: "unavailable", message: String(error) }) }));
      }
    };
    const read = async () => {
      try {
        for await (const input of requests) {
          switch (input.body.case) {
            case "hello":
              credit.bytes = input.body.value.receiveBytes; credit.frames = input.body.value.receiveFrames;
              emit(frame(0n, 0n, { case: "welcome", value: create(wire.SessionWelcomeSchema, { version: 1, nodeId: "fixture" }) }));
              break;
            case "interest":
              for (const removed of input.body.value.remove) {
                routes.get(removed.channelId)?.controller.abort();
                routes.delete(removed.channelId);
              }
              for (const subscription of input.body.value.upsert) {
                if (subscription.kind.case !== "follow") throw new Error("fixture expects Streams follows only");
                const { channelId: id, generation } = subscription.channel;
                routes.get(id)?.controller.abort();
                const route = { id, generation, subscription, controller: new AbortController(), credit: { bytes: 0n, frames: 0 } };
                routes.set(id, route);
                route.controller.signal.addEventListener("abort", wake, { once: true });
                emit(frame(id, generation, { case: "opened", value: create(wire.SessionOpenedSchema, { resume: subscription.resume }) }));
                void source(route);
              }
              emit(frame(0n, 0n, { case: "interestApplied", value: create(wire.SessionInterestAppliedSchema, { revision: input.body.value.revision }) }));
              wake();
              break;
            case "window": {
              const balance = input.channelId === 0n ? credit : routes.get(input.channelId)?.credit;
              if (balance !== undefined) { balance.bytes += input.body.value.bytes; balance.frames += input.body.value.frames; }
              wake();
              break;
            }
          }
        }
      } catch (error) { failure = error; }
      finally { stop(); }
    };
    void read();
    try {
      while (true) {
        while (output.length === 0 && !ended) await new Promise(resolve => { wakeOutput = resolve; });
        if (output.length === 0) { if (failure !== undefined) throw failure; return; }
        yield output.shift();
      }
    } finally { context.signal.removeEventListener("abort", stop); stop(); }
  };
}
