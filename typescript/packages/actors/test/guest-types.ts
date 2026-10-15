import { defineActor, type CheckpointCodec, type GuestContext, type GuestEvent } from "../src/guest.js";

type State = { count: number };
type Bindings = { inbox: { append(value: number[], key: string): Promise<void> } };

const codec: CheckpointCodec<State> = {
  identity: { name: "counter", version: "1" },
  readable: [{ name: "counter", version: "1" }],
  encode: state => [state.count],
  decode: checkpoint => ({ count: checkpoint.bytes[0] ?? 0 }),
};

export const actor = defineActor<State, Bindings>({
  restore(checkpoint, context) {
    return checkpoint === null ? { count: 0 } : context.codec.decode(checkpoint);
  },
  async event(state, event, context) {
    if (event.type === "stream") {
      state.count += 1;
      await context.env.inbox.append([state.count], context.retryKey("counter"));
    }
    return null;
  },
  checkpoint(state, context) {
    return { codec: context.codec.identity, bytes: context.codec.encode(state) };
  },
  fetch(state, request, context) {
    const now: bigint = context.clock();
    return { status: 200, headers: [["x-time", String(now)]], body: [state.count, request.body.length] };
  },
});

export const artifactCodec = codec;

function exhaustive(event: GuestEvent): string {
  switch (event.type) {
    case "stream": return event.sequence;
    case "invoke": return String(event.payload.length);
    case "fetch": return event.request.url;
    case "timer": return event.timerId;
    case "socket": return event.connectionId;
    default: { const impossible: never = event; return impossible; }
  }
}
export const eventKind = exhaustive;

function negative(context: GuestContext<Bindings, State>) {
  // @ts-expect-error undeclared bindings cannot be used
  context.env.database.query("select 1");
  // @ts-expect-error effect identity is required
  context.retryKey();
  // @ts-expect-error clocks preserve u64 precision
  const time: number = context.clock();
  return time;
}
export const rejectedShapes = negative;
