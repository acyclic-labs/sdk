import { defineActor, GUEST_ABI_VERSION, type CheckpointCodec, type GuestContext, type GuestEvent, type GuestState } from "../src/guest.js";

type State = { count: number };
type Bindings = { inbox: { append(value: Uint8Array, key: string): Promise<void> } };
const codec: CheckpointCodec<State> = {
  identity: { name: "counter", version: "1" },
  readable: [{ name: "counter", version: "1" }],
  encode: state => new Uint8Array([state.count]),
  decode: checkpoint => ({ count: checkpoint.bytes[0] ?? 0 }),
};
export const actor = defineActor<State, Bindings>({
  abiVersion: GUEST_ABI_VERSION,
  codec,
  restore(checkpoint) { return checkpoint === null ? { count: 0 } : codec.decode(checkpoint); },
  async event(state, event, context) {
    if (event.type === "stream") {
      let count = 0;
      state.with(value => { count = ++value.count; return undefined; });
      await context.env.inbox.append(new Uint8Array([count]), context.retryKey("counter"));
      state.with(value => { value.count += 1; return undefined; });
    }
    if (event.type === "socket" && event.event.type === "open") {
      return { type: "socketAccept", subprotocol: null, attachment: new Uint8Array(), tags: [] };
    }
    return null;
  },
  checkpoint(state) {
    let count = 0;
    state.with(value => { count = value.count; return undefined; });
    return { codec: codec.identity, bytes: codec.encode({ count }) };
  },
  fetch(_state, request, context) {
    const now: bigint = context.clock();
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode(`data: ${request.url} ${now}\n\n`));
        controller.close();
      },
    });
    return new Response(body, { headers: { "content-type": "text/event-stream" } });
  },
});
function exhaustive(event: GuestEvent): string {
  switch (event.type) {
    case "stream": return event.sequence;
    case "invoke": return String(event.payload.byteLength);
    case "fetch": return event.request.url;
    case "timer": return event.timerId;
    case "socket": return event.connectionId;
    case "unknown": return "";
    default: { const impossible: never = event; return impossible; }
  }
}
export const eventKind = exhaustive;
function negative(context: GuestContext<Bindings>, state: GuestState<State>) {
  // @ts-expect-error undeclared bindings cannot be used
  context.env.database.query("select 1");
  // @ts-expect-error effect identity is required
  context.retryKey();
  // @ts-expect-error clocks preserve u64 precision
  const time: number = context.clock();
  // @ts-expect-error mutation must finish synchronously, without an await
  state.with(async value => { value.count += 1; });
  // @ts-expect-error codec belongs to the artifact export
  context.codec;
  // @ts-expect-error byte buffers are not number arrays
  const bytes: number[] = codec.encode({ count: 0 });
  return { time, bytes };
}
export const rejectedShapes = negative;
