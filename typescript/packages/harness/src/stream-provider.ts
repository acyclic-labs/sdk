import { WasmStream, WasmFollow } from "../generated/wasm/acyclic_harness_wasm.js";

const noPreceding = new Uint8Array();

export type ProviderReply<T> = { value: T } | { code: string };
export interface StreamProviderCursor {
  next(): Promise<ProviderReply<Uint8Array | null>>;
  close(): void;
}
/** Canonical protobuf provider port, including length-delimited cursor frames. */
export interface StreamProviderPort {
  dispatch(operation: string, input: Uint8Array): Promise<ProviderReply<Uint8Array>>;
  open(operation: string, input: Uint8Array, preceding: Uint8Array): Promise<ProviderReply<StreamProviderCursor>>;
}
export interface OwnedStreamProviderPort extends StreamProviderPort {
  /** End this binding and every cursor it owns. Other bindings remain live. */
  close(): void;
}

export function providerFailure(error: unknown): { code: string } {
  return { code: typeof error === "object" && error !== null && "code" in error
    && typeof error.code === "string" ? error.code : "unavailable" };
}

/** Transfer one canonical provider into an injected port. Rust owns canonical
 * parsing and cursor state; this adapter makes no request byte snapshots. */
export function streamProviderPort(stream: WasmStream): OwnedStreamProviderPort {
  let closed = false;
  const owners = new Set<() => void>();
  const dispose = (cursor: WasmFollow) => { cursor.close(); cursor.free(); };
  const call = async (operation: string, input: Uint8Array, preceding: Uint8Array = noPreceding)
    : Promise<ProviderReply<Uint8Array | WasmFollow>> => {
    if (closed) return { code: "unavailable" } as const;
    let request: ReturnType<WasmStream["openProviderRequest"]>;
    try { request = stream.openProviderRequest(operation, input, preceding); }
    catch (error) { return providerFailure(error); }
    let ended = false;
    const close = () => {
      if (ended) return;
      ended = true;
      owners.delete(close);
      request.close();
    };
    owners.add(close);
    try {
      const value = await request.next();
      if (closed || value === null) {
        if (value instanceof WasmFollow) dispose(value);
        return { code: "unavailable" } as const;
      }
      return { value };
    } catch (error) { return providerFailure(error); }
    finally { close(); request.free(); }
  };
  return {
    async dispatch(operation, input) {
      if (operation === "read" || operation === "follow") return { code: "invalid_argument" };
      try {
        const reply = await call(operation, input);
        if ("code" in reply) return reply;
        return reply.value instanceof Uint8Array ? { value: reply.value } : { code: "invalid_response" };
      } catch (error) { return providerFailure(error); }
    },
    async open(operation, input, preceding) {
      if (operation !== "read" && operation !== "follow") return { code: "invalid_argument" };
      const opened = await call(operation, input, preceding);
      if ("code" in opened) return opened;
      if (!(opened.value instanceof WasmFollow)) return { code: "invalid_response" };
      const cursor = opened.value;
      let ended = false;
      let pulls = 0;
      const close = () => {
        if (ended) return;
        ended = true;
        owners.delete(close);
        cursor.close();
        if (pulls === 0) cursor.free();
      };
      if (closed) { close(); return { code: "unavailable" }; }
      owners.add(close);
      return { value: {
        async next() {
          if (ended) return { value: null };
          ++pulls;
          try {
            const value = await cursor.next();
            if (closed) return { code: "unavailable" };
            if (value === null) close();
            return { value };
          } catch (error) { close(); return providerFailure(error); }
          finally { if (--pulls === 0 && ended) cursor.free(); }
        },
        close,
      } };
    },
    close() {
      if (closed) return;
      closed = true;
      for (const close of owners) close();
      stream.free();
    },
  };
}

/** A host admits the original streams:/ grant, selects its authenticated
 * endpoint and passes the original credential owner unchanged. The binding
 * uses the existing remote provider and its shared Session for reads/follows. */
export function connectStreamProvider(endpoint: string,
  credential: Parameters<typeof WasmStream.connect>[1]): OwnedStreamProviderPort {
  return streamProviderPort(WasmStream.connect(endpoint, credential));
}
