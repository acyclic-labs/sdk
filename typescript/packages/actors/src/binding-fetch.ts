export async function readBindingRequestBody(request: Request, signal: AbortSignal): Promise<Uint8Array> {
  if (!request.body) { signal.throwIfAborted(); return new Uint8Array(); }
  const reader = request.body.getReader(), chunks: Uint8Array[] = [];
  const cancel = () => { void reader.cancel(signal.reason).catch(() => undefined); };
  signal.addEventListener("abort", cancel, {once:true});
  try {
    // A queued actor call may already be aborted before its reader is admitted.
    // Owning the reader first makes cancellation release that body as well.
    if (signal.aborted) cancel();
    signal.throwIfAborted();
    let length = 0;
    while (true) {
      const {value,done} = await reader.read();
      if (done) break;
      chunks.push(value.slice()); length += value.byteLength;
    }
    signal.throwIfAborted();
    const body = new Uint8Array(length); let offset = 0;
    for (const chunk of chunks) { body.set(chunk,offset); offset += chunk.byteLength; }
    return body;
  } finally {
    signal.removeEventListener("abort",cancel);
    reader.releaseLock();
  }
}
/** One Fetch response boundary, including statuses that forbid a body. */
export function bindingResponse(result: {readonly body: ArrayLike<number>; readonly status: number; readonly headers: readonly {readonly name:string;readonly value:string}[]}, method: string): Response {
  const body = method === "HEAD" || [204,205,304].includes(result.status) ? null : Uint8Array.from(result.body);
  return new Response(body, {status:result.status,headers:result.headers.map(h => [h.name,h.value])});
}
/** Standard Request cancellation and optional SDK cancellation share one signal. */
export function bindingSignal(request: Request, signal?: AbortSignal): AbortSignal {
  return signal ? AbortSignal.any([request.signal,signal]) : request.signal;
}

/** Keep one owned body stream when applying Request metadata overrides. */
export function bindingRequest(input: RequestInfo | URL, init?: RequestInit): Request {
  const body = init?.body ?? (input instanceof Request ? input.body : undefined);
  // Passing the body explicitly avoids a second Request's forwarding stream;
  // cancellation must reach the caller's source, including before admission.
  const options: RequestInit & {duplex?: "half"} = {...init,...(body == null ? {} : {body,duplex:"half"})};
  return new Request(input,options);
}
