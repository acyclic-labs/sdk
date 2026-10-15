// Generated from acyclic_actors::guest. Do not edit.
export type GuestRequest = {
/**
 * Original absolute URL.
 */
url: string,
/**
 * HTTP method.
 */
method: string,
/**
 * Ordered header pairs.
 */
headers: Array<[string, string]>,
/**
 * Bounded request body bytes.
 */
body: Array<number>, };
export type GuestResponse = {
/**
 * HTTP status code.
 */
status: number,
/**
 * Ordered header pairs.
 */
headers: Array<[string, string]>,
/**
 * Bounded response body bytes.
 */
body: Array<number>, };
export type GuestSocketEvent = { "type": "open",
/**
 * Negotiated subprotocol.
 */
protocol: string, } | { "type": "message",
/**
 * Decimal u64 frame offset.
 */
offset: string,
/**
 * Frame bytes.
 */
data: Array<number>,
/**
 * Whether this is a text frame.
 */
text: boolean, } | { "type": "close",
/**
 * WebSocket close code.
 */
code: number,
/**
 * Close reason.
 */
reason: string, } | { "type": "error",
/**
 * Stable error code, without credentials.
 */
code: string, } | { "type": "reconnecting" };
export type GuestEvent = { "type": "stream",
/**
 * Logical subscription ID.
 */
subscription: string,
/**
 * Source stream path.
 */
stream: string,
/**
 * Decimal u64 sequence.
 */
sequence: string,
/**
 * Decimal u64 publication time.
 */
committedAtMicros: string,
/**
 * Standard base64 record bytes.
 */
value: string,
/**
 * Lowercase hex commit ID.
 */
commitId: string, } | { "type": "invoke",
/**
 * Opaque invocation input bytes.
 */
payload: Array<number>, } | { "type": "fetch",
/**
 * Portable request.
 */
request: GuestRequest, } | { "type": "timer",
/**
 * Timer identity, also the inbox append idempotency key.
 */
timerId: string,
/**
 * Decimal u64 Unix deadline in milliseconds.
 */
deadlineUnixMs: string, } | { "type": "socket",
/**
 * Runtime-owned connection identity.
 */
connectionId: string,
/**
 * Socket event.
 */
event: GuestSocketEvent, };
export type CodecIdentity = {
/**
 * Stable artifact codec name.
 */
name: string,
/**
 * Artifact-defined format version.
 */
version: string, };
export type Checkpoint = {
/**
 * Encoder identity.
 */
codec: CodecIdentity,
/**
 * Encoded logical state. No runtime heap format is implied.
 */
bytes: Array<number>, };
export type GuestError = { "type": "missingIdentity" } | { "type": "incompatibleCodec" } | { "type": "decode", "detail": string } | { "type": "encode", "detail": string } | { "type": "handler", "detail": string };
export type CheckpointCodec<State> = {
/**
 * Identity written on the next checkpoint.
 */
identity: CodecIdentity,
/**
 * Explicit decoder compatibility, including identity itself.
 */
readable: Array<CodecIdentity>,
/**
 * Artifact encoder.
 */
encode: (state: State) => number[],
/**
 * Artifact decoder, including migration of readable older versions.
 */
decode: (checkpoint: Checkpoint) => State, };
export type GuestContext<Bindings, State> = {
/**
 * Scoped binding handles, never deployment secrets in plain configuration.
 */
env: Bindings,
/**
 * Injectable Unix millisecond clock; shared with the local runtime.
 */
clock: () => bigint,
/**
 * Stable across retries; null only for restore and quiesced checkpoint.
 */
deliveryId: string | null,
/**
 * Artifact-owned codec hooks.
 */
codec: CheckpointCodec<State>,
/**
 * Effect retry keys derived from the admitted delivery identity.
 */
retryKey: (effect: string) => string, };
export type ActorDefinition<State, Bindings> = {
/**
 * Restore absent state or decode/migrate a retained checkpoint.
 */
restore: (checkpoint: Checkpoint | null, context: GuestContext<Bindings, State>) => State | Promise<State>,
/**
 * Handle a single delivery. Public Invoke may return opaque response bytes.
 */
event: (state: State, event: GuestEvent, context: GuestContext<Bindings, State>) => number[] | null | Promise<number[] | null>,
/**
 * Capture only after all admitted handlers and exclusive sections quiesce.
 */
checkpoint: (state: State, context: GuestContext<Bindings, State>) => Checkpoint | Promise<Checkpoint>,
/**
 * HTTP entry, using the fetch member of the single event union.
 */
fetch: (state: State, request: GuestRequest, context: GuestContext<Bindings, State>) => GuestResponse | Promise<GuestResponse>, };
