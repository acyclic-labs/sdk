// Generated from acyclic_actors::guest. Do not edit.
export const GUEST_ABI_VERSION = 1 as const;
export const WEB_SOCKET_TAG_COUNT = 10 as const;
export const WEB_SOCKET_TAG_CHARACTERS = 256 as const;
export const WEB_SOCKET_ATTACHMENT_BYTES = 2048 as const;
export const WEB_SOCKET_AUTO_RESPONSE_CHARACTERS = 2048 as const;
export type AlarmInfo = {
/**
 * Zero for first delivery, increasing on each retry.
 */
retryCount: number,
/**
 * Whether this delivery follows a failed attempt.
 */
isRetry: boolean, };
export type WebSocketId = string;
export type WebSocketAutoResponse = {
/**
 * Exact incoming text frame.
 */
request: string,
/**
 * Exact outgoing text frame.
 */
response: string, };
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
body: Uint8Array, };
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
body: Uint8Array, };
export type SocketPayload = {
/**
 * Text content.
 */
text: string, } | {
/**
 * Binary content.
 */
binary: Uint8Array, };
export type SocketCloseInitiator = "peer" | "actor" | "runtime";
export type SocketTransport = "internal" | "external";
export type GuestSocketEvent = { "type": "open",
/**
 * Target actor; connection identity is on the enclosing socket event.
 */
actorId: string,
/**
 * Optional application channel.
 */
appChannel: string | null,
/**
 * Offered subprotocols.
 */
subprotocols: Array<string>,
/**
 * Host-allowlisted ordered headers, without credentials.
 */
headers: Array<[string, string]>,
/**
 * Last applied actor offset, decimal u64 (zero before first delivery).
 */
resumeAfter: string,
/**
 * Whether a declared outbound reconnect completed.
 */
reconnected: boolean,
/**
 * Optional transport diagnostics.
 */
transport: SocketTransport | null, } | { "type": "message",
/**
 * Decimal positive u64 actor delivery offset.
 */
offset: string,
/**
 * Text or binary payload.
 */
payload: SocketPayload, } | { "type": "close",
/**
 * Decimal positive u64 actor delivery offset.
 */
offset: string,
/**
 * WebSocket close code.
 */
code: number,
/**
 * Close reason.
 */
reason: string,
/**
 * Closing party, including runtime 1013 backpressure close.
 */
initiator: SocketCloseInitiator, } | { "type": "error",
/**
 * Decimal positive u64 actor delivery offset.
 */
offset: string,
/**
 * Stable error code.
 */
code: string,
/**
 * Diagnostic message without credentials.
 */
message: string, } | { "type": "reconnecting",
/**
 * One-based attempt number.
 */
attempt: number,
/**
 * Decimal u64 backoff delay in milliseconds.
 */
delayMs: string, } | { "type": "unknown" };
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
payload: Uint8Array, } | { "type": "fetch",
/**
 * Portable request.
 */
request: GuestRequest, } | { "type": "alarm",
/**
 * The sole alarm delivery counter.
 */
info: AlarmInfo, } | { "type": "timer",
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
event: GuestSocketEvent, } | { "type": "unknown" };
export type GuestEventResult = { "type": "reply",
/**
 * Opaque result.
 */
payload: Uint8Array, } | { "type": "socketAccept",
/**
 * Selected offered subprotocol.
 */
subprotocol: string | null,
/**
 * Opaque checkpointed attachment (contract maximum 2 KiB).
 */
attachment: Uint8Array,
/**
 * Checkpointed broadcast tags (at most ten, each at most 256 characters).
 */
tags: Array<string>, } | { "type": "socketReject",
/**
 * HTTP rejection status.
 */
status: number,
/**
 * Rejection reason.
 */
reason: string, };
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
bytes: Uint8Array, };
export type GuestError = { "type": "unsupportedAbi", "detail": number } | { "type": "nestedMutation" } | { "type": "missingIdentity" } | { "type": "incompatibleCodec" } | { "type": "decode", "detail": string } | { "type": "encode", "detail": string } | { "type": "handler", "detail": string };
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
encode: (state: State) => Uint8Array,
/**
 * Artifact decoder, including migration of readable older versions.
 */
decode: (checkpoint: Checkpoint) => State, };
export type CallerCapability = {
/**
 * Host handle protocol version; S1 owns the capability chain behind it.
 */
version: number,
/**
 * Invocation-scoped host handle, interpreted only by the host.
 */
handle: string, };
export type AdmittedCaller = {
/**
 * Owning account.
 */
accountId: Array<number>,
/**
 * Owning organization.
 */
orgId: string,
/**
 * Attested principal.
 */
principal: string,
/**
 * Credential identity, without credential bytes.
 */
credentialId: string,
/**
 * Actor audience admitted by the authority.
 */
actorId: string,
/**
 * Namespace of the admitted actor audience.
 */
namespace: string,
/**
 * Decimal u64 expiry in Unix milliseconds.
 */
expiresAtMs: string,
/**
 * Opaque caller capability, absent until S1 admission supplies it.
 */
capability: CallerCapability | null, };
export type RuntimeHandle = {
/**
 * Feature protocol version.
 */
version: number,
/**
 * Scoped runtime handle identity.
 */
handle: string, };
export type ActorRuntime = {
/**
 * Actor identity; absent for a stateless Worker.
 */
id: string | null,
/**
 * Account supplied by the host, separate from the existing local ID.
 */
account?: Array<number>,
/**
 * Exact namespace; absence maps to the reserved default namespace.
 */
namespace?: string,
/**
 * Actor class identity for actor-address and fork operations.
 */
class: string | null,
/**
 * Optional socket Send/Close/Broadcast/SetTags/SetAttachment/Connect authority.
 */
sockets?: RuntimeHandle,
/**
 * Optional runtime-owned auto-response configuration/timestamp authority.
 */
autoResponse?: RuntimeHandle,
/**
 * Optional setAlarm/schedule authority; fires are idempotent inbox appends.
 */
alarms?: RuntimeHandle,
/**
 * Optional SQL-over-fs-volume handle; never heap-checkpoint pages.
 */
sql?: RuntimeHandle,
/**
 * Optional host-managed lifetime extension authority.
 */
waitUntil?: RuntimeHandle, };
export type ActorMetadataWrite = {
/**
 * Actor-local metadata key.
 */
key: string,
/**
 * Opaque value; copied at the guest boundary.
 */
value: Uint8Array, };
export type ActorStateAlarm = { "type": "keep" } | { "type": "set",
/**
 * Exact u64 decimal, never a floating-point timestamp.
 */
deadline: string, } | { "type": "clear" };
export type ActorStateSeed = {
/**
 * Metadata from that same cut, without a second remote read.
 */
metadata: Array<ActorMetadataWrite>,
/**
 * Exact u64 decimal committed frame count.
 */
journalTail: string, };
export type ActorStateCommit = {
/**
 * Last assignment for each changed metadata key.
 */
writes: Array<ActorMetadataWrite>,
/**
 * Original committed journal tail, as exact u64 decimal.
 */
journalFrom: string,
/**
 * Opaque canonical journal frames, appended in order.
 */
journal: Array<Uint8Array>,
/**
 * Alarm mutation in this same conditional commit.
 */
alarm: ActorStateAlarm, };
export type GuestState<State> = { with: (mutate: (state: State) => undefined) => void };
export type GuestContext<Bindings> = {
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
 * Host-admitted caller, null for Stream/timer/restore/checkpoint deliveries.
 */
admittedIdentity: AdmittedCaller | null,
/**
 * Reserved runtime namespace, independent of manifest bindings.
 */
actor: ActorRuntime,
/**
 * Effect retry keys derived from the admitted delivery identity.
 */
retryKey: (effect: string) => string, };
export type ActorDefinition<State, Bindings> = {
/**
 * Explicit artifact handshake; hosts reject unsupported versions before restore.
 */
abiVersion: number,
/**
 * Artifact-owned codec exported as actor.codec, discoverable before update admission.
 */
codec: CheckpointCodec<State>,
/**
 * Restore absent state or decode/migrate a retained checkpoint.
 */
restore: (checkpoint: Checkpoint | null, context: GuestContext<Bindings>) => State | Promise<State>,
/**
 * Handle a single delivery. Public Invoke may return opaque response bytes.
 */
event: (state: GuestState<State>, event: GuestEvent, context: GuestContext<Bindings>) => GuestEventResult | null | Promise<GuestEventResult | null>,
/**
 * Capture only after all admitted handlers and exclusive sections quiesce.
 */
checkpoint: (state: GuestState<State>, context: GuestContext<Bindings>) => Checkpoint | Promise<Checkpoint>,
/**
 * HTTP entry, using the fetch member of the single event union.
 */
fetch: (state: GuestState<State>, request: Request, context: GuestContext<Bindings>) => Response | Promise<Response>, };
