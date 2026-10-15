// Generated from acyclic-auth contract-derive declarations.
export type ResourcePattern = { 
/**
 * Canonical `<product>:/<name>`, folded by the owning product.
 */
name: string, 
/**
 * Whether name is a byte prefix instead of an exact match.
 */
prefix: boolean, };
export type ResourceGrant = { 
/**
 * Canonical resource pattern.
 */
resource: ResourcePattern, 
/**
 * Semantic product actions such as fs/read.
 */
actions: Array<string>, };
export type ExecutionGrant = { 
/**
 * Execution actions and resource.
 */
grant: ResourceGrant, 
/**
 * Maximum CPU milliseconds.
 */
cpu_millis: bigint | null, 
/**
 * Maximum memory bytes.
 */
memory_bytes: bigint | null, };
export type BillingGrant = { 
/**
 * Billing actions and resource.
 */
grant: ResourceGrant, 
/**
 * Maximum spend in account currency micros.
 */
spend_micros: bigint | null, };
export type SocketPortRange = { 
/**
 * First port, 1 through 65535.
 */
first: number, 
/**
 * Last port, at least first.
 */
last: number, };
export type SocketHostRule = { 
/**
 * DNS name, wildcard suffix, IP or CIDR.
 */
host: string, 
/**
 * Allowed ports; empty means 443.
 */
ports: Array<SocketPortRange>, };
export type WebsocketConnectGrant = { 
/**
 * Union of allowed host and port rules.
 */
rules: Array<SocketHostRule>, };
export type Grant = { 
/**
 * Typed authority.
 */
kind: GrantKind, };
export type GrantSet = { 
/**
 * Allowed grants.
 */
grants: Array<Grant>, };
export type Attribute = { 
/**
 * Attribute name.
 */
name: string, 
/**
 * Opaque attribute value.
 */
value: Array<number>, };
export type Principal = { 
/**
 * Identity kind.
 */
kind: PrincipalKind, 
/**
 * Stable attested identity.
 */
id: string, 
/**
 * Append-only ancestor/child lineage.
 */
lineage: Array<string>, 
/**
 * Mint-time groups; never rewritten by attenuation.
 */
groups: Array<string>, 
/**
 * Mint-time roles; never rewritten by attenuation.
 */
roles: Array<string>, 
/**
 * Mint-time attested attributes.
 */
attributes: Array<Attribute>, };
export type KeyRange = { 
/**
 * Inclusive start.
 */
start: Array<number>, 
/**
 * Exclusive end; empty is unbounded.
 */
end: Array<number>, };
export type AttributeMatch = { 
/**
 * Header name.
 */
name: string, 
/**
 * Literal alternatives.
 */
any_of: Array<Array<number>>, 
/**
 * Also admit UTF-8 subject.id.
 */
subject_id: boolean, 
/**
 * Also admit UTF-8 entries from subject.lineage.
 */
subject_lineage: boolean, };
export type Filter = { 
/**
 * Empty means all keys. Opaque records require every key in scope.
 */
keys: Array<KeyRange>, 
/**
 * Every named attribute clause must match.
 */
attributes: Array<AttributeMatch>, };
export type Pin = { 
/**
 * Header name.
 */
name: string, 
/**
 * Required attribute value.
 */
value: PinValue, };
export type Limits = { 
/**
 * Requests per second.
 */
rps: bigint | null, 
/**
 * Burst requests.
 */
burst: bigint | null, 
/**
 * Bytes per second.
 */
bytes_per_s: bigint | null, 
/**
 * Concurrent channels.
 */
channels: bigint | null, 
/**
 * Concurrent operations.
 */
concurrency: bigint | null, 
/**
 * Total bytes.
 */
bytes: bigint | null, 
/**
 * Total spend micros.
 */
spend_micros: bigint | null, 
/**
 * Total inference tokens.
 */
tokens: bigint | null, };
export type Holder = { 
/**
 * Authenticated presenting account; absent means anyone.
 */
account: string | null, };
export type Caveat = { 
/**
 * Fixed caveat vocabulary.
 */
kind: CaveatKind, };
export type Block = { 
/**
 * Existing certified account-key JWS; required only in block zero.
 */
account_bearer: string | null, 
/**
 * Conjunctive restrictions; root must establish grants, subject and expiry.
 */
caveats: Array<Caveat>, 
/**
 * 32-byte Ed25519 key authorized to append the next block.
 */
next_public_key: Array<number>, };
export type SignedBlock = { 
/**
 * Canonical protobuf Block bytes.
 */
payload: Array<number>, 
/**
 * Ed25519 signature, 64 bytes. Revocation id is SHA-256 of its signed transcript.
 */
signature: Array<number>, };
export type Capability = { 
/**
 * Nonempty chain, oldest first.
 */
blocks: Array<SignedBlock>, 
/**
 * Possession of the tip secret or a final sealing signature.
 */
proof: TipProof, };
export type PrincipalKind = "Unspecified" | "User" | "Agent" | "Service" | "Group" | "Anonymous";
export type GrantKind = { "Resource": ResourceGrant } | { "Execution": ExecutionGrant } | { "Billing": BillingGrant } | { "WebsocketConnect": WebsocketConnectGrant };
export type PinValue = { "Literal": Array<number> } | { "SubjectId": boolean } | { "SubjectLineage": boolean };
export type CaveatKind = { "Grants": GrantSet } | { "ExpiresAt": bigint } | { "NotBefore": bigint } | { "Audience": string } | { "Holder": Holder } | { "Subject": Principal } | { "Filter": Filter } | { "Pin": Pin } | { "Limits": Limits } | { "MaxDepth": number };
export type TipProof = { "Secret": Array<number> } | { "Sealed": Array<number> };
