// Generated from acyclic-auth contract-derive declarations.
export type PrincipalKind = "Unspecified" | "User" | "Agent" | "Service" | "Group" | "Anonymous";
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
 * Structured alternatives resolved at the declaring block, never at the tip.
 */
subjects: Array<SubjectReference>, };
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
 * Public issuer-signed chain, required at index zero and forbidden later.
 */
certificates: CertificateChain | null,
/**
 * Conjunctive restrictions; issuer certificates establish grants, subject and expiry.
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
proof: TipProof,
/**
 * Exactly 1; also included in every signature transcript.
 */
version: number, };
export type GrantKind = { "Resource": ResourceGrant } | { "Execution": ExecutionGrant } | { "Billing": BillingGrant } | { "WebsocketConnect": WebsocketConnectGrant };
export type PinValue = { "Literal": Array<number> } | { "SubjectId": SubjectId };
export type CaveatKind = { "Grants": GrantSet } | { "ExpiresAtUnixSeconds": bigint } | { "NotBeforeUnixSeconds": bigint } | { "Audience": ResourcePattern } | { "Holder": Holder } | { "LineageAppend": LineageAppend } | { "Filter": Filter } | { "Pin": Pin } | { "Limits": Limits } | { "MaxDepth": number } | { "Label": string };
export type TipProof = { "Secret": Array<number> } | { "Sealed": Array<number> };
export type SubjectId = Record<symbol, never>;
export type SubjectLineage = Record<symbol, never>;
export type SubjectReference = {
/**
 * Exactly one structured source.
 */
kind: SubjectReferenceKind, };
export type SubjectReferenceKind = { "Id": SubjectId } | { "Lineage": SubjectLineage };
export type LineageAppend = {
/**
 * Append-only acting identities; cannot assert policy claims.
 */
ids: Array<string>, };
export type Validity = {
/**
 * Inclusive Unix seconds, UTC.
 */
not_before_unix_seconds: bigint,
/**
 * Exclusive Unix seconds, UTC; at most 30 days after not_before.
 */
expires_at_unix_seconds: bigint, };
export type Birth = {
/**
 * Account identity.
 */
account_id: string,
/**
 * Trust environment.
 */
environment: string,
/**
 * 32-byte account genesis key.
 */
genesis_public_key: Array<number>,
/**
 * Unix seconds, UTC.
 */
issued_at_unix_seconds: bigint, };
export type BirthCertificate = {
/**
 * Public claims signed by the environment trust root.
 */
birth: Birth,
/**
 * 64-byte Ed25519 signature over BIRTH_DOMAIN and canonical Birth.
 */
signature: Array<number>, };
export type CertifiedKey = {
/**
 * Same account as Birth.
 */
account_id: string,
/**
 * Unique online key identity, checked against keyring revocations.
 */
key_id: string,
/**
 * 32-byte Ed25519 leaf public key.
 */
public_key: Array<number>,
/**
 * Must meet the current keyring minimum epoch.
 */
key_epoch: bigint,
/**
 * Validity bounded by every parent.
 */
validity: Validity,
/**
 * Issuer-attested grant ceiling.
 */
grants: GrantSet,
/**
 * Issuer-attested roles, groups and attributes; immutable offline.
 */
principal: Principal,
/**
 * Previous certificate key id; absent only for genesis-signed first key.
 */
parent_key_id: string | null,
/**
 * All parent capability block ids at mint time; check every id.
 */
parent_revocation_ids: Array<Array<number>>, };
export type KeyCertificate = {
/**
 * Public claims signed only by the trusted account issuer root, never a leaf.
 */
key: CertifiedKey,
/**
 * 64-byte issuer-root Ed25519 over KEY_DOMAIN and canonical CertifiedKey.
 */
signature: Array<number>, };
export type CertificateChain = {
/**
 * Trusted environment account birth.
 */
birth: BirthCertificate,
/**
 * Complete oldest-to-leaf ancestry; check every epoch, key id and parent block id.
 */
keys: Array<KeyCertificate>, };
export type Revocation = {
/**
 * 32-byte block transcript id; invalidates it and all descendants.
 */
id: Array<number>,
/**
 * Retention deadline, no earlier than the revoked authority expiry.
 */
expires_at_unix_seconds: bigint,
/**
 * Audit reason.
 */
reason: string,
/**
 * Advisory issuer label.
 */
label: string | null, };
export type KeyringDelta = {
/**
 * Owning account.
 */
account_id: string,
/**
 * Strictly consecutive per account; gaps require resnapshot before acceptance.
 */
sequence: bigint,
/**
 * 32-byte digest of previous signed delta; zero for initial snapshot.
 */
previous_digest: Array<number>,
/**
 * Monotone epoch floor.
 */
minimum_key_epoch: bigint,
/**
 * Cumulative revoked online key ids, including ancestors.
 */
revoked_key_ids: Array<string>,
/**
 * Cumulative unexpired block revocations.
 */
revocations: Array<Revocation>,
/**
 * Current account root key, 32 bytes.
 */
root_public_key: Array<number>,
/**
 * Unix seconds, UTC.
 */
issued_at_unix_seconds: bigint, };
export type SignedKeyringDelta = {
/**
 * Canonical published state.
 */
delta: KeyringDelta,
/**
 * Ed25519 account-root signature over KEYRING_DOMAIN and canonical delta.
 */
signature: Array<number>, };
export type AuditRecord = {
/**
 * Unix seconds, UTC.
 */
at_unix_seconds: bigint,
/**
 * Oldest-to-tip block revocation ids.
 */
chain_ids: Array<Array<number>>,
/**
 * Issuer-attested identity.
 */
subject: Principal,
/**
 * Complete append-only delegation lineage, last entry is acting identity.
 */
lineage: Array<string>,
/**
 * Authenticated presenting account.
 */
holder: Holder,
/**
 * Declared semantic action.
 */
action: string,
/**
 * Canonical resource.
 */
resource: string,
/**
 * SHA-256 of effective canonical filter.
 */
filter_hash: Array<number>,
/**
 * allow, deny, mint, revoke or channel-open.
 */
decision: string,
/**
 * Decision reason.
 */
reason: string, };
