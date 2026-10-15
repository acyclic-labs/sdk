//! Compiler-linked acyclic.auth.v1 declarations.
use crate::Error;
use ts_rs::TS;

/// Attested principal category.
#[acyclic_contract_derive::enumeration(error = Error, unknown = Error::UnknownEnum)]
#[derive(TS)]
pub enum PrincipalKind {
    /// Invalid unspecified identity.
    Unspecified = 0,
    /// Human user.
    User = 1,
    /// Spawned agent.
    Agent = 2,
    /// Service workload.
    Service = 3,
    /// Group identity.
    Group = 4,
    /// Anonymous presenter.
    Anonymous = 5,
}

/// Canonical resource or byte prefix. Prefixes are literal bytes, never globs.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = crate::validation::resource)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct ResourcePattern {
    /// Canonical `<product>:/<name>`, folded by the owning product.
    #[wire(tag = 1, string)]
    pub name: String,
    /// Whether name is a byte prefix instead of an exact match.
    #[wire(tag = 2, bool)]
    pub prefix: bool,
}

/// Union of actions over a resource pattern.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = crate::validation::grant)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct ResourceGrant {
    /// Canonical resource pattern.
    #[wire(tag = 1, message)]
    pub resource: ResourcePattern,
    /// Semantic product actions such as fs/read.
    #[wire(tag = 2, string)]
    pub actions: Vec<String>,
}

/// Execution authority with a typed compute ceiling.
#[acyclic_contract_derive::message(error = Error, file = AuthFile)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct ExecutionGrant {
    /// Execution actions and resource.
    #[wire(tag = 1, message)]
    pub grant: ResourceGrant,
    /// Maximum CPU milliseconds.
    #[wire(tag = 2, uint64)]
    pub cpu_millis: Option<u64>,
    /// Maximum memory bytes.
    #[wire(tag = 3, uint64)]
    pub memory_bytes: Option<u64>,
}

/// Billing authority with a typed spend ceiling.
#[acyclic_contract_derive::message(error = Error, file = AuthFile)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct BillingGrant {
    /// Billing actions and resource.
    #[wire(tag = 1, message)]
    pub grant: ResourceGrant,
    /// Maximum spend in account currency micros.
    #[wire(tag = 2, uint64)]
    pub spend_micros: Option<u64>,
}

/// Inclusive port range, shared with Actors SocketHostRule.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = crate::validation::port)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct SocketPortRange {
    /// First port, 1 through 65535.
    #[wire(tag = 1, uint32)]
    pub first: u32,
    /// Last port, at least first.
    #[wire(tag = 2, uint32)]
    pub last: u32,
}

/// External WebSocket host rule with the Actors contract shape.
#[acyclic_contract_derive::message(error = Error, file = AuthFile)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct SocketHostRule {
    /// DNS name, wildcard suffix, IP or CIDR.
    #[wire(tag = 1, string)]
    pub host: String,
    /// Allowed ports; empty means 443.
    #[wire(tag = 2, message)]
    pub ports: Vec<SocketPortRange>,
}

/// External socket authority; internal targets use product grants.
#[acyclic_contract_derive::message(error = Error, file = AuthFile)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct WebsocketConnectGrant {
    /// Union of allowed host and port rules.
    #[wire(tag = 1, message)]
    pub rules: Vec<SocketHostRule>,
}

/// One typed grant. Grants within a block are a union.
#[acyclic_contract_derive::message(error = Error, file = AuthFile)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct Grant {
    /// Typed authority.
    #[wire(oneof = "1, 2, 3, 4")]
    pub kind: GrantKind,
}

/// A caveat grant union; an empty union denies everything.
#[acyclic_contract_derive::message(error = Error, file = AuthFile)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct GrantSet {
    /// Allowed grants.
    #[wire(tag = 1, message)]
    pub grants: Vec<Grant>,
}

/// Record header attribute.
#[acyclic_contract_derive::message(error = Error, file = AuthFile)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct Attribute {
    /// Attribute name.
    #[wire(tag = 1, string)]
    pub name: String,
    /// Opaque attribute value.
    #[wire(tag = 2, bytes)]
    pub value: Vec<u8>,
}

/// Attested identity. Later subjects preserve kind/id and append lineage.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = crate::validation::principal)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct Principal {
    /// Identity kind.
    #[wire(tag = 1, enumeration)]
    pub kind: PrincipalKind,
    /// Stable attested identity.
    #[wire(tag = 2, string)]
    pub id: String,
    /// Append-only ancestor/child lineage.
    #[wire(tag = 3, string)]
    pub lineage: Vec<String>,
    /// Mint-time groups; never rewritten by attenuation.
    #[wire(tag = 4, string)]
    pub groups: Vec<String>,
    /// Mint-time roles; never rewritten by attenuation.
    #[wire(tag = 5, string)]
    pub roles: Vec<String>,
    /// Mint-time attested attributes.
    #[wire(tag = 6, message)]
    pub attributes: Vec<Attribute>,
}

/// Half-open byte key interval.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = crate::validation::range)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct KeyRange {
    /// Inclusive start.
    #[wire(tag = 1, bytes)]
    pub start: Vec<u8>,
    /// Exclusive end; empty is unbounded.
    #[wire(tag = 2, bytes)]
    pub end: Vec<u8>,
}

/// Attribute equals one of the literal values or structured subject values.
#[acyclic_contract_derive::message(error = Error, file = AuthFile)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct AttributeMatch {
    /// Header name.
    #[wire(tag = 1, string)]
    pub name: String,
    /// Literal alternatives.
    #[wire(tag = 2, bytes)]
    pub any_of: Vec<Vec<u8>>,
    /// Also admit UTF-8 subject.id.
    #[wire(tag = 3, bool)]
    pub subject_id: bool,
    /// Also admit UTF-8 entries from subject.lineage.
    #[wire(tag = 4, bool)]
    pub subject_lineage: bool,
}

/// Ranges are OR; attribute clauses are AND. Caveat filters intersect.
#[acyclic_contract_derive::message(error = Error, file = AuthFile)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct Filter {
    /// Empty means all keys. Opaque records require every key in scope.
    #[wire(tag = 1, message)]
    pub keys: Vec<KeyRange>,
    /// Every named attribute clause must match.
    #[wire(tag = 2, message)]
    pub attributes: Vec<AttributeMatch>,
}

/// Required write attribute, literal or drawn from the structured subject.
#[acyclic_contract_derive::message(error = Error, file = AuthFile)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct Pin {
    /// Header name.
    #[wire(tag = 1, string)]
    pub name: String,
    /// Required attribute value.
    #[wire(oneof = "2, 3, 4")]
    pub value: PinValue,
}

/// Per-chain ceilings. Absent means unbounded; zero denies use.
#[acyclic_contract_derive::message(error = Error, file = AuthFile)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct Limits {
    /// Requests per second.
    #[wire(tag = 1, uint64)]
    pub rps: Option<u64>,
    /// Burst requests.
    #[wire(tag = 2, uint64)]
    pub burst: Option<u64>,
    /// Bytes per second.
    #[wire(tag = 3, uint64)]
    pub bytes_per_s: Option<u64>,
    /// Concurrent channels.
    #[wire(tag = 4, uint64)]
    pub channels: Option<u64>,
    /// Concurrent operations.
    #[wire(tag = 5, uint64)]
    pub concurrency: Option<u64>,
    /// Total bytes.
    #[wire(tag = 6, uint64)]
    pub bytes: Option<u64>,
    /// Total spend micros.
    #[wire(tag = 7, uint64)]
    pub spend_micros: Option<u64>,
    /// Total inference tokens.
    #[wire(tag = 8, uint64)]
    pub tokens: Option<u64>,
}

/// Presentation binding.
#[acyclic_contract_derive::message(error = Error, file = AuthFile)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct Holder {
    /// Authenticated presenting account; absent means anyone.
    #[wire(tag = 1, string)]
    pub account: Option<String>,
}

/// Every caveat in every block must hold. Unknown cases fail closed.
#[acyclic_contract_derive::message(error = Error, file = AuthFile)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct Caveat {
    /// Fixed caveat vocabulary.
    #[wire(oneof = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10")]
    pub kind: CaveatKind,
}

/// Signed payload. Block zero alone carries the certified account bearer.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = crate::validation::block)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct Block {
    /// Existing certified account-key JWS; required only in block zero.
    #[wire(tag = 1, string)]
    pub account_bearer: Option<String>,
    /// Conjunctive restrictions; root must establish grants, subject and expiry.
    #[wire(tag = 2, message)]
    pub caveats: Vec<Caveat>,
    /// 32-byte Ed25519 key authorized to append the next block.
    #[wire(tag = 3, bytes)]
    pub next_public_key: Vec<u8>,
}

/// Exact signed payload; verifiers reject noncanonical or unknown payload fields.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = crate::validation::signed)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct SignedBlock {
    /// Canonical protobuf Block bytes.
    #[wire(tag = 1, bytes)]
    pub payload: Vec<u8>,
    /// Ed25519 signature, 64 bytes. Revocation id is SHA-256 of its signed transcript.
    #[wire(tag = 2, bytes)]
    pub signature: Vec<u8>,
}

/// Ordered signed chain and exactly one tip proof.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = crate::validation::capability)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct Capability {
    /// Nonempty chain, oldest first.
    #[wire(tag = 1, message)]
    pub blocks: Vec<SignedBlock>,
    /// Possession of the tip secret or a final sealing signature.
    #[wire(oneof = "2, 3")]
    pub proof: TipProof,
}

/// Typed GrantKind cases. Unknown wire cases are rejected.
#[acyclic_contract_derive::oneof(error = Error, file = AuthFile)]
#[derive(Clone, Debug, PartialEq, TS)]
pub enum GrantKind {
    /// Resource payload.
    #[wire(tag = 1, message)]
    Resource(ResourceGrant),
    /// Execution payload.
    #[wire(tag = 2, message)]
    Execution(ExecutionGrant),
    /// Billing payload.
    #[wire(tag = 3, message)]
    Billing(BillingGrant),
    /// WebsocketConnect payload.
    #[wire(tag = 4, message)]
    WebsocketConnect(WebsocketConnectGrant),
}

/// Typed PinValue cases. Unknown wire cases are rejected.
#[acyclic_contract_derive::oneof(error = Error, file = AuthFile)]
#[derive(Clone, Debug, PartialEq, TS)]
pub enum PinValue {
    /// Literal payload.
    #[wire(tag = 2, bytes)]
    Literal(Vec<u8>),
    /// SubjectId payload.
    #[wire(tag = 3, bool)]
    SubjectId(bool),
    /// SubjectLineage payload.
    #[wire(tag = 4, bool)]
    SubjectLineage(bool),
}

/// Typed CaveatKind cases. Unknown wire cases are rejected.
#[acyclic_contract_derive::oneof(error = Error, file = AuthFile)]
#[derive(Clone, Debug, PartialEq, TS)]
pub enum CaveatKind {
    /// Grants payload.
    #[wire(tag = 1, message)]
    Grants(GrantSet),
    /// ExpiresAt payload.
    #[wire(tag = 2, uint64)]
    ExpiresAt(u64),
    /// NotBefore payload.
    #[wire(tag = 3, uint64)]
    NotBefore(u64),
    /// Audience payload.
    #[wire(tag = 4, string)]
    Audience(String),
    /// Holder payload.
    #[wire(tag = 5, message)]
    Holder(Holder),
    /// Subject payload.
    #[wire(tag = 6, message)]
    Subject(Principal),
    /// Filter payload.
    #[wire(tag = 7, message)]
    Filter(Filter),
    /// Pin payload.
    #[wire(tag = 8, message)]
    Pin(Pin),
    /// Limits payload.
    #[wire(tag = 9, message)]
    Limits(Limits),
    /// MaxDepth payload.
    #[wire(tag = 10, uint32)]
    MaxDepth(u32),
}

/// Typed TipProof cases. Unknown wire cases are rejected.
#[acyclic_contract_derive::oneof(error = Error, file = AuthFile)]
#[derive(Clone, Debug, PartialEq, TS)]
pub enum TipProof {
    /// Secret payload.
    #[wire(tag = 2, bytes)]
    Secret(Vec<u8>),
    /// Sealed payload.
    #[wire(tag = 3, bytes)]
    Sealed(Vec<u8>),
}

/// Canonical auth schema owner.
#[acyclic_contract_derive::file(
    family = "auth",
    messages(
        ResourcePatternProto,
        ResourceGrantProto,
        ExecutionGrantProto,
        BillingGrantProto,
        SocketPortRangeProto,
        SocketHostRuleProto,
        WebsocketConnectGrantProto,
        GrantProto,
        GrantSetProto,
        AttributeProto,
        PrincipalProto,
        KeyRangeProto,
        AttributeMatchProto,
        FilterProto,
        PinProto,
        LimitsProto,
        HolderProto,
        CaveatProto,
        BlockProto,
        SignedBlockProto,
        CapabilityProto
    ),
    enums(PrincipalKind)
)]
pub struct AuthFile;
