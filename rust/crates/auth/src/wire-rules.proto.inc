// Normative acyclic.auth.v1 wire rules:
// Capability.version is exactly 1 and enters each block transcript as LE u32
// after its LE u64 index. Unsupported versions have a distinct error.
// Block 0 contains only the PUBLIC Birth + complete KeyCertificate chain.
// No block contains an account bearer or a signing key (except the tip proof).
// Verify Birth with environment trust, each key with the trusted account issuer
// root (genesis or authenticated keyring rotation, NEVER an offline leaf), block 0
// with the certified leaf, and block k with block k-1.next_public_key.
// Certificate signatures use BIRTH_DOMAIN / KEY_DOMAIN followed by canonical
// protobuf claims; snapshots use KEYRING_DOMAIN followed by canonical snapshot.
// Domains are acyclic.auth.v1/{birth,key,keyring} followed by a NUL byte.
// Canonical ingress rejects unknown/duplicate fields and misplaced certificates.
// Check ALL ancestor key ids/epochs and parent_revocation_ids, not just the leaf.
// A revoked block or online key invalidates every descendant. Online issuance
// MUST include all parent block ids and the complete parent certificate ancestry.
// Issuer-certified Principal claims cannot appear in attenuation blocks.
// LineageAppend changes acting identity (last id, else certified root id) only.
// It cannot add roles/groups/attributes. Subject references bind at the declaring
// block, not the final tip; lineage read matching is ancestor-or-self, pins are
// single-valued acting-id or literal. All grant/filter/pin/holder/time/audience
// caveats intersect; each grant set is a union. Owner policy Deny always wins.
// Audience is an exact canonical ResourcePattern (prefix=false); no slash trim.
// All timestamps are Unix SECONDS UTC; validity is [not_before, expires_at),
// at most 30 days. Values after 9999-12-31 are invalid (catches modern millis).
// MaxDepth is an absolute terminal block index (root=0), minimum over all blocks.
// Sealing appends no block and consumes no depth. Limits share the chain root:
// rates/concurrency at admission, cumulative lifetime totals globally at the
// metering owner/billing ledger. Delegation and renewal never reset counters.
// Label is advisory, scoped to its block revocation id, and grants no authority.
// Product resource encodings/actions and static/dynamic extractor vocabularies
// live in validation/v1/options.proto FileOptions. Owners encode substitutions
// BEFORE expansion: Objects bucket / entire opaque key as separate components;
// other products canonicalize paths then encode each component. Never normalize
// Objects keys or decode before exact/whole-segment-prefix matching. New product catalogs
// do not require changing the verifier. HTTP-only routes use FileOptions.http_routes.
// Resource prefixes match complete slash-delimited component lists, never
// partial components: objects:/logs covers that bucket and its keys, but never
// objects:/logs-private. A final slash on a prefix is a segment separator.
// Opaque Objects keys remain encoded as one component; use bucket scope or exact
// encoded keys, not a partial encoded-key prefix.
// RecordAttributes is single-valued: every duplicate name is rejected at decode,
// including equal-value duplicates. All record consumers use this admitted type;
// certified principal attribute decoding applies the same uniqueness rule.
// External WebSocket destinations use typed DNS/IP and nonzero port facts, not
// resource grants. DNS is ASCII case-insensitive (one trailing dot is equivalent).
// Wildcard *.example.com matches subdomains at a dot boundary, never the apex.
// IP/CIDR rules match literal IP facts of the same family; no DNS lookup occurs.
// Each socket rule is host AND port (empty ports means 443); rules are a union,
// and ancestor certificates plus caveat grant sets are all intersected.
// A child certificate preserves principal kind/id and inherited lineage, and
// may only remove roles/groups/attested attributes. No child can certify a claim
// absent from its parent; only lineage may append.
