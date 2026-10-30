# A6 canonical AUTH / SECRETS API sketch

Owner: rb-identity, 2026-10-26. Proposed extensions, not a qualified dependency pin. First delivery remains the separate P1 SDK security PR. Predecessor source/proofs and open findings are recorded in `../auth/adoption-rb-identity.md`.

## Reuse and proposed signatures

Retain `acyclic_auth::verify(bytes, trust, now) -> Result<VerifiedCapability, VerifyError>`, its private construction, `VerifiedCapability::check`, `Effective`'s conjunctive grant sets and `TrustEvidence::check`. TrustEvidence contains no coverage/grants: it cannot authorize a subtree. Reuse harness VerifiedScope and the filesystem policy compiler rather than introducing another scope evaluator or serialized verified flag.

Extend canonical verification with admission budgets checked before decoding/allocation: total raw bytes, chain links, aggregate grants/caveats and canonical path depth. The current 12 KiB credential envelope alone does not bound the direct verifier. Keep Cloud's existing ceiling; do not raise limits. Signature verification precedes expensive semantic subset conversion. A separate pure bounded production hook exposes the same decision/subset arithmetic to Kani/properties.

Proposed auth-owned opaque coverage evidence, using existing canonical resource/action/request types:

```rust
// Sketch names; construction remains inside the canonical verifier/decision.
fn authorize_effects(
    scope: &VerifiedCapability,
    effects: &CanonicalEffects,
    cut: &AuthenticatedPolicyCut,
    now: TrustedTime,
) -> Result<AuthorizedEffects, AccessError>;

impl AuthorizedEffects {
    fn require_current(
        &self,
        request_digest: &RequestDigest,
        precommit: &AuthenticatedPolicyCut,
        lease: &AcceptedOwnerFence,
        now: TrustedTime,
    ) -> Result<(), AccessError>;
}
```

AuthorizedEffects binds account/audience, canonical request digest including exclusions, source cut, coverage, expiry, actual accepted keyring stream head, policy revision and accepted owner fence. AuthenticatedPolicyCut comes from canonical authenticated namespace reads, including records that advance the head without changing interpreted keyring fields. It is never an operator-supplied revision number or process-local invalidation counter. The service compares the real pre-commit keyring state at authoritative direct/coordinated publication; no async preproposal check substitutes for that comparison. A mutation cannot authorize itself. During partition, authority is the last locally ordered policy; immediate global revocation is not promised.

Auth stays below harness in the dependency graph: the shared entrypoint accepts auth's verified capability; harness's existing crate-private VerifiedScope adapts to this evidence at its composition boundary. It is not exported back into auth or replaced with an independently implemented verifier.

Native runtime remains the only OS signing-custody implementation. Proposed `ProvisionalSigningKey` owns newly created credential cleanup until durable owner publication commits ownership; reference-only durable metadata and missing/replaced-custody refusal remain. Pi and local owners consume this common lifecycle/bootstrap, including authenticated identity-preserving migration. Existing credentials and another publisher's winner cannot be removed during abandonment. Memory/test/WASM explicit bootstrap is distinguished from production OS custody.

One shared auth request-budget layer supplies identity/secrets with the existing 30-second whole-request deadline, including headers and successful body. It preserves fresh credential resolution, sensitive authorization headers, origin admission, redirect/retry refusal and fixed redacted failures. Deterministic stalled-peer proofs exercise the real adapters; a watchdog does not constitute acceptance.

Shared typed access errors preserve unauthenticated, denied (sanitized decision), missing, stale/revoked/expired and unavailable distinctions, with exhaustive adapter mappings. Extend existing errors rather than keeping a parallel mapper. One sensitive refreshable interceptor consumes the existing CredentialProvider and request-local credential override. No duplicated bearer parser, secret-bearing Debug or separate refresh policy per product.

## Invariants and cost

Owner deny is final. Every conjunctive parent grant set must permit the requested operation; unions across parents never widen access. Prefix matching uses canonical segments, expiry is half-open (`now < expires`), and revision is equality to accepted policy, never numeric privilege ordering. Empty operations deny. Compile/cache the existing typed policy once per authenticated revision; invalidate on revision, expiry or lease movement. Admit storage/copy/cryptographic work through the caller's original ledger.

Subtree Fork consumes the canonical Streams request and exclusions (ForkRequest/ForkMutation field 5, CommittedFork field 6 in the adopted source). Require covering source Read and destination Fork/Commit for every included descendant and protected path without descendant enumeration. An exact-root grant cannot cover an unbounded subtree. Canonical exclusions narrow effects only. Children tail projection adds Read to List. Bind direct and coordinated effects to the same request and pre-commit authority cut.

Initial verify/compile costs O(bounded bytes + signatures + grants/caveats); admitted point lookup is bounded by canonical path depth and applicable grant intersections. Subtree proof cost depends on bounded grant/exclusion structure, never descendant count. The publication predicate compares fixed authenticated heads/fences plus trusted time; any cross-authority coordination must be charged and reported explicitly. No issuer/Global/control-plane call enters the request hot path.

Audit guarantee is explicit: acknowledged mint/write facts remain durable where the authoritative transaction includes them. Reads/denials use bounded asynchronous audit with visible loss accounting, matching the current Cloud guarantee; they are not described as durably recorded on every decision. A future durable-every-decision contract would require acknowledged append availability/latency. Audit append is private and nonrecursive; audit failure cannot grant denied work.

## Private-read consumer and deletion contract

The sessions handoff  is adopted by rb-identity. This supersedes the retired b-authority delivery. rb-identity owns both the canonical Rust decision and deletion of the TS authority copy in `typescript/packages/harness/src/task-context.ts:98-119`; sessions consumes the released API for its remaining host qualification. No peer-branch edit is requested.

The existing Rust `TaskContext::read_private_path` must remain the public operation, with `read_pinned_source` and `read_bound_private_path` sharing the task-scope admission and `conversation/private_read.rs::verified_private_path` sharing owner/result verification. Native, browser and installed TS must call this Rust boundary through the existing bindings. Extend this boundary and the existing verified-scope decision; do not introduce a second TS grant reducer, bridge, TaskVolume or deprecated coordinator dependency. `contracts.verifyFileBytes` remains the shared descriptor verifier.

The binding must retain authenticated task authority, the bound owner/provider, requested volume/prefix/path, expected retained generation, and original byte/path/result bounds. A caller-supplied grant list or serialized verified flag cannot replace authenticated task authority. Provider responses must be checked by the same Rust boundary before returning bytes. Internal/.system and escape-path handling must follow Rust's canonical path rules.

Release requires native/browser/installed-TS positives and denials for absent/narrowed grants, owner/provider substitution, retained-generation mismatch, internal and escaping paths, body substitution and body/result limits. Retain actual-old and bypass controls, exact source and built-artifact receipts. Existing Linux/FUSE proofs alone do not close this deletion. Deliver sessions the exact signed public source commit, exported Rust/binding symbols and signatures, package/artifact identities and these receipts when released. Current status: proposed contract only; no released A6 consumer API or signed dependency pin.

## Deletions, proof and integration

Delete actual current redundant label/projection checks, repeated same-process signature verification, bootstrap variants, bearer paths and catch-all mappers only after their shared replacement is consumed. Preserve independent authenticated direct ingress, original allocation/time/cancel checks, retained-generation checks and contracts.verifyFileBytes. Historical audit site counts are not a current inventory. Remove per-event harness MAC only when A6 authority integration proves the replacing stream write boundary.

Prove production trie/decision against an independent segment/grant specification with generated insertion/deny/allow cases. Controls cover unchecked parent, textual prefix, union/wider operations, widened expiry, dropped revision and stale cached scope. Exercise the real publication fence with deterministic concurrent policy/lease movement, loom/DST where applicable, and bounded production Kani hooks. Generic provider conformance plus URL/env-credential runner exercises memory/local/arbitrary endpoint; no hosted fork, public fault endpoint or fake adapter that implements authorization itself. Every test <10 s; affected-crate Linux jobs <600 s.

Cloud review obligations already adopted from `../cloud-review` discussions and `../../cloud/rb-identity/hostability-response.md`: actual observed keyring head, pre-commit no-self-grant, direct plus accepted/coordinated publication, exact-path subtree denial, bounded direct decode, honest partition/audit semantics. No such publication API is yet qualified. rx-hostability owns Streams publication; rb-identity supplies canonical evidence and Trust/Edge coverage. Main-green, sessions and verify consume the same production types under the handover's named proof/deletion contracts.
