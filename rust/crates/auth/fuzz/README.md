The `capability/public-certificates` and `capability/lineage-chain` captures
retain the original v1 writer bytes as unsupported-format inputs.

`capability/v2-signed-capability` and `capability/v2-attenuated-capability` are
byte-for-byte copies of the corresponding `rust/codec-fuzz/corpus/auth` captures
produced by `rust/codec-fuzz/examples/capture.rs` on 2026-10-31. They use the
original `testing::root` and `VerifiedCapability::attenuate`/`encode` producers
with deterministic SDK test keys. Their source provenance and exact bytes are
also recorded in `compatibility/frames/golden.hex` and the shared corpus
manifest. The contract test checks current admission and historical refusal
without changing either captured format.

The `addressing/{actor,resource,resource-hostname,canonical-alabel-hostname}`
seeds are byte-for-byte copies of `rust/codec-fuzz/corpus/identities` captures.
`corpus/SOURCES.tsv` in that shared project records their lengths, SHA-256 and
original producers: `ActorReference::encode`, `ResourceIdentity::encode`,
`ResourceIdentity::hostname` and `CanonicalHost::parse`.
They exercise the binary identity and plain hostname inputs consumed directly
by this target. Shared `route-*` captures include a NUL-separated URL context
for the shared route oracle; that tuple is not an input format of this target.
These original captures use public deterministic SDK identities. Copying them
does not establish a new campaign, branch-coverage or mutation-test result.

The `credential` target and shared `auth` target use the same
`acyclic_auth::credential::testing::check` oracle as the credential property.
It checks the real provider's admission and immediate completion, preserves
the original bytes through the bearer writer and requires injected transport
delimiters to fail. The original `auth/v2-opaque-bearer` shared capture remains
owned by `rust/codec-fuzz/examples/capture.rs`. Its byte-for-byte copy in
`credential/v2-opaque-bearer` is also admitted by the credential property, so
ordinary property validation exercises the retained real-writer input.

With nightly Rust and cargo-fuzz installed, run `python3 negative-control.py`
from this directory. The default `delimiter` control removes only the
production delimiter-admission check. `python3 negative-control.py text-decoder`
substitutes the standard alphabet for the production base64url decoder;
`python3 negative-control.py json-field` drops decoded bytes in the actual JSON
field adapter. Each requires its specific unchanged semantic assertion to fail.
Compilation failures do not satisfy the controls. The script restores the
original source bytes and checks that the oracle did not change. These controls
qualify those defects, not complete coverage of credential cache, source changes
or transport implementations.

The text and JSON controls use `credential/v2-signed-capability`, a byte-for-byte
copy of the 451-byte `capability/v2-signed-capability` capture described above.
The shared corpus manifest records SHA-256
`ef98dfe0fbf265ffa36881f04905afede048060d91adf3731ba5979444143f6a` and
`acyclic_auth::testing::root` as its original producer. Its signed bytes exercise
the URL alphabet and decoded-byte preservation without introducing another
writer or fixture.

The same oracle also exercises the production capability text codec and its
JSON field adapter. Binary inputs round-trip through the actual writers;
arbitrary accepted text must have its canonical spelling, and padding,
invalid-alphabet and NUL suffixes must refuse through both entrances. Raw JSON
also reaches the field adapter, including non-string input. These checks admit
format only; they do not authenticate the decoded capability or qualify an
enclosing product message.

`trust_record` retains signed records captured by the actual `Issuer` and
`AccountRoot` writers in `examples/trust_corpus.rs`. Regenerate from
`rust/crates/auth` with `cargo run --locked --example trust_corpus`.
`corpus/trust-record-sources.tsv` records each producer, length and SHA-256.
All signing keys are public deterministic test keys. The records cover a foreign
account keyring/checkpoint, an advanced selected-account floor and consecutive
environment rotations. A record is tried intact before separator-based sequences,
since signed wire bytes may contain the sequence separator.

Replay keeps only records admitted by the original selected-account scope and
compares custody with the actual follower, rather than with a local writer that
may lag accepted external records. The maximum observed served floor survives
unavailable intervals. Environment changes still exclude the fixed-anchor
account replay assertion; this target does not qualify rotation catchup or live
source-loss topology. `python3 negative-control.py trust-target` removes the
production follower scope guard and requires the unchanged target's disjointness
assertion to fail. It restores the source and never counts a build error as
mutation detection.
The capability target also reaches the original issuance request and reply
admission wrappers. `examples/issuance_corpus.rs` captures the actual request
writer and all reply decisions, including a certificate from the original
account signer. Regenerate from `rust/crates/auth` using
`cargo run --locked --features test-support --example issuance_corpus`.
`corpus/issuance-sources.tsv` records their lengths, SHA-256 and original
producers. These are public deterministic test identities and certificates.
The target requires exact canonical bytes and refuses a duplicate version
produced by the same protobuf writers. `negative-control.py issuance-request`
and `negative-control.py issuance-reply` independently remove the original
canonicality guards and must trigger their distinct unchanged assertions.
These controls exercise wire admission, not the serving store's atomic commit,
current-authority checks, transport or cancellation topology.

`secret_ref` calls the production reference JSON and protobuf admission through
one test-support oracle. `examples/secret_ref_corpus.rs` captures the original
writers, including maximum-u64 versions, escaped names and Unicode. Regenerate
from `rust/crates/auth` with `cargo run --locked --example secret_ref_corpus`.
`corpus/secret-reference-sources.tsv` records each capture's producer, length and
SHA-256. These inputs contain reference metadata only, without secret values.
The oracle checks string and numeric version identity, unknown-field and invalid
version refusal, and exact canonical protobuf admission. Run
`python3 negative-control.py secret-json` or `secret-wire` to remove the actual
JSON unknown-field or protobuf canonicality guard. Each must fail its unchanged
semantic assertion; the runner restores source bytes. This target does not
qualify secret-value custody, catalog providers or live operation cancellation.

The same mutation runner covers signed harness filesystem refinements through
`python3 negative-control.py filesystem-ancestor` and `filesystem-certified`.
It runs the existing `harness-fuzz/filesystem_access` target against its retained
`ceiling-set` policy input. The shared oracle uses the actual scope bootstrap,
append, signing and verification APIs to produce descendants in memory. Each
control removes one production admission guard and requires its specific
unchanged refusal assertion to fail. The retained unsigned policy seed originates
in the initial filesystem-access corpus; no external capture provenance is
inferred. The runner checks the shared oracle and target bytes and restores the
original production source. These controls cover ceiling restoration and
uncertified labels; source coverage and other admission branches need their own
qualification.

Both addressing fuzz entrances and the existing identity properties use Auth's
`addressing::testing` oracle for hostname identity and route decisions. The shared
corpus's seven `route-*` inputs are retained unchanged in `corpus/addressing`;
`corpus/addressing-sources.tsv` joins their original producer, size and SHA-256.
They contain actual SDK route-pattern writer output with caller URL context in
a NUL-separated fuzz tuple. That tuple is not a product format. Run
`python3 negative-control.py addressing-fragment` to remove the production URL
fragment guard and require the unchanged routing-refusal assertion to fail.
The same oracle constructs lowercase LDH labels with letter or digit edges,
checks single-byte and 63-byte boundaries, and requires invalid aliases, empty
labels and 64-byte labels to refuse. Both the existing arbitrary-byte property
and fuzz entrances call it, including for non-UTF-8 input. Run
`python3 negative-control.py addressing-case` or `addressing-length` to weaken
the production character or length guard. Each must fail its unchanged semantic
assertion. These replace the fixed label table's mock-validator control.
The generated label also supplies a 253-byte host with valid label boundaries;
adding one byte must return `HostTooLong`. Decimal and hexadecimal final labels
must return `NumericHost` before URL-parser fallback. Run
`python3 negative-control.py addressing-host-length` or
`addressing-host-numeric` to remove each actual host guard and require its
unchanged typed refusal assertion to fail. These shared checks subsume the
numeric entries in the hostname alias example.
The runner restores the source and checks oracle custody. Complete source and
branch coverage, and the other addressing guards, need separate qualification.

Binary actor/resource admission uses this same oracle before text admission.
Arbitrary nonempty input also supplies opaque fields to the real resource writer
and an account plus generated legal actor name to the real actor writer. Their
outputs must decode to the original values, so rejecting valid writer output
cannot evade the checks gated on successful decoding. The actor name generator
obeys the public 128-byte name grammar; it does not bound opaque resource fields.
The same oracle sends empty, 129-byte, slash, NUL and non-UTF-8 names through the
owning namespace constructor and actor-name validator, requiring their exact
typed refusals. `python3 negative-control.py addressing-name-length` removes
the shared name-length guard; `addressing-name-character` admits a slash through
the actual shared character guard. Both must trip the unchanged namespace
refusal assertion. Existing golden namespace/legacy/actor vectors are retained.
`python3 negative-control.py addressing-resource-admission` and
`addressing-actor-admission` make each production reader expect a different
version from its unchanged writer. Each must trip its forward admission check.
The shared field reader obtains the eight-byte length prefix as an array through
`split_first_chunk`, so no second fallible slice-to-array conversion is needed.
`python3 negative-control.py addressing-field-endian` reverses the actual shared
length reader's byte order and must trip the same writer admission assertion.
The oracle preserves both original decode/encode round trips and actor/resource grammar
disjointness. `python3 negative-control.py addressing-resource` and
`addressing-actor` swap fields in the actual production decoders; the original
writer captures `resource` and `actor` must fail their unchanged exact-byte
assertions. No reference decoder or alternative wire format is introduced.

For each admitted binary input, the shared oracle also requires a trailing byte
and a truncated final field to return `IdentityError::Malformed`. The existing
framing examples delegate those assertions and retain their full-prefix checks.
`python3 negative-control.py addressing-resource-tail` and
`addressing-actor-tail` remove each production decoder's trailing-byte guard;
the same original writer captures must trip the unchanged framing assertion.

The oracle also derives header-only aliases from admitted identity bytes using
the owning format's header boundary. Old, unknown and missing versions must
return `UnsupportedVersion` before fields are available. Both existing framing
examples reach these shared checks; the separate Actor version example is
redundant. Run `python3 negative-control.py addressing-resource-version` or
`addressing-actor-version` to bypass the actual shared header's version guard
and require each original writer capture to trip the unchanged refusal oracle.
The same header oracle changes a domain byte and removes the complete header;
resource inputs must retain `UnsupportedVersion`, and actor inputs `Malformed`.
It subsumes the separate headerless-resource example. Run
`python3 negative-control.py addressing-resource-domain` or
`addressing-actor-domain` to bypass the actual shared domain guard. Each original
writer capture must trip the unchanged typed domain-refusal assertion, while
the previously qualified version checks remain in the same shared oracle.
