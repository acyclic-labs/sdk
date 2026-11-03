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