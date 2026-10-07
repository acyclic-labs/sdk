# Full ActorId property approach

The production rule is simple: ActorId::try_from(String) succeeds exactly when
the input String is non-empty, and the as_str and String conversions preserve
the exact spelling. The prepared finite-witness harness checks direct
production calls plus the empty negative case, but it intentionally does not
claim arbitrary-string mathematics.

A bounded symbolic proof can be prepared after the module layout is fixed:
choose a symbolic byte array of fixed capacity N and a symbolic length in
0..=N; assume the prefix is valid UTF-8; construct String::from_utf8 for that
prefix; then call production ActorId::try_from. The theorem is restricted to
valid UTF-8 strings of length at most N and proves empty rejection or exact
round-trip for every value in that bounded domain. It must assert the
production result and conversions, not reimplement the emptiness check.

An all-String theorem would require verified symbolic String allocation support
in the pinned Kani build or a source-level production predicate that accepts a
bounded representation. Until that support is demonstrated, report only the
finite witness and bounded-domain scope; do not claim arbitrary strings.
