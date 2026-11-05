Run the syntax-data decoder property with:

```sh
cargo +nightly fuzz run --fuzz-dir rust/crates/markdown/fuzz syntax_data
```

Its three corpus files are exact copies of the maintained Syntect production
syntax sets (both newline modes) and themes. See
[`../../../vendor/syntect-5.3.0/README.md`](../../../vendor/syntect-5.3.0/README.md)
for capture provenance. The same property runs in the Markdown integration
test, alongside parsing through every bundled grammar. The shared library
property checks round trips, unsupported versions, truncated values, invalid
MessagePack and trailing decoded bytes for syntax sets, themes and the original
lazy grammar payloads. Every malformed decoded value is checked both directly
and after recompression through the production readers.

The retained mutations remove version rejection for the actual private lazy
grammar type or the theme type. Each must make the same corpus fail with
`accepted unsupported version in compressed syntax data` for that type:

```sh
git apply rust/crates/markdown/fuzz/mutations/syntax-data-lazy-version.patch
cargo +nightly fuzz run --fuzz-dir rust/crates/markdown/fuzz syntax_data rust/crates/markdown/fuzz/corpus/syntax_data -- -runs=0
git apply -R rust/crates/markdown/fuzz/mutations/syntax-data-lazy-version.patch
```

Repeat with `syntax-data-themes-version.patch`, then run the corpus again on the
restored decoder; it must pass.
