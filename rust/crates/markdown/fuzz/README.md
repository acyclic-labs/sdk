Run the syntax-data decoder property with:

```sh
cargo +nightly fuzz run --fuzz-dir rust/crates/markdown/fuzz syntax_data
```

Its three corpus files are exact copies of the maintained Syntect production
syntax sets (both newline modes) and themes. See
[`../../../vendor/syntect-5.3.0/README.md`](../../../vendor/syntect-5.3.0/README.md)
for capture provenance. The same property runs in the Markdown integration
test, alongside parsing through every bundled grammar. It checks round trips,
unsupported versions and trailing bytes. Removing the version rejection from
the production decoder makes this property fail on the retained syntax corpus.
