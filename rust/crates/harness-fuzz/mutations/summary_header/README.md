# Summary header selection negative

This mutation makes summary rendering reuse the ordinary declaration bytes,
ignoring the explicit strip choice. It must fail the existing declaration
property at `summary retained executable builtins or changed client declarations`.

From the repository root:

```sh
git apply rust/crates/harness-fuzz/mutations/summary_header/keep-server-tools.patch
cargo nextest run --locked -p acyclic-model-render --lib -E 'test(tools_tests::declarations_preserve_order_and_visit_each_borrowed_schema_once)'
git apply -R rust/crates/harness-fuzz/mutations/summary_header/keep-server-tools.patch
```

The property uses generated operator-authored declarations and schemas, not
captured provider output. Its independent wire grammar checks every supported
family, both summary choices, unchanged ordinary bytes, client declaration
order and contents, bounded schema visits, and repeated production rendering.
The original freeform property covers the native grammar declaration too.
