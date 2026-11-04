# Cache namespace mutation control

The namespace isolation control uses
`cache::pool::tests::namespaces_isolate_bytes_decoders_and_clear`. It exercises
the shipped pool's canonical and decoded entries, clone sharing and namespace
clear. Apply `mutations/cache_namespace_alias.patch` separately to make the
production `share_cache_with` reuse its parent's namespace; the unchanged
`namespace alias` assertion must fail. Reverse the patch and require the same
test to pass. This replaces direct test-state corruption and a duplicate
panic-text test; it does not claim provider I/O or persistence coverage.

```sh
cargo test --locked -p acyclic-fs --no-default-features --features memory --lib -- \
  --exact cache::pool::tests::namespaces_isolate_bytes_decoders_and_clear
```
