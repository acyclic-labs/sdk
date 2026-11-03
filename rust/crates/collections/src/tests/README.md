# Fixed index read admission

The retained property exercises the production `FixedIndex` with a read-only
view of the original work owner. Insertion retains its original allocation
tokens. Ordered successors and mutable exact values are compared with an
independent ordered map, including every branch coordinate at several key
widths. Every insufficient work allowance refuses before exposing a mutable
value; cancelled empty reads must also refuse.

```sh
cargo test --locked -p acyclic-collections --lib tests::ordered_seek_and_mutation_preserve_original_credit_and_sparse_cursors -- --exact
```

The planted production mutation removes exact-read admission on an empty index:

```sh
git apply rust/crates/collections/src/tests/unchecked_empty_read.patch
cargo test --locked -p acyclic-collections --lib tests::ordered_seek_and_mutation_preserve_original_credit_and_sparse_cursors -- --exact
git apply -R rust/crates/collections/src/tests/unchecked_empty_read.patch
```

Require the runtime assertion `cancelled empty exact read must be refused`.
Compilation failure is not detection. After restoring the exact source,
run the same property successfully. These controls cover read admission and
bounded traversal, not a consumer's value growth or resident custody transfer.
