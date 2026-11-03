The cleanup property calls the shipped `Error::after_cleanup` operation for both
groupings of all triples of seven error classes. Its independent oracle selects
the first original cause at the highest public uncertainty tier. It checks the
public code, headless outcome and union of uncertainty facts. The existing cause
identity and success/single-failure checks remain in the same property.

Run it from the repository root:

```sh
cargo test --locked -p acyclic-harness-core --no-default-features --features filesystem-local,native-execution,test-support --lib contract::errors::cleanup_tests::cleanup_failure_retains_causes_and_uncertainty -- --exact --nocapture
```

`cleanup_nested_classification.patch` plants a bug in the actual production
operation: a nested primary incorrectly takes the last cleanup's code. Apply it
with `git apply`, run the same command, and require the assertion
`cleanup grouping changed its public class` to fail. Restore the production
source with `git apply -R` and require the same command to pass. The former
pair-only classifier assertion and nested uncertainty-only check missed this bug.
No async, physical lifecycle or platform guarantee is inferred from this property.
