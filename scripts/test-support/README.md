# Exact libtest evidence

Install this directory as `@acyclic-labs/test-support` and import its public API:

```js
import { verifyExactTestListing, verifyExactTests } from '@acyclic-labs/test-support/libtest';

verifyExactTestListing(names, listing.stdout);
const evidence = verifyExactTests(names, execution);
```

`names` contains distinct literal Rust test names. `execution` contains the original
`status`, `stdout`, and optional `signal` and `error`. Discovery must list exactly
those tests. Execution must report each selected case passing once and exactly
one successful summary with matching counts and no failed, ignored or measured
cases. Both LF and CRLF transcripts are accepted.

Callers own package selection, process identity, output capture, cancellation,
deadlines and cleanup. Verify those obligations before accepting the returned
evidence; the verifier neither starts processes nor imposes a runtime budget.
It performs linear work in the transcript and selected names, with no I/O or waits.

Run the shared checks with `node --test scripts/test-support/libtest.test.mjs`.
The retained transcript is an original Linux libtest result from
`cargo test --locked -p acyclic-harness-core --lib executor::json::tests::object_work::checked_codec_fuzz_corpus -- --exact --nocapture`.
Adversarial transcripts are explicit derivatives of those original bytes.
