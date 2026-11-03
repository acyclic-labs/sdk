# Client session decoder controls

From the repository root, exercise the original command, reply and layout
decoders with the retained corpus:

```sh
cargo +nightly-2026-07-01 fuzz run --fuzz-dir rust/fuzz client_files \
  --no-default-features --features client-files --dev --sanitizer none \
  rust/fuzz/corpus/client_files -- -runs=512
```

The corpus manifest records the source, producer and hashes. The protocol
records contain a real encoded command, one NUL byte and its encoded reply.
They were captured from the authenticated local filesystem journey in
`session_files_resume_reopen_and_version_refusal`; its configured model is
not called by that journey. Derived invalid records change one recorded
property, without replacing the provider or storage path.

Apply one production patch in a clean checkout and run the corresponding
retained input through that same fuzz target with `-runs=1`. Keep the target
unchanged. The target must fail at the named assertion; a compiler error or
an unrelated crash is not a successful control. Reverse the patch before
the next control and before running the restored corpus.

| Production patch | Corpus input | Required assertion |
| --- | --- | --- |
| `layout-version.patch` | `unsupported-layout-version` | `layout version admitted outside v1` |
| `command-path-duplicates.patch` | `duplicate-request-path` | `client files admitted duplicate paths` |
| `reply-file-path.patch` | `mismatched-reply-path` | `reply file path did not match request` |

The restored corpus must pass. Retain the instrumented coverage report with
the exact source and invocation; a bounded run does not establish complete
branch coverage or native terminal acceptance.
