# Observable fuzz smoke

Build only the selected fuzzer in the existing warm Cargo target, with its original
features, profile, target and instrumentation. Then submit its binary, the entire
retained corpus and the original libFuzzer options to the shared campaign runner:

```sh
SDK_FUZZ_CAMPAIGN=/absolute/path/to/fuzz-campaign.mjs node scripts/fuzz-smoke.mjs "$binary" "$corpus" -- -seed=1 -runs=4096
```

The caller declares the workload; there is no default run count or time budget.
For example, the existing callback-path smoke remains **4096 runs**, with its full
corpus and sanitizer-coverage flags documented in
[the target README](../rust/fuzz/native-callback-path/README.md). Replace that
README's `cargo run` with `cargo build` using the same options, then use the
resulting `$CARGO_TARGET_DIR/$target/debug/callback_path` binary above. Cargo's
existing source clocks and target keep unchanged compilation units warm. Building
in another checkout or changing flags silently splits that artifact cache.

Each invocation prints exactly one `FUZZ_CAMPAIGN_TERMINAL PASS|FAIL` line, with
version 1 JSON containing the binary, corpus, options, elapsed time, phase timings
and cache evidence. A new PASS requires successful process exit, loaded coverage
instrumentation, libFuzzer's `DONE` line and matching completed run count. Compiling
the crate or replaying corpus unit tests cannot satisfy this contract. Failed runs
retain their private corpus, trace and crash artifacts at the printed path.

An unchanged campaign uses a recorded PASS from the shared action cache. Its key
includes executable bytes, all corpus names and bytes, options (including seed and
budget), working directory, platform, child environment and runner policy. The
shared tracer adds observed files, directory listings, absent probes and executed
helpers. A changed target misses independently; unrelated targets do not rerun.
Failures and unknowable traces are never cached. A new exploration uses a new
seed; a cached smoke receipt proves the recorded campaign, not additional searches.
Private generated corpus is discarded after success, so it cannot invalidate the
retained corpus or silently change the next campaign's starting state.

Set `SDK_FUZZ_CAMPAIGN` to the supplied runner module's absolute path before
invoking the command. There is no implicit host path.
Missing infrastructure fails explicitly. Supplied runners publish
their terminal through the `log` callback;
the entry point publishes FAIL if import or invocation fails before that receipt,
or if the runner returns without one. A runner that publishes FAIL and then throws
does not produce a second receipt. A FAIL receipt also exits unsuccessfully when
the runner returns normally. Repeated terminal receipts are refused before another
receipt is printed. Cache assumptions match the
shared tracer: process/kernel state and file metadata are outside the content
key, and private scratch names have no semantic significance. Non-loopback
network activity, foreign sockets, namespace changes, privileged executables and
incomplete traces prevent recording.

The real-binary control is `node --test scripts/fuzz-smoke.test.mjs` with
`SDK_FUZZ_TEST_BINARY` naming the instrumented callback-path binary. It checks an
actual 4096-run campaign, exact hits, corpus/seed/budget/binary/environment misses,
unrelated edits, preserved retained corpus, and explicit failure for non-fuzz
programs and replay-only requests.
