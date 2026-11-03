# Native claim decoder validation

The `native_claim_record` target includes the production claim codec directly.
It checks arbitrary bytes, writer round trips, bounded admission, torn writes,
checksum rejection, domain separation, all five field lengths and trailing data.
Grammar mutations use the production writer and checksum, not another parser.

Capture the initial corpus and run the bounded instrumented control with:

```sh
python3 rust/fuzz/native-claim-control.py /tmp/native-claim-control
```

The control captures four seed files through `Record::encode`, records their
hashes, runs 10,000 deterministic fuzz iterations, then disables the production
checksum check in a private checkout. The unchanged target must report
`claim checksum bypassed`; compilation failures do not satisfy this negative
control. Original source bytes are restored even on failure. Run this command
only in a disposable checkout with no concurrent edits. The report retains
commands, source/oracle hashes and original logs. The bounded run's instrumented
edge counts are partial coverage, not an exhaustive proof or OS ownership test.
The nightly toolchain and cargo-fuzz must already be installed.
