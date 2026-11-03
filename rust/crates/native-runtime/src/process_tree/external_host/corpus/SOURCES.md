`linux-arguments.json` contains the exact argv, exit status, signal and output
captured from the shipped `acyclic-process-host` on Linux x86_64, 2026-11-03,
by `node_process_bridge_owns_real_input_output_and_descendant_cancellation`.
Capture source: `408d5ef19c1547d1192d8c9aa6af15b56635eb04`.
SHA-256: `24085c3c6c4a0931535f6adb1779c82d9dd0d4031914194009d67b093da8a738`.

The two refused invocations have no executable. The admitted invocation runs
the installed Node.js executable with exit status 17. These original inputs
contain no installation or temporary paths; no fields or bytes were rewritten.
The existing `process_host_argument_fuzz` property replays the captured
admission decisions alongside generated option combinations and OS strings.

Capture with:

```sh
cargo test --locked -p acyclic-native-runtime --test node_process -- node_process_bridge_owns_real_input_output_and_descendant_cancellation --nocapture
```

The `PROCESS_HOST_PORTABLE_CORPUS_BASE64` line retains the original JSON bytes.
This bounded Linux corpus does not qualify other platforms or every parser
branch. The planted-bug control removes `&& !inherit_input` from the production
parser, then runs the same `process_host_argument_fuzz` property. It must reject
the now-admitted duplicate option. Restore the guard before replaying the
ordinary property. Measured branch coverage remains a separate obligation.

The retained `process_host_arguments` byte fuzzer includes the exact same
production argument module. It checks literal OS-string round trips, endpoint
admission and duplicate-flag refusal. The original recorded status is an oracle
only for these exact captured bytes; mutated records do not inherit it.

```sh
cargo run --locked -p acyclic-sdk-fuzz --no-default-features --features process-arguments --bin process_host_arguments -- rust/crates/native-runtime/src/process_tree/external_host/corpus/linux-arguments.json
```

Passing a file replays that corpus without an open-ended fuzzing campaign.
The same production duplicate-input-guard removal must fail this replay too.

`linux-endpoints.json` retains 27 additional original argv/output/status records
from the same shipped executable and integration target on Linux x86_64,
2026-11-03. Capture source: `5d1144a96f3db5598e85a85329e3d31499849ee3`.
SHA-256: `c113425a01344bb73972cc251384a7448c6928ce1bc4d59a2582f0191ba3db17`.
The `PROCESS_HOST_ENDPOINT_CORPUS_BASE64` line preserves the exact JSON bytes.
No field or byte was rewritten. These records cover missing, relative,
duplicate and conflicting options. Every invocation refuses before endpoint
IO; its absolute path uses only the platform's volume root. The existing wire
control retains successful bound completion, without another success fixture.

The byte target trusts these recorded decisions only for the exact original
bytes on Unix. Arbitrary or mutated metadata never becomes an admission oracle.
Replay the file with the command above, substituting `linux-endpoints.json`.
Removing the real duplicate-endpoint guard must fail this same replay at the
recorded admission decision. The earlier round-trip-only oracle missed that
omission: discarding an extra option still produced a canonical invocation.
That original replay covered 68 of 70 parser lines and 107 of 110 regions.

`linux-native-arguments.json` retains two original executable records captured
on Linux x86_64, 2026-11-03, at source
`51d1b81b76b812d420e778be2d6abbc66cbabc40` by the same integration target.
SHA-256: `fab277f8c96965cf230b128bf8ad0906d331a3e1126ad9cfe12acf46d27ea491`.
The `PROCESS_HOST_NATIVE_ARGUMENTS_JSON` line preserves its original JSON.
Each `argv` is the exact native OS-string bytes separated by NUL, with no
trailing separator. The original host refused the invalid UTF-8 identifier
with status 125 and retained the direct `/bin/false` child's status 1.
Node cannot pass the former byte sequence, so the capture uses the same host
executable directly. No field or byte was rewritten.

`linux-invalid-utf8.argv` and `linux-absolute.argv` contain those exact original
argument bytes, respectively. Their SHA-256 hashes are
`7e08966bdc2b15ad9b2898bfc3ebc55d34879cd1e7a57eacfdc08846dcfabf18` and
`f04e2e8870542565585b0f2bbf41942539dd75bf4e549c2f5bfcf8454b4927a3`.
Replay both files with the byte-target command above. Together with the
earlier qualified positive profiles, these two new inputs reach all 70 parser
lines, five functions and 110 regions. The profiles use the same production
module, byte target and compiler; earlier inputs were not replayed. Branch
instrumentation, an arbitrary-byte campaign and other native platforms remain
separate obligations. Fixed-input replay is not a fuzzing campaign.
