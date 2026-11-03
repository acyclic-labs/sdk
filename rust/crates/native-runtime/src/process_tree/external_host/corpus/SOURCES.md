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
