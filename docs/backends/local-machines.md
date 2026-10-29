# Local Machines execution

Enable `acyclic-machines/local`. Bind `local::LocalMachine::new(machine_id,
LocalOptions { cwd, environment, inherit_environment, process_custody })` from
the host's existing admitted workspace. Pass the same public `ExecCommand` to
`exec(command, handler)`. The native `CaptureHandler` retains the original
source checks, publication callbacks and workspace custody. It runs on the
shared native IO workers. No shell is inserted around argv.

Pull `execution` for binary stdout/stderr and the original terminal
exit or stopped capture. After its terminal result, `execution.finish().await`
returns the original handler. Admission refusal returns a stream error and
preserves that handler too. Interrupted physical ownership returns
`CommandIndeterminate` with the original Machine identity. Dropping output
closes native credit and wakes cleanup even for a silent child.

The relative command deadline starts at admission and survives submission and
spawn. An optional authenticated absolute expiry uses the shared wall alarm.
One direct child and the existing process containment/capture pump perform
execution. Stdin and output buffers move without a payload copy; output has one
queued batch, not a second reader or collector. Later consumers that concatenate
frames still pay for that concatenation.

This is the execution primitive for a local Machine owner, not a complete
durable lifecycle provider or a filesystem authorization boundary. Processes
retain the host user's OS privileges. The host must retain its existing path
policy, selected-volume admission, callback custody and publication logic.
Checkpoint/fork and lifecycle composition remain separate requirements.

Run `cargo test -p acyclic-machines --features conformance,local --lib` and
`cargo test -p acyclic-native-runtime --lib`. Real-process controls cover binary
stdin, channel bytes, cwd, exact exit, original handler/buffer identity, expiry
before spawn, producer loss and elapsed admission deadlines. They do not
establish complete local lifecycle conformance or hosted volume enrollment.
