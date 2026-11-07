# Original Windows pipe ownership

Run from the repository root on Windows. These controls use the shipped
`ProcessTree` readiness and capture paths with real child processes and pipes.

`stopped_capture_preserves_output_prefix_and_cleanup_observation` waits on the
original diagnostic pipe, retains both completion-operation identities, then
captures and terminates the child. Capture must retain those identities, the
original timeout, both output prefixes and the observed physical cleanup.
The same fixed two slots survive callback unwinding; capture has no second
reader registry or restoration step.

`a_child_inherits_only_its_standard_handles` compares a real inheritable event
in an ordinary child and a controlled child. Its existing report must survive
libtest's unterminated test label. The probe frames its first report before
entering any input or console wait, so every streaming report reader observes
that original producer event. No parser, readiness probe or timeout is added.

Readiness followed by capture previously tried to create replacement operations
for both present pipes. A refused IOCP association stops that attempt before its
notification-mode call and allocation; a successful replacement repeats all
three costs. Capture now adds zero of each, zero locks, round trips or durability
barriers and zero buffer copies. First use still associates each present pipe once. Setup
examines exactly two slots; original native completion, process exit and
deadline events drive waits. Work remains independent of process history and
tree size.

`ProcessPipeReader::take_stdout` also transfers the original operation with its
pipe. The raw-handle streaming constructor is removed. The existing real-child
protocol property first arms readiness while the real child waits for input,
then transfers stdout and requires the original observation to be cancelled
before a new read. It also arms and transfers stderr, requiring its prior
observation to be cancelled. It retains a strong reference to the original operation
while checking its identity after transfer. It completes a
quiet read, a request, a response and EOF through the same production reader.
An already observed handoff adds zero associations, notification-mode calls,
allocations or copies. Transfer takes at most one original registry lock to cancel a
prior observation, with no native cancellation syscall: its pending overlapped
operation follows the pipe. An unobserved handoff initializes only stdout.
Both inspect one fixed slot; no second registry, search or readiness retry is added.

Capture's identity check also retains its original strong references, so an
allocator cannot make a replacement appear identical by reusing an address.

One pending pipe operation admits one observation. A competing observation
returns `AlreadyExists` before changing its token or native operation. The
original registration retains that operation and withdraws only its exact token
on drop, including immediate readiness and refused admission. A retired wait
cannot clear a successor's claim. The same real-child protocol property checks
both refusals before releasing the child's original input event; no clock or
readiness probe is involved.

Admission replaces one atomic token store with one compare/exchange and one
strong-reference increment. Drop adds one compare/exchange and one reference
decrement. These fixed operations buy exclusive observation, rollback and
source lifetime. They add zero allocations, copies, native calls, round trips
or durability barriers, with no index, scan, retry or historical growth.

The original Windows execution failed four capture/cleanup assertions and
five console properties, including two blocked report waits. The controls
below restore each planted bug on the corrected source. Require each named
failure, restore even when validation fails, then require the corrected test
to pass. They do not run the blocking console cases with framing removed.

```powershell
$controls = @(
    @('replace-readers.patch', 'process_tree::tests::stopped_capture_preserves_output_prefix_and_cleanup_observation', 'capture replaced the original pipe completion owners'),
    @('replace-transferred-reader.patch', 'process_tree::stdio::tests::quiet_protocol_read_leaves_the_coordination_worker_available', 'stdout handoff replaced the original completion owner'),
    @('omit-transfer-retirement.patch', 'process_tree::stdio::tests::quiet_protocol_read_leaves_the_coordination_worker_available', 'stdout handoff left its previous observation pending'),
    @('replace-observation-token.patch', 'process_tree::stdio::tests::quiet_protocol_read_leaves_the_coordination_worker_available', 'competing pipe observation was admitted'),
    @('withdraw-successor-token.patch', 'process_tree::stdio::tests::quiet_protocol_read_leaves_the_coordination_worker_available', "old observation removed its successor's pipe claim"),
    @('omit-report-boundary.patch', 'process_tree::windows_create::tests::a_child_inherits_only_its_standard_handles', 'console probe record framing lost')
)
foreach ($control in $controls) {
    $patch = 'rust/proofs/native-pipe-ownership/' + $control[0]
    git apply --check $patch
    if ($LASTEXITCODE -ne 0) { throw "STEP_FAILED mutation check; recovery: git apply --check $patch" }
    git apply $patch
    if ($LASTEXITCODE -ne 0) { throw "STEP_FAILED mutation apply; recovery: git apply $patch" }
    try {
        $result = cargo test --locked -p acyclic-native-runtime --all-features --lib $control[1] -- --exact --nocapture 2>&1
        $failed = $LASTEXITCODE -ne 0
        $result
        if (!$failed -or !($result -match [regex]::Escape($control[2]))) {
            throw "STEP_FAILED negative control $($control[0]); recovery: inspect the original test output and restore $patch"
        }
    } finally {
        git apply -R $patch
        if ($LASTEXITCODE -ne 0) { throw "STEP_FAILED mutation restore; recovery: git apply -R $patch" }
    }
    cargo test --locked -p acyclic-native-runtime --all-features --lib $control[1] -- --exact --nocapture
    if ($LASTEXITCODE -ne 0) { throw "STEP_FAILED restored property; recovery: inspect this exact named test before a changed-source run" }
}
```

After restoration, run the affected real-child capture and console properties:

```text
cargo nextest run --locked -p acyclic-native-runtime --all-features --lib -E 'test(=process_tree::platform::denied_job_cleanup_retains_child_and_reports_failure) | test(=process_tree::tests::capture_timeout_and_output_overflow_reap_owned_tree) | test(=process_tree::tests::stopped_capture_preserves_output_prefix_and_cleanup_observation) | test(=process_tree::tests::streaming_input_overflow_parser_failure_and_deadline_preserve_cleanup) | test(process_tree::windows_create::tests::)'
```

The console selection includes the real parent/child/grandchild owner-exit,
blocked-reader, sibling-isolation and window checks. Existing controls retain
the live-owner comparison, inherited-event comparison and standard-library
creation oracle. Shared producer-loss properties separately check that loss
settles original waits with the retained native cause.
