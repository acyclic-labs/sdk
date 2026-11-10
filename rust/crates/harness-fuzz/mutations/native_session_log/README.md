# Native resume recovery validation

`native_session_log` calls the production `logged_session_id` used by
`HostWorkspaceOwner::complete_session`. Its validation-only export does not
replace the decoder or the settlement path.

Reuse the original corpus at
`rust/crates/harness/src/local/host_process/fixtures` and its README provenance.
The Claude acceptance seed is the unchanged completed response from Claude Code
2.1.278, selected from the existing installed-host capture. Escaped member and
session-type spellings are equivalent JSON representations derived in memory;
they are not new native captures. The oracle compares selection across those
representations and separately requires the original captured UUID, so a
reject-all implementation cannot pass.

From the repository root:

```sh
cargo run --locked -p acyclic-harness-fuzz --no-default-features --features runtime --bin native_session_log -- rust/crates/harness/src/local/host_process/fixtures -runs=0 -seed=1
cargo test --locked -p acyclic-harness --features filesystem-local,native-execution,test-support --lib local::host_owner::tests::claude_native_session_lifecycle -- --exact
```

`raw-json-prefilter.patch` restores the original production defect. With that
patch, both unchanged oracles must fail at the resume-ID equality assertion;
compilation errors and setup failures do not count. Reverse the patch before
running the positive controls. The lifecycle control settles the original
captured log through the workspace owner, restarts that owner, reads the next
claimed turn and checks the retained session ID after merge and retirement.

Corpus replay is not an instrumented coverage campaign. Source branch coverage,
finite mutation coverage and installed native-process qualification remain
separate obligations. The other host lifecycle examples' authored event logs do
not qualify their external wire formats.
