# Original terminal source lifetime

Darwin `terminal::Readiness` retains its source container, including borrowed
lifetimes or an existing `Arc` owner. Its wait accepts only an enable mask.
A descriptor cannot be substituted between waits, even if its number is reused.
The source trait is evaluated once during construction. Source implementations
keep their returned descriptors open until the retained owner is dropped.

The census is `rg -n 'Readiness::|Readiness<' rust/crates -g '*.rs'`.
Both terminal consumers use the same primitive. Input moves its existing
`Arc<Input>` into readiness; it adds no reference increment, allocation or copy.
The select write fallback retains one additional `Arc` reference once, with one
increment at registration and one decrement at retirement. Kqueue adds neither.
No descriptor duplication, round trip, durability barrier or native call is added.

Each select wait still makes one native call. The mask removes the original
N descriptor comparisons, leaving N enable operations and N bit clears, with
no allocation, source getter, scan over descriptor-set words or history growth.
Storage adds the source owner; the input wait's existing owner moves into it.
An original readable pipe's writer drop produces EOF, and retiring an owned
writable wait closes its last writer and produces EOF for the reader.

Run on macOS from the repository root. The compiler-negative example is the
public API itself. Removing source retention makes the expired borrow compile,
so rustdoc must fail with `Test compiled successfully, but it's marked
compile_fail`. The mutation changes the shipped implementation, leaving the
original negative example unchanged. It does not run an invalid descriptor.

```sh
cargo test --locked -p acyclic-native-runtime --doc terminal::Readiness

git apply rust/proofs/terminal-source-ownership/omit-source-ownership.patch
# Require failure: Test compiled successfully, but it's marked compile_fail.
cargo test --locked -p acyclic-native-runtime --doc terminal::Readiness
git apply -R rust/proofs/terminal-source-ownership/omit-source-ownership.patch
# Require a pass after restoring source ownership.
cargo test --locked -p acyclic-native-runtime --doc terminal::Readiness

cargo nextest run --locked -p acyclic-native-runtime --lib -E 'test(=terminal::tests::waits_at_descriptors_beyond_fd_setsize)'
cargo nextest run --locked -p acyclic-harness-tui --features tui --lib -E 'test(=term::tty::tests::real_pty_waits_only_until_writable) | test(=term::tty::tests::high_descriptors_wait_only_until_writable)'
```

The native property covers repeated high-descriptor waits, suspension masks,
original pipe input, producer-drop EOF and owner retirement. Its existing
ordinary-select negative requires EINVAL at the same high descriptor. The
consumer properties use real PTYs and force the same select fallback. Failure
to provision the original high descriptor is an explicit error, never a skip.

The descriptor-capacity census is
`rg -n 'RLIMIT_NOFILE|skipped:' rust/crates/harness-tui/src/term`.
Both missing-capacity paths refuse: an insufficient hard limit returns
`Unsupported`, and a failed `setrlimit` preserves its OS error kind. Neither
path reports a passing readiness property. Successful checks perform the same
resource-limit read, conditional limit update and PTY operations as before;
there is no additional syscall, wait, allocation or copy on that path.

On Linux, a real child-process hard limit exercises the refusal without
changing the parent shell's capacity. `original-descriptor-skips.patch`
restores the exact original test file from
`96804b393d7457c820bf6581dd8a1f7bb84f300d`. Its false pass is the negative
control; it must print `skipped: RLIMIT_NOFILE` without exercising the PTY.

```sh
git apply rust/proofs/terminal-source-ownership/original-descriptor-skips.patch
# Original bug: exits successfully and prints skipped: RLIMIT_NOFILE.
prlimit --nofile=1500:1500 cargo nextest run --locked -p acyclic-harness-tui --features tui --lib --success-output final -E 'test(=term::tty::tests::high_descriptors_wait_only_until_writable)'
git apply -R rust/proofs/terminal-source-ownership/original-descriptor-skips.patch
# Require test failure with Unsupported and the descriptor-capacity recovery command.
prlimit --nofile=1500:1500 cargo nextest run --locked -p acyclic-harness-tui --features tui --lib -E 'test(=term::tty::tests::high_descriptors_wait_only_until_writable)'
# Require a pass with the original offered capacity restored.
cargo nextest run --locked -p acyclic-harness-tui --features tui --lib -E 'test(=term::tty::tests::high_descriptors_wait_only_until_writable)'
```

This control exercises the hard-limit refusal. The security-policy-dependent
`setrlimit` failure is propagated through the same test result; it is not
claimed as a separately exercised native failure.
