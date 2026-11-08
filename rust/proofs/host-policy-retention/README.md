# Installed native-policy retention

The real host control uses `LocalHarnessConfig::native_host` and the public
`LocalWorkspace::executor`. It installs six granted executors, uses each twice
through the ordinary shell or hook path, and also executes unconfined controls.
It checks read-only and outside-root denial and wrong-workspace refusal.
Unsupported enforcement fails this control; it cannot qualify zero native work.

On Linux with Landlock, capture only policy syscalls from that control:

```sh
strace -f -qq -e trace=landlock_create_ruleset,landlock_add_rule,landlock_restrict_self \
  -o retained.trace cargo test --locked -p acyclic-harness \
  --lib --features filesystem-local,native-execution \
  local::checkout_tools::publisher::tests::granted::host_executor_grants_reach_shells_and_hooks_without_erasing_refusals \
  -- --exact --nocapture
```

Separate ABI queries (`landlock_create_ruleset(NULL, 0, ...)`) from successful
ruleset creations. Six installed grants require six rulesets. The eight valid
granted child launches must each call `landlock_restrict_self`; refusing a cwd
outside the original workspace must not enter a child. The unconfined cases
must not install Landlock restrictions.

Apply `original-constructor.patch` to restore the complete production constructor
while keeping the same control, then repeat into `original.trace`. It must still
preserve confinement, but create twelve rulesets instead of six. Reverse the
patch before qualifying the retained source. This is the cost negative control;
it restores real production compilation rather than a simulated counter.

The shared native grant controls
`kernel_boundary_confines_descendants_and_preserves_host_authority` and
`native_preparation_preserves_original_operation_admission_and_late_failures`
retain the independent admission, root-replacement, narrower-policy and
operation-credit checks. Installation uses the existing offloaded completion:
its sole worker owns the grant until completion, and producer loss is an I/O
error. No new wait, cancellation protocol or policy compiler is introduced.
Warm launch still binds and verifies the original roots and cwd. Its retained
policy selection adds one Arc clone, with no policy mutation or cache lookup;
root verification remains proportional to configured authority.
