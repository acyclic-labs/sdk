`native_patch_hooks` feeds operator-authored V4A patch bytes through the shipped
OpenAI registry, canonical argument adapter, publication parser and hook request
normalization. It checks the ordered affected paths against the actual parsed
publication, refuses malformed documents, and checks guard membership across
every affected path.
The real-process control
`hooks::request::tests::atomic_edit_guards_cover_every_path_and_rewritten_move_destination`
also executes plugin programs through the public hook runner, captures their
actual stdin, and checks blocking before and after rewrites. It covers additions,
updates, deletions and both ends of moves, with an unguarded public-path control.

The corpus consists of generated operator inputs, not captured model or external
tool output: `atomic-edit.patch` covers the four operation forms;
`move-source.patch` guards the deleted end of a move; `public.patch` keeps an
apparent header inside added content; `unfinished.patch` exercises whole-document
refusal. No provider response is synthesized.

From the repository root, replay with instrumentation through cargo-fuzz, or
run the retained inputs with:

```sh
cargo run --locked -p acyclic-harness-fuzz --no-default-features --features native-arguments --bin native_patch_hooks -- rust/crates/harness-fuzz/corpus/native_patch_hooks -runs=0 -seed=1
```

`single-path-normalization.patch` restores the original production normalization
function unchanged. Apply it with `git apply` in an isolated checkout. Both the
real-process control and corpus must fail: the original function classifies an
atomic edit as `other`, so its paths never reach file guards. Reverse the patch
before positive qualification. Require nonzero instrumented coverage separately;
an uninstrumented replay is not a coverage measurement.

`matcher-error-escapes.patch` restores the former before-hook error boundary.
The same real-process control must fail with
`malformed rewritten patch aborted the hook chain`. It covers an incomplete
document and a wrongly typed native argument, each with the rewriting hook
before and after the file guard. Both hooks use the required blocking plugin
failure policy. Invalid input blocks with the original reason, and no guard
process starts for the rejected rewrite. Reverse this patch before qualification.

```sh
cargo nextest run --locked -p acyclic-harness-core --lib --features test-support,filesystem,hooks -E 'test(hooks::request::tests::atomic_edit_guards_cover_every_path_and_rewritten_move_destination)'
```
