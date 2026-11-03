# Production mutation controls

Console guard ownership has one shared control:
`term::console_owner::tests::metadata_and_io_follow_the_original_guard_handoff`.
It observes the original owner from the callback submitted by `Owner::run`,
while the production worker owns its guard, then checks the acknowledged result.
It also checks the original generation before and after that worker's write.
No guard state is manually removed and no negative overwrites an observation.
Apply each retained mutation separately, run the control below and require its
exact assertion; reverse the patch before the next control.

| Mutation | Required assertion |
| --- | --- |
| `console-profile-without-guard.patch` | `borrowed profile must be unknown` |
| `console-counter-without-guard.patch` | `borrowed guard must be unknown` |
| `console-empty-without-guard.patch` | `borrowed guard acknowledged an empty frame` |
| `console-io-holds-owner.patch` | `console I/O retained owner state` |

```sh
RUSTC_WORKSPACE_WRAPPER="$PWD/scripts/loom-rustc.sh" cargo test -p acyclic-harness-tui --features 'tui model-checking' --lib term::console_owner::tests::metadata_and_io_follow_the_original_guard_handoff -- --exact
```

The device is a supplied writer. These production ownership checks do not
establish physical console input, cancellation or OS completion guarantees.
