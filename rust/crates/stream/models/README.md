# Session model routing

These on-demand models use the production synchronization primitives. From the
repository root, the explicit compiler routing is:

```sh
RUSTC_WORKSPACE_WRAPPER=./scripts/loom-rustc.sh cargo test --locked -p acyclic-stream-models --features model-checking --test session_model
```

An ordinary Rust compile without that routing deliberately refuses this model target. Compiling
the target does not execute or qualify its schedules or negative controls.

The model target requires the existing `model-checking` feature. Ordinary
workspace checks leave it unselected; explicit model checks still require the
Loom compiler routing above. Publication models follow the same rule:

```sh
RUSTC_WORKSPACE_WRAPPER=./scripts/loom-rustc.sh cargo test --locked -p acyclic-stream-publication-models --features model-checking --test publication
```

The workspace consolidation exposed stale consumers of removed production APIs.
The model mapping follows the current main contract, also inspected by the
Stream owner at `b5899587789ad2152b29beaab1ee85e843a25645`:

| Retained history | Current production seam and oracle |
| --- | --- |
| Gate registration, omitted notification, wake under lock | Included `session/gate.rs`; `poll_changed(false)` observes the completed true setter. |
| Independent delivery with a saturated source; global-gate negative | `session/sources/model/admission.rs` constructs the actual `Channel`, calls `source_mode`, and traverses `Sources::next_with_control` and its queues using `policy::room`. It checks the healthy record and preserves the blocked provider's queued record. |
| Disable a parked source; keep-polling negative | The same production source queues stop consumption after credit removal. Re-admission must deliver the original queued record. |
| Generated reserved-slot capacities | The existing frame/record-size inputs exercise actual source admission for uncredited latest, credited ordinary, and globally full cases. |
| Cursor movement and stale generation | Included `client/progress.rs`, with the real generated `acyclic_stream::wire::SessionCursor`. |
| Atomic anchored resume cut; split-observation and initial-anchor negatives | Both `consume_record` and `consume_progress` race `fence` and `subscription`. Sequence and preceding immutable commit must move together; both stale consumption methods must refuse, then the new generation must advance the same cut. This single race subsumes the former duplicate `checked::moving`; the generated checked-history fold remains distinct. |
| Conflated latest movement | `tests/client_pool_loom.rs` runs the production `session::verify_latest_progress` model and its generation-split negative control. The standalone target does not repeat that same pair. |
| Cancellation of a queued Open | `tests/client_pool_loom.rs` races the actual cursor drop with `Pool::command` and the original removal event. It checks route retirement, pending membership, the cancellation fence and the last interest count. The former standalone `installed` boolean did not execute the dispatcher or receiver closure. |
| Initial EOF retry yield | `client/pool/retry_model.rs` calls the real `Pool::reconnect`, polls its dial, then applies completion after cancellation. Its test clock holds the original executor delay; its typed `Welcome` is internal model input, not captured transport evidence. |

The former child-only opaque-consumption model called the removed
`consume_opaque`. The product retired child subscriptions: `session.proto`
reserves the former `child` and `fs_publication` fields 11 and 12, and record
follows refuse a nonempty opaque resume. The replacement anchored-cut model
retains generic atomic movement and stale-cut controls; it does not claim to
qualify historical child resume. Historical source and receipts remain unchanged
in Git and retain their original scope.

Actor-call opaque bytes still describe immutable response length. Actor-call
fragment, reassembly, refusal, and cancellation assertions remain at their
existing owners; this mapping introduces no resumable actor-call cursor. Existing
source wake/recheck models remain distinct from these admission histories.

The record and progress split-observation controls are independent test functions,
so either expected panic cannot prevent the other control from reaching its pair
assertion.

The source-admission fixtures use `binding = None`. They establish that fixture's
credit/consumption contract; authority-bound non-call follows retain their actual
bounded lookahead and are not qualified by this fixture as never polling after
credit removal.

The retry clock's bypass control alone does not establish that the oracle detects
removing the production delay. The kept source mutation exercises that boundary:

```sh
git apply rust/crates/stream/models/mutations/initial_retry_delay.patch
RUSTC_WORKSPACE_WRAPPER=./scripts/loom-rustc.sh cargo test --locked -p acyclic-stream --no-default-features --features grpc,model-checking --test main client_pool_loom::initial_eof_retry_yields_to_deadline_and_cancellation -- --exact --nocapture
git apply --reverse rust/crates/stream/models/mutations/initial_retry_delay.patch
```

The mutated source must fail at `initial retry must yield before another physical
RPC`; restore it before running the same command as the positive control. This
checks scheduling at the executor entrance, not physical RPC timing, wire refusal
metadata, membership changes or transport retirement.

The ordinary `sources::tests::every_local_and_global_credit_combination_matches_the_admission_contract`
matrix also checks follow and finite-reply bindings admitted by the production
authority methods, plus a held progress watermark. It traverses the actual source
queues for every boolean credit/terminal/progress state and pending queue lengths
around the lookahead boundary. The existing signed test credential is SDK-generated;
the typed source events are internal scheduler inputs, not recorded external wire.
This component check does not qualify credential rotation or transport lifecycle.

`mutations/source_progress_fence.patch` removes the production progress fence.
The unchanged matrix command must then fail at `source admission differs`:

```sh
cargo test --locked -p acyclic-stream --no-default-features --features session --lib session::sources::tests::every_local_and_global_credit_combination_matches_the_admission_contract -- --exact --nocapture
```

Restore the patch before using that command as the positive control. The former
matrix always supplied no binding and no progress; it could not detect this fence
omission or a mistaken credit-free lookahead for finite replies.

The local replay model calls the production `require_accepted` durability handoff,
`flush_through` and publication wait. Its `REQUIRE = false` control selects a
model branch; it does not itself demonstrate detection of a production defect.
`mutations/replay_durability_requirement.patch` removes the production promotion
of the original appended frame into the required durability cut. Apply it and run
the unchanged positive oracle:

```sh
git apply rust/crates/stream/models/mutations/replay_durability_requirement.patch
RUSTC_WORKSPACE_WRAPPER=./scripts/loom-rustc.sh cargo test --locked -p acyclic-stream --no-default-features --features local,model-checking --test main local_replay_model::replay_and_flush_preserve_durability -- --exact --nocapture
git apply --reverse rust/crates/stream/models/mutations/replay_durability_requirement.patch
```

The mutant must fail at `replay omitted the original durability requirement`;
the same command on restored production source must pass. This model exercises
the unreserved handoff with no native work budget. Its volatile journal fixture
and inline blocking adapter do not qualify native reservation, root ownership,
physical worker cancellation or OS durability failures. The public reopen and
emitted-history recovery properties remain separate and are kept.

The latest-consumption model calls the production `Progress::consume_latest`,
`fence` and `next` on the same cursor lock. Its existing broken branch writes the
cursor directly; `mutations/progress_generation_fence.patch` instead removes the
shipping generation check without changing the model or its positive oracle:

```sh
git apply rust/crates/stream/models/mutations/progress_generation_fence.patch
RUSTC_WORKSPACE_WRAPPER=./scripts/loom-rustc.sh cargo test --locked -p acyclic-stream --no-default-features --features grpc,model-checking --test main client_pool_loom::latest_consumption_and_move_fence_share_one_application_cut -- --exact --nocapture
git apply --reverse rust/crates/stream/models/mutations/progress_generation_fence.patch
```

The mutant must fail at `a stale delivery cannot advance the fenced resume cut`;
restored source must pass the same command. This model races one conflated
consumption against one generation fence. It does not qualify bootstrap or
checked-record admission, cursor overflow, transport loss, physical restarts or
multiple placement participants. The sequential generation property and distinct
strict-record and overflow refusal controls remain separate.
