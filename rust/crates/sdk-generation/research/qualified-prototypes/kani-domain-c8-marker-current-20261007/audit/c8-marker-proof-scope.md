# c8 current-main marker proof audit

Canonical source: `Q:\sdk\work\sdkgen-actors-c8-minimal`.

The proof snapshot is commit `84a508449ea247297a0170db67377dd65a1baa41` with the Actors domain and Kani proof files clean at capture. Other work remains dirty in the shared worktree; the receipt binds the proof closure hashes explicitly.

The current producer now has `subscription_start::CurrentHeadMarker`, a unit type with `TryFrom<bool>` accepting exactly `true`, `From<CurrentHeadMarker> for bool` returning `true`, and `SubscriptionStart::current_head()` as a no-argument constructor. The wire oneof remains `CurrentHead(bool)` with `#[proto(tag = 2, bool)]`, so descriptor payload compatibility is preserved while the semantic marker makes false unrepresentable after validation.

Three direct production Kani obligations passed with Kani 0.68.0, CBMC 6.11.0, nightly-2026-08-21, offline mode, one build job, and unwind 2:

* `subscription_start_ingress_accepts_only_true_current_head_payload`: for every symbolic wire bool, actual `SubscriptionStart::try_from` accepts only true and returns `InvalidSubscription` for false; successful values retain the current-head presence and no cursor.
* `current_head_marker_is_true_only_and_lowers_to_true_wire_payload`: actual `CurrentHeadMarker::try_from` rejects false, its inverse lowers only to true, and the production no-argument constructor round-trips to the true wire oneof.
* `subscription_start_valid_wire_round_trip_preserves_oneof_identity`: every symbolic u64 cursor and the true current-head oneof round-trip through actual production conversion without changing identity or cursor value.

These harnesses call the maintained production functions and generated conversion impls directly. No copied validator or alternate projection algorithm is present. The proof does not claim arbitrary strings, transport behavior, whole-message clone correctness, or the pending long expected-revision obligation.

The source-bound long expected-revision proof remains the separate original PTY 17660 / CBMC PID 1031718 process and was not restarted or modified. The small c8 runs used a separate task-local target and independent harness selectors.
