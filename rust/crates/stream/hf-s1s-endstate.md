# Stream end state on main `362947de8`
1. Keep main's domain, provider contract, wire format, transport clients and journal recovery.
2. Split main's large files by concept; source files <=800 lines, tests <=1200.
3. Splits are moves. Add no forwarding modules, alternate providers or compatibility APIs.
4. `Stream<P>` owns `Arc<P>` directly; remove the redundant `StreamClient` layer.
5. Preserve public native signatures that the SDK's current callers use.
6. `OwnedBytes` shares immutable backing and preserves ownership through empty slices.
7. `PreparedOwnedBytes` funds writable storage and its owner before physical input.
8. Native results retain original I/O causes separately from `StreamError`.
9. `NativeStreamOutput<T>` couples actual output storage with its allocation guard.
10. `NativeProviderFuture` funds its concrete heap frame before boxing.
11. `PreparedNativeCommit` transfers one prebuilt completion and one recovery future.
12. One private ingress cell accepts the completion body; dropping it releases reservations.
13. Keep allocation and I/O credits in the operation context and existing frame objects.
14. Extend main's memory commit staging; share validation, digest, histories and publication.
15. Preserve guarded callbacks outside provider locks and recheck after contention or drift.
16. Prepared memory completion reserves resident capacity before the external action.
17. Settlement fills funded slots; no new allocation or admission follows packet issuance.
18. Recovery checks family, original request identity and the funded result shape.
19. Extend main's local visibility, journal and group-flush paths; add no second engine.
20. Detached accepted work retains input, guard and root ownership through physical completion.
21. An accepted short journal write poisons the provider; ordinary writes retain retry semantics.
22. Journal encoding borrows domain data; validation and encoding share size calculations.
23. Preserve behavioral tests for limits, cancellation, replay, recovery and ownership.
24. Delete duplicated transport/conformance cases and tests of removed internal layouts or counts.

| Files / responsibility | Added-line budget against main |
|---|---:|
| `lib/`, pure backend and transport splits | 80 |
| `owned_bytes/`, native byte ownership and preparation | 300 |
| `native/`, funded results, futures and ingress | 200 |
| `memory/`, native context and shared prepared settlement | 300 |
| `local/`, native journal completion and original-owner drain | 350 |
| `wire_codec/requests.rs`, borrowed journal encoding | 180 |
| Surviving native behavioral tests and one shared meter | 600 |
| Remove main's redundant client, benchmarks, essays and duplicate tests | -1,133 |
| **Net ceiling** | **877** |
