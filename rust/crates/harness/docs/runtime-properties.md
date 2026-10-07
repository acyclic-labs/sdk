# Generic runtime contracts and verification map

The task, workflow, execution and conversation journals remain authoritative.
These are implementation invariants and regression targets, not unrestricted
correctness proofs. No machine-checked proof is supplied by this table.

| Invariant | Production mechanism | Assumptions | Verification and evidence |
| --- | --- | --- | --- |
| Cancellation never removes an owned reservation without its exact fence. | `Scheduler::apply`: cancellation records intent; owned completion and lease release require `LeaseFence`. | Trusted host acknowledges stopped ownership; Stream compare-and-append is linearizable. | `cancellation_retains_owned_capacity_until_fenced_acknowledgement` covers root/direct-child and partial/full admission; existing running-release test covers the stopped-worker path. |
| An indeterminate execution retains its reservation through cancellation and restart; it cannot be pulled again. | `Completed(Indeterminate)` enters `Reconciling` with its reservation; readiness excludes reconciliation; completion still requires the reservation fence. | Trusted provider reconciles effects rather than repeating uncertain dispatch. | `uncertain_execution_retains_capacity_and_rejects_stale_owners`; `active_worker_reservations_are_subtracted_from_capacity` reopens the Stream coordinator after uncertainty. Memory-provider test evidence, not crash/filesystem qualification. |
| A waiting parent has durable identity and no execution reservation. | Existing fenced `WaitingForChildren` transition. | Executor suspends before publishing this transition. | `waiting_parent_releases_execution_capacity`; waiting-parent cancellation regression. |
| A suspended workflow releases its slot, retains one authenticated resume input, and cannot be advanced by its previous lease. Bounded yields and uncertain commands retain their slot. | The existing scheduler adds `WorkflowSuspended`, `WorkflowResumed` and same-fence `ReconciliationResumed`; `run_task` uses the existing registry, host and checkpoint consumption marker. | Linearizable Stream, immutable task-readable input files, deterministic machines, and a trusted command adapter reporting quiescence and using existing provider journals. | `workflow_suspension_releases_shared_capacity_and_fences_wakes` covers shared capacity, wake revisions, cancellation and legacy state decoding. `bounded_worker_suspends_and_consumes_wake_after_full_reopen` uses real local stores. `worker_reconciles_journaled_model_without_releasing_ownership` checks a bounded yield, interrupted stock dispatch, retained same-fence ownership and one generate/one reconcile across reopen. Remaining adapters, acknowledgement faults and cross-platform qualification remain open. |
| Stock model/tool workflow commands use the admitted task's existing authorities and reconcile uncertain dispatch without fresh execution. | `FilesystemTaskCommands` uses the mandatory stock executor or `DurableToolRunner`, the same exact tool registry revision, task-derived execution identity and bound journal. The composed content reader retains `TaskContext` read-grant checks. | Linearizable Stream, immutable payloads, trusted provider reconciliation, and stable explicit model binding across recovery. Lower-level custom command callbacks remain trusted. | `worker_reconciles_journaled_model_without_releasing_ownership` and `worker_reconciles_pinned_tool_without_reexecuting_after_reopen` reopen real local stores, retain the same reservation, and observe one dispatch/one reconciliation. The tool case rejects a missing revision before dispatch and reads actual retained files in both provider paths. `registered_task_reopens_checkpoint_under_replacement_lease` also rejects a task file read allowed only to the broader storage owner. Model/tool coverage does not qualify the remaining command kinds or all provider fault windows. |
| Configured sessions never exceed retained-task, depth, owned-reservation or distinct-model-attempt ceilings (task, turn and step). Exact request retries consume no additional units. | Immutable `SessionConfigured` and fenced `ModelDispatchClaimed` events in the existing scheduler journal; child claims count against their root. A task-bound `StockExecutor` charges before its `ModelStarted` CAS; the host derives ceilings from retained admission. | Stream compare-and-append is linearizable. Charges represent admitted attempts: a crash between accounting and `ModelStarted` retains the charge. Mandatory generic composition remains incomplete. | `session_ceilings_are_shared_and_claims_survive_recovery`, competing-coordinator race and `local_session_budget_survives_provider_reopen`; `model_claims_use_pinned_ceiling_and_current_owner` verifies host ceilings, retries and cancellation. The interrupted executor regression checks one claim across reconciliation and rejects a stale binding; these are separate component tests, not a complete persistent executor qualification. |
| New child admissions cannot widen their immediate parent's pinned grants, content limits or run limits. Invalid child batches publish no manifest. | `CoordinatorTaskHost` loads the parent admission and uses the existing `RuntimeScope::narrow` and `with_run_limits` checks before declaration or batch publication. | Immutable owner-attested parent admissions. | The coordinator batch/reopen regression now attempts wider grants, model ceilings and removed step/concurrency limits through the host directly; a narrower child succeeds. |
| A retained model-charge receipt cannot bypass cancellation observed when the coordinator completes its append. | Initial intent hits, committed receipts and tail-conflict intent resolution all revalidate the current reservation fence and cancellation state. | The refresh observes durable cancellation. This check does not atomically fence later execution-journal writes or provider dispatch. | The competing-coordinator budget regression passes actual committed and conflict outcomes through the completion boundary after cancellation; it runs with MemoryStream and reopened LocalStream. The charge remains retained. |
| Workflow open consumes at most 64 transition bodies per page; retained host state contains no historic transition bodies. | Bounded `WorkflowJournal::replay`, compact revision/digest retry indexes, one checkpoint, and shared chain/identity validation. Existing 4,096-record and 65,536-identity bounds now apply to both journal providers. | Immutable records and collision-resistant canonical digests; provider payload-size limits bound each body. | `workflow_pages_preserve_old_retries_and_reject_cross_page_corruption` and the Filesystem/Stream workflow qualification cover multiple pages, cold reopen and exact old retries. Structural bounds, not a measured process-memory benchmark. |
| Task-bound workflow publications cannot follow durable cancellation or lease replacement. | `CoordinatorTaskHost::journal_owner` and `FilesystemWorkflowJournal::for_task` bind the admitted machine/input and exact lease. A Stream coordinated commit compares the authenticated coordinator tail and workflow tail together. | Stream conditions and mutations are linearizable on the same provider; machines initialize and transition deterministically. Trusted provider wiring uses the task-bound constructor. | The task-host regression prepares the actual commit request, then cancels before publication and observes an atomic conflict with no journal advance; it also checks lease replacement, forged state and exact retries. The local variant reopens Stream only; Filesystem payloads in this guard test remain in memory. Generic command dispatch and persistent composition still require this binding. |
| Task-bound execution publications require the retained lease, and model starts require their exact retained shared-budget claim. | `FilesystemExecutionJournal::for_task` uses the coordinator provider; coordinated Stream commits compare both tails. A bounded compact index validates starts, settlements and exact retries; replay checks historical model claims without charging again. | Linearizable Stream transactions, immutable claims and trusted wiring with unique task-owned execution operations. Payload staging can leave unused files. Provider dispatch is outside this publication transaction. | The host regression covers missing or mismatched claims, stale retry rejection, cancellation settlements, cold replay across 64-record pages and uncharged-history rejection. Prepared execution commits conflict after lease replacement and cancellation. The local variant persists Stream while payloads remain in memory; full persistent executor qualification is still required. |
| Execution replay reads at most 64 observations per page and the stock loop retains only the current model step or tool call. | Paged `ExecutionJournal::replay`; shared scanner validates gapless sequences and compact retry digests across pages. Model selections use the configured event ceiling; tool selections are capped at three records. Uncertain dispatch reconciliation reads its exact slot. | Immutable journals and collision-resistant retry digests. Provider limits bound payload bodies; the existing model event ceiling bounds the retained current-step prefix. | `execution_pages_select_bounded_records_and_reject_cross_page_corruption`, the interrupted stock-executor regression spanning two pages, and the Filesystem/Stream dispatch qualification. Structural bounds; repeated scans and process memory still need performance qualification. |
| Reattachment cannot substitute task implementation, schemas, input, authority or execution route. | Existing `TaskAdmissionRecord`, registered machine identity and state/spawner attestation. | Trusted provider observations attest committed records. | Existing runtime admission/reattachment and coordinator tests; persistent ordinary-task execution remains a required gate. |
| Registered task recovery uses the admitted checkpoint; exact step retries neither advance it twice nor bypass cancellation. | `AgentHarness::open_task`, `TaskDefinition::open` and `ResumableTaskSession` use the existing workflow host and require an exact task/lease-bound journal. Fresh steps check uncancelled ownership, including cached retries; completed values must satisfy the pinned task output schema. Latest status/outbox recovery reads one verified record. | Deterministic pinned machines, immutable provider records, linearizable task-bound publication, trusted provider binding attestations. A recovered workflow result still requires scheduler settlement. | `registered_task_reopens_checkpoint_under_replacement_lease` uses real LocalStream and local Filesystem stores, drops all runtime/provider handles, replaces the crashed lease and resumes its checkpoint. It covers invalid output, conflicting/exact retries, unbound journal rejection and read-only completed recovery after cancellation. This is task workflow qualification, not command dispatch or a complete worker loop. |
| A configured host never exposes a newly admitted root without its immutable shared ceilings. | `CoordinatorTaskHost::with_session_limits` uses `DistributedCoordinator::declare_session` to append `Declared` and `SessionConfigured` in one tail-CAS batch. Reattachment checks the configured root ceilings; retries verify the complete logical event pair, adjacent revisions and common committed envelope. | Stream batch append is atomic and linearizable; trusted persistent wiring selects the configured host. Existing unconfigured hosts remain available. | `root_declaration_and_shared_limits_are_atomic` races incompatible and identical requests with MemoryStream; `atomic_root_limits_survive_local_stream_reopen` repeats the race and reopens LocalStream. `root_limits_reconcile_a_lost_atomic_append_acknowledgement` forces a committed MemoryStream append to lose its acknowledgement, with both available and temporarily unavailable receipt inspection. `session_retry_rejects_separately_committed_lookalike_events` rejects matching events from separate envelopes. The real Filesystem/Stream task restart test uses the configured host and rejects changed ceilings. Persistent worker composition is still required. |

| Persistent stock construction cannot omit task accounting or substitute another journal, and model inputs cannot borrow the storage owner's broader read grants. | `FilesystemTaskRuntime` composes one configured host and the existing registries/providers; `FilesystemTaskExecution` owns its stock binding and task-namespaced execution journal. `TaskJournalOwner` validates input files against retained task grants and limits. | Trusted caller-owned persistent providers and linearizable Stream transactions; collision-resistant task/turn namespace. Uncomposed deadline, policy and execution-route runners are rejected. | The real local registered-task restart test interrupts the model, drops runtime/provider handles, replaces the lease and reconciles without redispatch or another charge. A second turn hits the retained shared ceiling; wrong turn IDs, widened step counts and an actual owner-readable/task-ungranted file are rejected. Publication acknowledgement-fault and generic worker qualification remain open. |

## Remaining implementation gates

The bounded generic worker now opens the same registry entry without typed
input/output parameters. `WorkflowSuspended` releases its exact reservation;
authenticated `WorkflowResumed` retains one verified wake file and revision
across restart. Checkpoint advancement consumes that input without another
ledger. `ReconciliationResumed` preserves the existing uncertain reservation.
The command adapter is trusted to use existing journaled dispatch and report
quiescence honestly; this is not a guarantee for arbitrary callbacks. The local
worker regressions cover suspension/wake, stale leases, and interrupted stock
model reconciliation. Built-in adapters and full provider-fault qualification
remain open.

The optional `FilesystemTaskRuntime` now composes the existing configured host,
typed runtime and task-bound journals. Its stock execution wrapper accepts no
alternate journal or accounting binding and namespaces turn identities by task.
`registered_task_reopens_checkpoint_under_replacement_lease` drops real local
Filesystem/Stream stores and runtime handles after an interrupted model stream,
then replaces the lease, recovers both journals, reconciles once without another
dispatch, and rejects a second turn at the retained shared ceiling. It also
denies a real owner-readable input file whose read grant was removed from the
task admission. Task-bound journal input validation uses retained grants and
limits, rather than the storage owner's broader capabilities. These are runtime
restart and model interruption results, not publication acknowledgement-fault
or generic worker qualification.

- Qualify the task-bound persistent stock path under publication/provider
  faults. The optional composition now binds its two journals together; it
  charges fresh attempts and checks ownership on resume; reconciliation retains
  the same claim without refunding uncertainty. The task-bound Filesystem
  execution journal now compares coordinator and journal tails atomically;
  further stock execution/provider fault qualification remains required.
- Complete built-in command adapters using existing provider journals and
  scheduler fences. The generic loop now exposes bounded transitions and durable
  suspension, but callback dispatch/quiescence must be qualified for each provider.
  Do not introduce a second scheduler, lifecycle or registry.
- Qualify active ownership, durable suspension and cancellation under provider
  faults; retain no passive worker futures or whole execution histories.
- Incremental bounded cold replay and lazy rebuildable listing projections.
  Coordinator Stream replay is paged, but its current projection retains all
  operations and intent events. Workflow and execution replay now read bounded
  pages; repeated execution scans still require cost qualification, and the
  coordinator needs lazy projections.
- Complete persistent worker composition, including admitted deadlines and
  pinned policy/execution routes; extend real local restart tests with accounting
  races, stale-owner and cancellation faults; check generated-contract parity for
  changed public APIs and perform the final diff audit.

Executor seams: use `WorkflowJournal` for checkpoint/command atomicity,
`ExecutionJournal::append_if_tail` for model dispatch claims, and the existing
coordinator journal for shared admission. Model-prefix representation and native
approval/provider effects belong to their separate owners; dependent code must
wait for merged main changes.

Reproduce the current focused evidence with:

```text
cargo test -p acyclic-harness --lib -j 2 --target-dir <isolated-build-directory>
cargo clippy -p acyclic-harness --lib --tests -j 2 --target-dir <isolated-build-directory> -- -D warnings
```

Record actual results outside source. Missing or skipped gates remain incomplete.
