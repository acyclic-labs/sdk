# SDK limit classification

Baseline: `2146782983`, 2026-10-28. Locations refer to that baseline so deleted limits remain traceable.

Categories: **1** arbitrary product cap; **2** chunk/page; **3** backpressure/credit; **4** protection or representation invariant.

Actions marked pending require implementation or a semantic follow-up within this milestone. A retained page is valid only when the remainder remains available.

| Constant | File:line | Category | Action |
|---|---|---|---|
| `MAX_MESSAGE_BYTES` | `rust/crates/actors/src/client.rs:47` | 1 | DELETE refusal and cap-specific tests; page/stream growing state. Pending. |
| `MAX_SUBSCRIPTIONS` | `rust/crates/actors/src/contract.rs:6` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_BINDINGS` | `rust/crates/actors/src/contract.rs:8` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_SECRETS` | `rust/crates/actors/src/contract.rs:10` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_GUEST_METADATA_BYTES` | `rust/crates/actors/src/guest/runtime.rs:5` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_BEARER_TOKEN_BYTES` | `rust/crates/actors/src/lib.rs:51` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_TIMER_RECORD_BYTES` | `rust/crates/actors/src/timers.rs:11` | 4 | Keep fixed-schema encoding guard; canonical values have a structural size bound below it. |
| `MAX_CREDENTIAL_BYTES` | `rust/crates/auth/src/credential.rs:11` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_UNIX_SECONDS` | `rust/crates/auth/src/validation.rs:65` | 4 | Keep exact wire/OS/integer representation invariant; no smaller product ceiling. |
| `MAX_CHILD_OUTPUT_BYTES` | `rust/crates/conformance/src/bin/native_mount_qualify.rs:69` | 4 | Audit hostile-input/record protection; whole-value limits must stream rather than refuse. Pending. |
| `MAX_BATCH_CONCURRENCY` | `rust/crates/filesystem-wasm/src/opfs/mod.rs:41` | 3 | Keep batching/window; verify continuation never refuses total work. |
| `MAX_EVENT_BYTES` | `rust/crates/filesystem/src/accounting/mod.rs:23` | 4 | Audit hostile-input/record protection; whole-value limits must stream rather than refuse. Pending. |
| `MAX_AUTHORITY_GROUP_MEMBERS` | `rust/crates/filesystem/src/async_storage/authority.rs:3` | 2 | Keep batching/window; verify continuation never refuses total work. |
| `MAX_CA_CERTIFICATE_BYTES` | `rust/crates/filesystem/src/hosted.rs:30` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_BYTE_RESPONSE_ENVELOPE_BYTES` | `rust/crates/filesystem/src/hosted.rs:35` | 4 | Keep fixed-schema encoding guard; canonical values have a structural size bound below it. |
| `MAX_NATIVE_EXACT_CAPTURE_PATHS` | `rust/crates/filesystem/src/native_capture/model.rs:4` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_UNITS` | `rust/crates/filesystem/src/native_host/root_path.rs:35` | 4 | Keep exact wire/OS/integer representation invariant; no smaller product ceiling. |
| `MAX_NATIVE_SUBTREE_CAPTURE_PATHS` | `rust/crates/filesystem/src/native_mount/adapter/mod.rs:42` | 1 | DELETE refusal and cap-specific tests; page/stream growing state. Pending. |
| `MAX_SERIAL` | `rust/crates/filesystem/src/native_mount/fuse/shared.rs:21` | 4 | Keep exact wire/OS/integer representation invariant; no smaller product ceiling. |
| `MAX_ENCODED_BYTES` | `rust/crates/filesystem/src/operation_window/lease_path/encoding.rs:3` | 4 | Keep exact wire/OS/integer representation invariant; no smaller product ceiling. |
| `MAX_COMMAND_BYTES` | `rust/crates/harness-client/src/command.rs:13` | 4 | Audit hostile-input/record protection; whole-value limits must stream rather than refuse. Pending. |
| `MAX_CURSOR_BYTES` | `rust/crates/harness-client/src/command.rs:14` | 4 | Audit hostile-input/record protection; whole-value limits must stream rather than refuse. Pending. |
| `MAX_AGENT_REPLY_BYTES` | `rust/crates/harness-client/src/reply.rs:14` | 4 | Audit hostile-input/record protection; whole-value limits must stream rather than refuse. Pending. |
| `MAX_CHECKPOINT_BYTES` | `rust/crates/harness-core/src/agent/checkpoint.rs:27` | 1 | Deleted the checkpoint ceiling; checkpoint stores membership roots/counts. Remaining whole-value transport work is tracked below. |
| `MAX_FANOUT` | `rust/crates/harness-core/src/agent/kernel.rs:11` | 2 | Keep batching/window; verify continuation never refuses total work. |
| `MAX_DEPTH` | `rust/crates/harness-core/src/agent/layout.rs:32` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_QUEUED` | `rust/crates/harness-core/src/agent/machine.rs:23` | 3 | Keep turn lookahead credit. Additional input remains in the durable inbox and resumes after drain. |
| `MAX_QUEUED_BYTES` | `rust/crates/harness-core/src/agent/machine.rs:26` | 3 | Keep turn lookahead credit. Additional input remains in the durable inbox and resumes after drain. |
| `MAX_OUTCOME_BYTES` | `rust/crates/harness-core/src/agent/machine.rs:30` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_RECORD_BYTES` | `rust/crates/harness-core/src/agent/records.rs:16` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_INTENT_BYTES` | `rust/crates/harness-core/src/agent/records.rs:18` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_MESSAGE_BYTES` | `rust/crates/harness-core/src/agent/records.rs:20` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_VOLUMES` | `rust/crates/harness-core/src/agent/records.rs:22` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_OPEN_CHILDREN` | `rust/crates/harness-core/src/agent/slots.rs:4` | 1 | Deleted the fallback and reservation arithmetic; immutable Patricia membership roots give O(1) checkpoint clones and cursor pages. |
| `MAX_PRIVATE_DIRECTORY_PAGE` | `rust/crates/harness-core/src/conversation/directory.rs:43` | 2 | Keep batching/window; verify continuation never refuses total work. |
| `MAX_PATH_BYTES` | `rust/crates/harness-core/src/conversation/mod.rs:20` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_LABEL_BYTES` | `rust/crates/harness-core/src/conversation/mod.rs:23` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_EXACT_JS_INTEGER` | `rust/crates/harness-core/src/conversation/mod.rs:25` | 4 | Keep exact wire/OS/integer representation invariant; no smaller product ceiling. |
| `MAX_EXACT_JS_FLOAT` | `rust/crates/harness-core/src/conversation/mod.rs:27` | 4 | Keep exact wire/OS/integer representation invariant; no smaller product ceiling. |
| `MAX_PORTABLE_COUNT` | `rust/crates/harness-core/src/conversation/mod.rs:53` | 4 | Keep exact wire/OS/integer representation invariant; no smaller product ceiling. |
| `MAX_CONVERSATION_PAGE_MESSAGES` | `rust/crates/harness-core/src/conversation/mod.rs:56` | 2 | Keep batching/window; verify continuation never refuses total work. |
| `MAX_FORK_AGENTS` | `rust/crates/harness-core/src/fork/mod.rs:16` | 4 | Keep exact wire/OS/integer representation invariant; no smaller product ceiling. |
| `MAX_FORK_RESOURCES` | `rust/crates/harness-core/src/fork/mod.rs:18` | 4 | Keep exact wire/OS/integer representation invariant; no smaller product ceiling. |
| `MAX_FORK_REFERENCES` | `rust/crates/harness-core/src/fork/mod.rs:20` | 4 | Keep exact wire/OS/integer representation invariant; no smaller product ceiling. |
| `MAX_FORK_ATTACHMENT_MANIFEST_BYTES` | `rust/crates/harness-core/src/fork/mod.rs:22` | 4 | Keep exact wire/OS/integer representation invariant; no smaller product ceiling. |
| `MAX_FORK_INHERITED_BYTES` | `rust/crates/harness-core/src/fork/mod.rs:24` | 4 | Keep exact wire/OS/integer representation invariant; no smaller product ceiling. |
| `MAX_FORK_REFERENCE_BYTES` | `rust/crates/harness-core/src/fork/mod.rs:26` | 4 | Keep exact wire/OS/integer representation invariant; no smaller product ceiling. |
| `MAX_FORK_INHERITED_MESSAGES` | `rust/crates/harness-core/src/fork/mod.rs:28` | 4 | Keep exact wire/OS/integer representation invariant; no smaller product ceiling. |
| `MAX_SECRET_NAME_BYTES` | `rust/crates/harness-core/src/secrets.rs:12` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_PROJECTION_BYTES` | `rust/crates/harness-core/src/store/checkpoints/mod.rs:20` | 1 | DELETE refusal and cap-specific tests; page/stream growing state. Pending. |
| `MAX_NODE_BYTES` | `rust/crates/harness-core/src/store/effect_heads.rs:17` | 4 | Keep fixed-schema encoding guard; canonical values have a structural size bound below it. |
| `MAX_NODE_BYTES` | `rust/crates/harness-core/src/store/effect_heads/tests/old_effect_heads.rs:19` | 4 | Keep fixed-schema encoding guard; canonical values have a structural size bound below it. |
| `MAX_CERTIFICATE_BYTES` | `rust/crates/harness-core/src/store/effect_heads/tests/old_effect_heads.rs:20` | 4 | Keep finite verification domain; remove cap-specific proofs/tests when production cap is deleted. |
| `MAX_CHILDREN` | `rust/crates/harness-core/tests/formal/agent_control_model.rs:8` | 4 | Keep finite verification domain; remove cap-specific proofs/tests when production cap is deleted. |
| `MAX_AUTH_BYTES` | `rust/crates/harness-providers/src/codex.rs:20` | 4 | Audit hostile-input/record protection; whole-value limits must stream rather than refuse. Pending. |
| `MAX_ASYNC_WORKERS` | `rust/crates/harness/tests/e2e/root.rs:287` | 4 | Keep finite verification domain; remove cap-specific proofs/tests when production cap is deleted. |
| `MAX_LIST_PAGES` | `rust/crates/machines-daytona/src/api.rs:27` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_FORK_BACKOFF` | `rust/crates/machines-daytona/src/lib.rs:1604` | 2 | Keep batching/window; verify continuation never refuses total work. |
| `MAX_EVENTS_PER_MACHINE` | `rust/crates/machines-daytona/src/ops.rs:29` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_SAFE_INTEGER` | `rust/crates/machines-wasm/src/lib.rs:16` | 4 | Keep exact wire/OS/integer representation invariant; no smaller product ceiling. |
| `MAX_CONSECUTIVE_UNKNOWN_INSPECTIONS` | `rust/crates/machines/src/grpc/mod.rs:16` | 4 | Audit hostile-input/record protection; whole-value limits must stream rather than refuse. Pending. |
| `MAX_MESSAGE_BYTES` | `rust/crates/machines/src/grpc/mod.rs:17` | 1 | DELETE refusal and cap-specific tests; page/stream growing state. Pending. |
| `MAX_PAGE_SIZE` | `rust/crates/machines/src/lib.rs:45` | 2 | Keep batching/window; verify continuation never refuses total work. |
| `MAX_BEARER_TOKEN_BYTES` | `rust/crates/machines/src/lib.rs:47` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_OUTPUT_READ_BYTES` | `rust/crates/machines/src/lib.rs:49` | 2 | Keep batching/window; verify continuation never refuses total work. |
| `MAX_FORK_CHILDREN` | `rust/crates/machines/src/lib.rs:51` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_EVENT_PAGE_SIZE` | `rust/crates/machines/src/lib.rs:53` | 2 | Keep batching/window; verify continuation never refuses total work. |
| `MAX_JSON_BYTES` | `rust/crates/manifest/src/lib.rs:17` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_ENVIRONMENTS` | `rust/crates/manifest/src/lib.rs:19` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_ITEMS` | `rust/crates/manifest/src/lib.rs:21` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_STRING_BYTES` | `rust/crates/manifest/src/lib.rs:23` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_NODES` | `rust/crates/manifest/src/lib.rs:25` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_TRANSFER` | `rust/crates/native-runtime/src/apple.rs:33` | 2 | Keep batching/window; verify continuation never refuses total work. |
| `MAX_SECONDS` | `rust/crates/native-runtime/src/apple_terminal.rs:16` | 4 | Audit hostile-input/record protection; whole-value limits must stream rather than refuse. Pending. |
| `MAX_IO_BYTES` | `rust/crates/native-runtime/src/linux/mod.rs:26` | 2 | Keep batching/window; verify continuation never refuses total work. |
| `MAX_TRANSFER_TIMEOUT` | `rust/crates/objects/src/v1/grpc.rs:19` | 2 | Keep batching/window; verify continuation never refuses total work. |
| `MAX_PEM_BYTES` | `rust/crates/objects/src/v1/mod.rs:102` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_BEARER_TOKEN_BYTES` | `rust/crates/objects/src/v1/mod.rs:104` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_METADATA` | `rust/crates/objects/src/v1/s3.rs:18` | 4 | Audit hostile-input/record protection; whole-value limits must stream rather than refuse. Pending. |
| `MAX_BEARER_TOKEN_BYTES` | `rust/crates/stream/src/lib.rs:106` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_CA_CERTIFICATE_BYTES` | `rust/crates/stream/src/lib.rs:109` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_RECORD_BYTES` | `rust/crates/stream/src/lib.rs:136` | 4 | Audit hostile-input/record protection; whole-value limits must stream rather than refuse. Pending. |
| `MAX_ITEMS` | `rust/crates/stream/src/lib.rs:138` | 4 | Audit hostile-input/record protection; whole-value limits must stream rather than refuse. Pending. |
| `MAX_COMMAND_BYTES` | `rust/crates/stream/src/lib.rs:142` | 4 | Audit hostile-input/record protection; whole-value limits must stream rather than refuse. Pending. |
| `MAX_IDEMPOTENCY_KEY_BYTES` | `rust/crates/stream/src/lib.rs:146` | 4 | Audit hostile-input/record protection; whole-value limits must stream rather than refuse. Pending. |
| `MAX_PATH_BYTES` | `rust/crates/stream/src/lib.rs:148` | 4 | Audit hostile-input/record protection; whole-value limits must stream rather than refuse. Pending. |
| `MAX_RECORD_KEYS` | `rust/crates/stream/src/record_headers.rs:14` | 1 | DELETE refusal and cap-specific tests; page/stream growing state. Pending. |
| `MAX_RECORD_KEY_BYTES` | `rust/crates/stream/src/record_headers.rs:16` | 1 | DELETE refusal and cap-specific tests; page/stream growing state. Pending. |
| `MAX_RECORD_ATTRIBUTES` | `rust/crates/stream/src/record_headers.rs:18` | 1 | DELETE refusal and cap-specific tests; page/stream growing state. Pending. |
| `MAX_ACTOR_CALL_REPLY_BYTES` | `rust/crates/stream/src/wire_codec/session.rs:10` | 4 | Audit hostile-input/record protection; whole-value limits must stream rather than refuse. Pending. |
| `MAX_PAYLOAD` | `scripts/buildbox/buildbox.mjs:322` | 2 | Keep batching/window; verify continuation never refuses total work. |
| `MAX_RECEIPT_BYTES` | `scripts/plugin-qualification.mjs:8` | 4 | Audit hostile-input/record protection; whole-value limits must stream rather than refuse. Pending. |
| `MAX_RECEIPT_BYTES` | `scripts/typescript-qualification.mjs:9` | 4 | Audit hostile-input/record protection; whole-value limits must stream rather than refuse. Pending. |
| `MAX_ARCHIVE_BYTES` | `scripts/validate-npm-package.mjs:7` | 4 | Audit hostile-input/record protection; whole-value limits must stream rather than refuse. Pending. |
| `MAX_EXPANDED_BYTES` | `scripts/validate-npm-package.mjs:8` | 4 | Audit hostile-input/record protection; whole-value limits must stream rather than refuse. Pending. |
| `MAX_BEARER_TOKEN_BYTES` | `typescript/packages/filesystem/src/hosted.ts:20` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_BEARER_TOKEN_BYTES` | `typescript/packages/inference/src/index.ts:301` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_OUTPUT_READ_BYTES` | `typescript/packages/machines/src/grpc.ts:8` | 2 | Keep batching/window; verify continuation never refuses total work. |
| `MAX_OFFSET` | `typescript/packages/machines/src/grpc.ts:9` | 4 | Keep exact wire/OS/integer representation invariant; no smaller product ceiling. |
| `MAX_BEARER_TOKEN_BYTES` | `typescript/packages/objects/src/v1.ts:21` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |
| `MAX_BEARER_TOKEN_BYTES` | `typescript/packages/stream/src/http.ts:202` | 1 | Deleted the default ceiling, its refusal checks and cap-only tests. Explicit caller budgets and structural validation remain. |

## Additional constant inventory

| Constant | File:line | Category | Action |
|---|---|---|---|
| `MAXIMUM_MANIFEST_BYTES` | `rust/crates/filesystem-napi/src/bindings/transfer/mod.rs:55` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_MANIFEST_OBJECTS` | `rust/crates/filesystem-napi/src/bindings/transfer/mod.rs:56` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_MANIFEST_BYTES` | `rust/crates/filesystem-wasm/src/bindings/transfer/mod.rs:75` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_MANIFEST_OBJECTS` | `rust/crates/filesystem-wasm/src/bindings/transfer/mod.rs:76` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_NFA_STATES` | `rust/crates/filesystem/src/access/compilation.rs:3` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_STATES` | `rust/crates/filesystem/src/access/compilation.rs:4` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_TRANSITIONS` | `rust/crates/filesystem/src/access/compilation.rs:5` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_STATE_MEMBERS` | `rust/crates/filesystem/src/access/compilation.rs:6` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_TEXT_BYTES` | `rust/crates/filesystem/src/access/compilation.rs:7` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_WORK` | `rust/crates/filesystem/src/access/compilation.rs:8` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_POLICY_RECORD_BYTES` | `rust/crates/filesystem/src/access/mod.rs:74` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_FIRST_RECORD_BYTES` | `rust/crates/filesystem/src/async_storage/fork.rs:50` | 4 | Verify bound derives from the fixed canonical schema, platform ABI or integer representation; arbitrary whole-value cases must stream. Audit pending. |
| `MAXIMUM_BLOCKED_REMOTE_READS` | `rust/crates/filesystem/src/demand/native/file.rs:24` | 3 | Keep bounded concurrency/residency or retry delay only where work waits, spills or cache misses reload; refusal branches require removal. Audit pending. |
| `MAXIMUM_NATIVE_REQUESTS` | `rust/crates/filesystem/src/demand/native/mod.rs:21` | 3 | Keep bounded concurrency/residency or retry delay only where work waits, spills or cache misses reload; refusal branches require removal. Audit pending. |
| `STREAM_RECORD_LIMIT` | `rust/crates/filesystem/src/distributed/mod.rs:26` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_VOLUME_EVENT_BYTES` | `rust/crates/filesystem/src/facade/generation.rs:3` | 4 | Verify bound derives from the fixed canonical schema, platform ABI or integer representation; arbitrary whole-value cases must stream. Audit pending. |
| `MAXIMUM_PATCH_FILE_BYTES` | `rust/crates/filesystem/src/git_compat/mod.rs:25` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `BROKEN` | `rust/crates/filesystem/src/kernel/allocation/accounting/charge_model.rs:12` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `BROKEN` | `rust/crates/filesystem/src/kernel/codec/accounting/model_checking.rs:136` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `BROKEN` | `rust/crates/filesystem/src/kernel/file_read/accounting/model_checking.rs:40` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_INLINE_FILE_BYTES` | `rust/crates/filesystem/src/kernel/file_table.rs:36` | 2 | Keep a chunk/page/inline threshold; verify larger work continues through further chunks, pages or external bodies. |
| `MAXIMUM_PARENTS` | `rust/crates/filesystem/src/kernel/generation.rs:13` | 4 | Verify bound derives from the fixed canonical schema, platform ABI or integer representation; arbitrary whole-value cases must stream. Audit pending. |
| `MAXIMUM_GENERATION_ROOT_BYTES` | `rust/crates/filesystem/src/kernel/generation.rs:15` | 4 | Verify bound derives from the fixed canonical schema, platform ABI or integer representation; arbitrary whole-value cases must stream. Audit pending. |
| `MAXIMUM_LABEL_BYTES` | `rust/crates/filesystem/src/kernel/retention.rs:16` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_BUILTIN_BYTES` | `rust/crates/filesystem/src/materializer/work.rs:410` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `DEFAULT_MAX_OBJECT_BYTES` | `rust/crates/filesystem/src/memory/mod.rs:27` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_RECEIPT_BYTES` | `rust/crates/filesystem/src/mount_qualification.rs:14` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_EXACT_INSTALL_CHANGES` | `rust/crates/filesystem/src/native_mount/adapter/mod.rs:41` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_LOOKUP_CACHE_ENTRIES` | `rust/crates/filesystem/src/native_mount/darwin_mount/mod.rs:28` | 3 | Keep bounded concurrency/residency or retry delay only where work waits, spills or cache misses reload; refusal branches require removal. Audit pending. |
| `MAXIMUM_LISTING_RESTARTS` | `rust/crates/filesystem/src/native_mount/darwin_mount/mod.rs:29` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_VERIFIED_LABELS` | `rust/crates/filesystem/src/native_mount/darwin_mount/mod.rs:30` | 3 | Keep bounded concurrency/residency or retry delay only where work waits, spills or cache misses reload; refusal branches require removal. Audit pending. |
| `MAXIMUM_CALLBACK_BYTES` | `rust/crates/filesystem/src/native_mount/darwin_mount/mod.rs:31` | 4 | Verify bound derives from the fixed canonical schema, platform ABI or integer representation; arbitrary whole-value cases must stream. Audit pending. |
| `MAXIMUM_NEGATIVE_ENTRIES_PER_DIRECTORY` | `rust/crates/filesystem/src/native_mount/fuse/mod.rs:43` | 3 | Keep bounded concurrency/residency or retry delay only where work waits, spills or cache misses reload; refusal branches require removal. Audit pending. |
| `MAXIMUM_DIRECTORY_STREAMS` | `rust/crates/filesystem/src/native_mount/fuse/mod.rs:52` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_WRITE` | `rust/crates/filesystem/src/native_mount/fuse/packet_session.rs:16` | 2 | Keep a chunk/page/inline threshold; verify larger work continues through further chunks, pages or external bodies. |
| `MAXIMUM_LAZY_DIRECTORY_CURSORS` | `rust/crates/filesystem/src/native_mount/lazy/mod.rs:32` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_PROMOTION_RETRIES` | `rust/crates/filesystem/src/native_mount/lazy/mod.rs:36` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_INTERRUPTED_LISTINGS` | `rust/crates/filesystem/src/native_mount/lazy/mod.rs:38` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_NATIVE_ATTRIBUTE_LIST_BYTES` | `rust/crates/filesystem/src/native_mount/mod.rs:169` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_CACHED_BINDINGS` | `rust/crates/filesystem/src/native_mount/projfs/mod.rs:84` | 3 | Keep bounded concurrency/residency or retry delay only where work waits, spills or cache misses reload; refusal branches require removal. Audit pending. |
| `HELD_SETTLE_LIMIT` | `rust/crates/filesystem/src/native_mount/projfs/projector_state.rs:158` | 3 | Keep bounded concurrency/residency or retry delay only where work waits, spills or cache misses reload; refusal branches require removal. Audit pending. |
| `MAXIMUM_SAFE_REBASE_ATTEMPTS` | `rust/crates/filesystem/src/native_mount/publication.rs:11` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_TRACKED_KEYS` | `rust/crates/filesystem/src/native_mount/view_ledger/mod.rs:34` | 3 | Keep bounded concurrency/residency or retry delay only where work waits, spills or cache misses reload; refusal branches require removal. Audit pending. |
| `MAXIMUM_ENTRIES` | `rust/crates/filesystem/src/path_index/mod.rs:14` | 3 | Keep bounded concurrency/residency or retry delay only where work waits, spills or cache misses reload; refusal branches require removal. Audit pending. |
| `MAXIMUM_RETAINED_BYTES` | `rust/crates/filesystem/src/path_index/mod.rs:15` | 3 | Keep bounded concurrency/residency or retry delay only where work waits, spills or cache misses reload; refusal branches require removal. Audit pending. |
| `MAXIMUM_VALUE_BYTES` | `rust/crates/filesystem/src/path_index/mod.rs:16` | 3 | Keep bounded concurrency/residency or retry delay only where work waits, spills or cache misses reload; refusal branches require removal. Audit pending. |
| `MAXIMUM_CAS_ATTEMPTS` | `rust/crates/filesystem/src/record_store.rs:7` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_BUFFER_BYTES` | `rust/crates/filesystem/src/source_watch/directory_changes/mod.rs:50` | 2 | Keep a chunk/page/inline threshold; verify larger work continues through further chunks, pages or external bodies. |
| `MAXIMUM_DRAIN_BYTES` | `rust/crates/filesystem/src/staged_objects/mod.rs:25` | 2 | Keep a chunk/page/inline threshold; verify larger work continues through further chunks, pages or external bodies. |
| `MAXIMUM_RESIDENT_BYTES` | `rust/crates/filesystem/src/staged_objects/mod.rs:26` | 3 | Keep bounded concurrency/residency or retry delay only where work waits, spills or cache misses reload; refusal branches require removal. Audit pending. |
| `MAXIMUM_SPILL_BYTES` | `rust/crates/filesystem/src/staged_objects/mod.rs:27` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_DRAINED_IDENTITIES` | `rust/crates/filesystem/src/staged_objects/mod.rs:28` | 2 | Keep a chunk/page/inline threshold; verify larger work continues through further chunks, pages or external bodies. |
| `DEFAULT_HOSTED_MAXIMUM_RESPONSE_BYTES` | `rust/crates/filesystem/src/wire_service.rs:28` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `DEFAULT_HOSTED_MAXIMUM_PAGE_ITEMS` | `rust/crates/filesystem/src/wire_service.rs:30` | 2 | Keep a chunk/page/inline threshold; verify larger work continues through further chunks, pages or external bodies. |
| `MAXIMUM_EVENT_BYTES` | `rust/crates/filesystem/src/wire_service/references.rs:45` | 4 | Verify bound derives from the fixed canonical schema, platform ABI or integer representation; arbitrary whole-value cases must stream. Audit pending. |
| `MAXIMUM_WORKSPACE_NAME_BYTES` | `rust/crates/filesystem/src/workspace/mod.rs:30` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_CONTEXT_WIRE_BYTES` | `rust/crates/filesystem/src/workspace_context_wire.rs:15` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_RECEIPT_BYTES` | `rust/crates/filesystem/tests/mount_qualification.rs:5` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `COMPONENT_LABEL_MAX_BYTES` | `rust/crates/harness-core/src/contract.rs:8` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_PREFIX_BYTES` | `rust/crates/harness-core/src/core/reducer/admission.rs:351` | 4 | Verify bound derives from the fixed canonical schema, platform ABI or integer representation; arbitrary whole-value cases must stream. Audit pending. |
| `RECORD_MAXIMUM_DEPTH` | `rust/crates/harness-core/src/executor/json/mod.rs:21` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_METADATA` | `rust/crates/harness-core/src/filesystem/host/ranges.rs:48` | 1 | Deleted default refusal. Capture uses continuation/cancellation; replies use existing credited fragments. Explicit caller limits remain optional. |
| `RETAINED_LIMIT` | `rust/crates/harness-providers/src/fuzzing/tool_arguments.rs:14` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_RESPONSE_BYTES` | `rust/crates/harness-search/src/http.rs:10` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_URL_BYTES` | `rust/crates/harness-search/src/url_fetch.rs:21` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_RESPONSE_BYTES` | `rust/crates/harness-search/src/url_fetch/http.rs:16` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_REDIRECTS` | `rust/crates/harness-search/src/url_fetch/http.rs:17` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `PROBE_LIMIT` | `rust/crates/harness-tui/src/term/probe.rs:12` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_BYTES` | `rust/crates/harness/examples/explicit-image-conversion.rs:26` | 4 | Finite fixture/example-selected input or explicit demonstration budget; no product admission ceiling. |
| `MAXIMUM_RESULT_BYTES` | `rust/crates/harness/examples/file-tools-contract.rs:25` | 4 | Finite fixture/example-selected input or explicit demonstration budget; no product admission ceiling. |
| `MAXIMUM_TEXT` | `rust/crates/harness/src/local/agent_conversation.rs:20` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_PATHS` | `rust/crates/harness/src/local/checkout_tools/capture.rs:24` | 1 | Deleted default refusal. Capture uses continuation/cancellation; replies use existing credited fragments. Explicit caller limits remain optional. |
| `MAXIMUM_RESCANS` | `rust/crates/harness/src/local/checkout_tools/capture/converge.rs:9` | 1 | Deleted default refusal. Capture uses continuation/cancellation; replies use existing credited fragments. Explicit caller limits remain optional. |
| `MAXIMUM_BYTES` | `rust/crates/harness/src/local/host_files.rs:27` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_BYTES` | `rust/crates/harness/src/local/host_owner/agents.rs:33` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_MESSAGE` | `rust/crates/harness/src/local/host_owner/agents.rs:35` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_PROMPT` | `rust/crates/harness/src/local/host_owner/sessions.rs:13` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `EVENT_LIMIT` | `rust/crates/harness/src/local/host_process/children.rs:23` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `ANSWER_LIMIT` | `rust/crates/harness/src/local/host_process/children.rs:27` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_METADATA_BYTES` | `rust/crates/harness/src/local/owner/exclusion.rs:42` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_PAGE_BYTES` | `rust/crates/harness/src/wasm/page_window.rs:11` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_FILE_BYTES` | `rust/crates/harness/tests/e2e/resume.rs:39` | 4 | Finite fixture/example-selected input or explicit demonstration budget; no product admission ceiling. |
| `MAXIMUM_MESSAGE_BYTES` | `rust/crates/inference/src/contract/mod.rs:6` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_HTTP_JSON_BYTES` | `rust/crates/inference/src/contract/mod.rs:9` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_EVALUATION_CANDIDATES` | `rust/crates/inference/src/contract/mod.rs:11` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_EVALUATION_CASES` | `rust/crates/inference/src/contract/mod.rs:13` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_EVALUATION_METRICS` | `rust/crates/inference/src/contract/mod.rs:15` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_EVALUATION_RESULTS` | `rust/crates/inference/src/contract/mod.rs:17` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_CA_CERTIFICATE_BYTES` | `rust/crates/inference/src/host/mod.rs:15` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `LIST_PAGE_LIMIT` | `rust/crates/machines-daytona/src/api.rs:25` | 2 | Keep a chunk/page/inline threshold; verify larger work continues through further chunks, pages or external bodies. |
| `IOV_MAX_FALLBACK` | `rust/crates/native-runtime/src/apple.rs:31` | 2 | Keep a chunk/page/inline threshold; verify larger work continues through further chunks, pages or external bodies. |
| `RECORD_LIMIT` | `rust/crates/native-runtime/src/claim/record.rs:10` | 4 | Verify bound derives from the fixed canonical schema, platform ABI or integer representation; arbitrary whole-value cases must stream. Audit pending. |
| `MAXIMUM_FAILURE_BYTES` | `rust/crates/native-runtime/src/service/mod.rs:26` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |
| `MAXIMUM_INLINE_BODY_BYTES` | `rust/crates/objects/src/physical/mod.rs:17` | 2 | Keep a chunk/page/inline threshold; verify larger work continues through further chunks, pages or external bodies. |
| `MAXIMUM_SEGMENT_BODIES` | `rust/crates/objects/src/physical/mod.rs:21` | 2 | Keep a chunk/page/inline threshold; verify larger work continues through further chunks, pages or external bodies. |
| `MAXIMUM_SEGMENT_BYTES` | `rust/crates/objects/src/physical/mod.rs:22` | 2 | Keep a chunk/page/inline threshold; verify larger work continues through further chunks, pages or external bodies. |
| `PACK_LIMIT` | `rust/crates/objects/src/v1/memory/durable/mod.rs:13` | 2 | Keep a chunk/page/inline threshold; verify larger work continues through further chunks, pages or external bodies. |
| `RECORD_LIMIT` | `rust/crates/objects/src/v1/memory/durable/mod.rs:14` | 4 | Verify bound derives from the fixed canonical schema, platform ABI or integer representation; arbitrary whole-value cases must stream. Audit pending. |
| `DEFAULT_VOLUME_LIMITS` | `typescript/packages/filesystem/src/contracts.ts:296` | 1 | Remove default refusal and cap-only tests; preserve explicit caller policy and implement continuation where required. Pending. |

## Non-constant limits

| Knob/mechanism | File:line | Category | Action |
|---|---|---|---|
| `Preset::graphcoder().max_children` | `rust/crates/harness-core/src/preset.rs:52` | 1 | Set `None`; retained the explicit downstream execution-policy knob. |
| Preset `max_steps` | `rust/crates/harness-core/src/preset.rs:32` | 1 (opt-in) | Keep optional user execution budget; no default refusal. |
| Conformance `max_children` | `rust/crates/conformance/agent-host/src/protocol/embedded.rs:112`; `cloudflare/worker.ts:23` | 1 | Removed the default eight-child cap. |
| Stream memory/local/browser/journal capacity | `rust/crates/stream/src/memory/memory_limits.rs:3` and provider options | 1 | Removed finite defaults; explicit caller capacity selections remain optional. |
| Stream session `channels` | `rust/crates/stream/src/session/mod.rs:73` | 1 | Removed active-follow count refusal; pending frame/byte credits remain backpressure. |
| Objects memory/local capacity | `rust/crates/objects/src/v1/memory/mod.rs:61`; `src/local_options.rs:14` | 1 | Removed finite defaults; user resource reservations remain opt-in. |
| Filesystem `VolumeLimits` | `rust/crates/filesystem/src/model.rs:104` | mixed | Separate page/chunk sizing and tree invariants from whole-volume/batch/read refusals. Pending. |
| Filesystem probe/capture/closure/decode/wire budgets | `rust/crates/filesystem/src/kernel/probe.rs:60` and related options | mixed | Preserve user allocation accounting and bounded pages; remove default total-work refusals. Pending. |
| Harness conversation `Limits` | `rust/crates/harness-core/src/conversation/mod.rs:63` | 1 (opt-in) | Defaults already use wire representations; explicit scope attenuation remains user-controlled. |
| MCP catalog servers/tools/pages/bytes | `rust/crates/harness-core/src/mcp/install.rs:22` | 1 | Default allowances are None; follow all cursor pages. Explicit host allowances remain optional; repeated cursors still fail. |
| OIDC JWKS/key/token options | `rust/crates/harness-core/src/oidc/keys.rs:36` | 4 | Default byte/key allowances are None, explicit host allowances remain optional. Signature, issuer, audience, expiry and cryptographic shape checks remain. |
| Context discovery directories/entries/bytes | `rust/crates/harness-core/src/context/discovery/declaration.rs:180` | 1 | Default allowances are None. Instruction reads use the representable file length; skill frontmatter reads grow geometrically until the closing delimiter or EOF. Explicit caller allowances remain optional. |
| Simulation machines/operations/events | `rust/crates/machines/src/simulation/backend.rs:46`; `simulation/mod.rs:117` | 1 | Deleted machine/operation/event refusal checks and cap-only tests. |
| Model context windows | Provider model capabilities and compaction | 4 | Keep physical model windows and compaction. |
| G62 approvals/reservations; explicit actor CPU/memory/time | User configuration | 3/4 | Keep user-managed resource capacity; no new default product cap. |

## Work still in progress

The following mechanisms still need structural changes before this milestone can ship:

- Stream whole-record/whole-command limits versus framed transport, request item/path limits, dynamic header preparation, and continuation of large atomic payloads.
- Actor request envelopes and harness command chunking; large agent record/checkpoint values must cross every backend without hidden refusal.
- Filesystem whole-volume defaults, native extent collection defaults and exact-install change-count refusal. Path counts are optional; bulk capture collection/union scaling is being verified.
- Projection checkpoint manifests and default cold-history refusal budgets.
- Codex credential-file/refresh byte limits, qualification/archive limits, and remaining non-constant refusal sites.
- Rebase onto merged be-host PR1725's authoritative subscription cuts without restoring the checkpoint cursor.

Verification so far: full core library tests passed on bb2 (600 passed, zero failed,
ignored or filtered), including persistent membership pagination/reopen. This test
receipt predates later string/MCP/OIDC edits. Native compile validation is ongoing.

## Latest implementation evidence

- Native compile check 6 passed on bb2, including harness filesystem/native execution/MCP HTTP/OIDC and core, streams, objects, actors, machines and conformance all-targets. Receipt: `cc8885c3d0e4`, `/home/var/log/buildbox/1793160020596-18539-8e9dbb1d.log`. Later discovery/capture/reply changes are not covered by that receipt.
- Check 7 exposed three N-API test callers requiring explicit `Some` for their deliberately finite path controls. Those callers are fixed; check 8 includes filesystem all-targets and N-API and is still running.
- Actor call replies no longer have a hidden whole-value maximum. The receiver reserves only bytes in received fragments; credit, fragment order, announced-total consistency and explicit finite caller allowances remain.
- Discovery entry/directory/instruction/frontmatter defaults are unbounded. Source revision, canonical order, scan accounting, cancellation and explicit finite controls remain.

This inventory remains a working audit. Pending entries are not represented as implemented or validated.
