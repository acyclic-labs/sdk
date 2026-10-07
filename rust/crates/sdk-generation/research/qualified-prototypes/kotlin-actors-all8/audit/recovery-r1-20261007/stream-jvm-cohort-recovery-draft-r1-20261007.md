# Stream JVM cohort recovery draft r1

Revision: `stream-jvm-cohort-recovery-r1-20261007`  
Date: 2026-10-07  
Status: unpublished recovery evidence; no merge, registry update, or deployment.

## Exact preserved cohort

The Mac source archive and raw terminal receipt were copied under unique revision names on both the task-local Mac host and the primary research tree. Independent digest verification matched:

| Artifact | SHA-256 |
| --- | --- |
| `source/recovery-r1-20261007/stream-jvm-cohort-recovery-r1-source-20261007.tar.gz` | `C982E1886F204C24F250766EC2F0A4A28512B2F88B32E71A869A05DACDBACFFB` |
| `audit/recovery-r1-20261007/stream-jvm-cohort-recovery-r1-mac-receipt-20261007.raw.txt` | `6E40FB658024CDD75BC17FC73979C92FC28F0276A5F04A430785F76D5C302B46` |
| `source/recovery-r1-20261007/stream-jvm-cohort-recovery-r1-platform-artifacts-20261007.tar.gz` | `03A3915C864B3F74C8CF1856F708F260D081516AD936F36C90CC6069DD721A37` |

The archive contains the exact Stream producer source closure, prototype source and lockfiles, fixture source and lockfile, and native-runtime source used for the Mac rebuild. The receipt records Rust 1.98.1, task-local Temurin 17.0.20.1+1, the actual source/lock hashes, native dylib and fixture hashes, final installed JAR hash, and the local/remote pass markers. No historical native library is assigned this cohort's lock identity.

The platform archive contains the final Windows JAR/DLL, Linux JAR/SO, and Mac arm64 JAR/dylib. Its manifest records the six independent artifact digests. The source and platform archives and the raw receipt were also retained under the same revision names on `/Users/var/task-stream-cohort-20261007` on `ivar`; the archive digest was rechecked after transfer.

## Shared producer contract for the next language cohort

Kotlin, Dart, and .NET should generate bindings from the same Rust Stream producer and metadata. The foreign surface remains the minimal opaque-cursor contract:

```text
connect(endpoint, bearerToken, caCertificatePem)
append(path, value)
read(path, after, limit) -> RecordCursor
follow(path, from) -> RecordCursor
children(parent?, limit) -> ChildCursor
activeRecordCursors() -> u64
RecordCursor.next() -> StreamRecord?
RecordCursor.closeCursor()
ChildCursor.next() -> StreamChild?
ChildCursor.closeCursor()
```

The adapters may expose language-native async streams (`Flow`, Dart `Stream`, or .NET `IAsyncEnumerable`) but only pull from the generated cursor and close it in cancellation/finally. Rust remains authoritative for ordering, replay/cursor semantics, transport, typed errors, and stream drop. No language owner should add a recovery algorithm, operation list, semantic field mirror, retry policy, or custom native loader.

## Minimal production integration and deletion plan

1. Admit one shared Rust Stream producer snapshot and lock identity. Generate Kotlin, Dart, and .NET bindings from that snapshot; compare generated operation and record inventories before packaging.
2. Keep only thin language adapters: Kotlin `Flow`, Dart `Stream`, and .NET async enumeration. Each adapter must have one cancellation test proving the generated cursor closes and the Rust/server stream is dropped.
3. Package one platform-native library per supported target using the maintained standard loader/resource convention. Record each native source and lock identity separately; never retag a prior platform byte sequence.
4. After all three generated surfaces and cancellation receipts are admitted, delete the research-only handwritten probes, fixture launch wrappers, duplicate generated outputs, and temporary task-local build directories. Retain this source archive, raw receipts, generated-source hashes, and the minimal adapter source as recovery evidence.
5. Do not delete the Rust producer, generated metadata source, or shared fixture contract. Those remain the single semantic producer for later cohorts.

The Kotlin-specific API receipt is adjacent at `../stream-uniffi-dart-shared-api-20261007.md`; it documents the same contract for Dart and is intentionally free of language-specific recovery behavior.
