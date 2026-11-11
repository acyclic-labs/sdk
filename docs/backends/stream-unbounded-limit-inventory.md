# Stream limit source inventory

Baseline 506843a3822; raw sites supplement the design classification.
Includes tests so removed contracts and negative controls can be located.

## Classification and required disposition

The entries below classify mechanisms, rather than treating every use of the
error enum as an independent restriction. A frame check is protective only
after the shared sender splits every legitimate logical input and output before
that check. The current unary encoders do not satisfy that prerequisite.

| Source mechanism | Class | Required disposition |
| --- | --- | --- |
| `request.rs` record/effect/condition/mutation counts, command bytes and exclusions | Logical request size | Replace with shared staged validation and incremental canonicalization. Empty records are malformed input, not a size limit. Allocation failures become capacity failures. |
| `lib/path.rs`, `memory/capacity.rs` graft depth/length, `memory/provider.rs` joined paths, `wire_codec/session/resource.rs` resource length | Logical request size | Stage path bytes and preserve segment semantics; remove the path and inherited-subtree ceilings together. Validate resource syntax independently of length. |
| `record_headers/validation.rs`, `record_headers.rs`, `lib/domain.rs` key selectors, `follow_filter.rs` keys/attributes/predicates | Logical request size | Page headers and selectors; preserve canonical ordering and query semantics without count or byte rejection. |
| `changes.rs` change-envelope sizing and single oversized change | Logical data size and physical page packing | Keep page packing, but allow one logical change to span physical pages; a record or change must not need to fit one page. |
| `memory/capacity.rs` requested read limit, `memory/provider.rs` heads page bytes, `wire_codec/head.rs`, `http_validation.rs`, `http/follow.rs`, `http.rs` result memberships | Logical output size | Return bounded result pages rooted in the whole decision. A requested page size is a work hint; it must not reject a legitimate commit or truncate its atomic batch. |
| `memory/prepared.rs`, `native/prepared.rs`, `record_headers/prepared/controls.rs` single-record prepared envelope and maximum | Logical request size | Feed the same staged cursor and planner; remove the one-mutation/one-record representation dependency rather than bypassing native accounting. |
| `journal.rs`, `journal_provider.rs`, `local/prepared_commit.rs`, `local/prepared_frame.rs`, `local/frame_checksum.rs`, `memory/snapshot/mod.rs` whole-command/envelope bounds | Logical persisted size currently disguised as frame protection | Persist immutable pages first and one small versioned root decision last. Recovery must never reconstruct or admit a request-sized frame. |
| `grpc/server.rs`, `http_codec.rs`, `browser/port.rs`, `wasm/provider_rpc.rs`, `wasm/validation.rs` unary request/response bounds | Logical request size at current API boundary | Route logical operations through shared staging and result cursors. Retain bounds solely on individual physical transport frames. |
| `wire_codec/records.rs` record payload and `http_validation.rs` record payload | Logical record size | Resolve content references transparently, with bounded record-content pages. The owned adapter may assemble at the caller boundary. |
| `wire_codec/records.rs` decoded response length and decompression, `wire_codec/provider_cursor.rs`, session framing and wasm incoming-frame checks | Protective physical frame bound, conditional on splitting | Retain bounded decoding before allocation/decompression; require page senders to split first. Never raise a frame ceiling to accommodate a logical commit. |
| `session/client/input.rs`, `session/client/socket.rs`, `session/sockets.rs`, `session/mod.rs`, `session/frame.rs` socket/actor payloads | Logical payload size until frame split | Split and reassemble through shared session/content framing. Preserve actor/socket message boundaries; frame boundaries cannot become application message boundaries. |
| `session/calls.rs` whole actor reply, `session/client/call.rs` invocation, `session/interest.rs`, `session/authority.rs` invocation/open bytes, hello/credential size | Logical control payload size | Stage logical payloads before bounded session envelopes. Auth syntax/authority checks remain separate; a credential length check is not proven hostile-input protection merely because it is a credential. |
| `session/connection.rs` waiter count, `session/client.rs` channel capacity, session pending frame/byte credits | Backpressure where waiting; arbitrary count where refused | Preserve event-driven credit waits. Replace waiter-count refusal with admission/ownership transfer; do not use a queue ceiling as a logical operation limit. |
| `preparation.rs` skipped effect `u32` conversion | Logical representation ceiling | Use wide effect positions in paged results and persisted facts. A staged request must not pass ingestion and fail when its skipped positions are encoded. |
| `memory/commit.rs`, `memory/prepared.rs`, `preparation.rs`, `lib/stream.rs` checked sequence/cursor additions; session credit additions; encoded-size arithmetic | Protective arithmetic overflow | Keep checked arithmetic. Physical counters do not wrap; page counters must not be substituted for logical counts. Overflow is distinct from the removed 1024/64 KiB/1 MiB policy. |
| `lib/receipt.rs::project` exhausted caller-funded buffers | Explicit caller capacity | Preserve allocation custody, but page projection and expose capacity/backpressure rather than implying a primitive request ceiling. Never grow the caller-funded buffer silently. |
| `lib/error.rs`, `wire_codec/status.rs`, wasm error codes | Error transport, not a new bound | Audit remaining producers before removing or repurposing a public error code. Mapping an error is not evidence that the original restriction is protective. |

Tests, conformance assertions, fuzz dispatch and negative controls in the raw
inventory below are consumers of these contracts. Replace tests of deleted
ceilings with the shared atomic staged cases; retain malformed frame and
arithmetic controls. This classification is a removal plan, not evidence that
the production logical limits have already been removed.

```text
rust/crates/stream/proto\stream\v1\stream.proto:12:  STREAM_LIMIT_MAX_RECORD_BYTES = 65536;
rust/crates/stream/proto\stream\v1\stream.proto:13:  STREAM_LIMIT_MAX_ITEMS = 1024;
rust/crates/stream/proto\stream\v1\stream.proto:14:  STREAM_LIMIT_MAX_COMMAND_BYTES = 1056768;
rust/crates/stream/proto\stream\v1\stream.proto:16:  STREAM_LIMIT_MAX_PATH_BYTES = 65535;
rust/crates/stream/proto\stream\v1\stream.proto:297:// A client rejects decoded_length above STREAM_LIMIT_MAX_COMMAND_BYTES before it
rust/crates/stream/proto\stream\v1\session.proto:180:    // One whole encoded Actors socket frame, at most MAX_RECORD_BYTES. From
rust/crates/stream/proto\stream\v1\session.proto:185:    // MAX_RECORD_BYTES. after.next counts chunks starting at 1; after.opaque
rust/crates/stream/src\changes.rs:143:            return Err(StreamError::LimitExceeded);
rust/crates/stream/src\changes.rs:155:        if self.total_with(value, self.count + 1).saturating_add(keys) > crate::MAX_RECORD_BYTES {
rust/crates/stream/src\changes.rs:156:            return Err(StreamError::LimitExceeded);
rust/crates/stream/src\changes.rs:197:            Err(StreamError::LimitExceeded) if size.count > 0 => break,
rust/crates/stream/src\changes.rs:202:        return Err(StreamError::LimitExceeded);
rust/crates/stream/src\changes.rs:236:        usize::try_from(value).map_err(|_| StreamError::LimitExceeded)
rust/crates/stream/src\changes.rs:426:            if bytes.len().saturating_add(headers.payload_bytes()) > crate::MAX_RECORD_BYTES {
rust/crates/stream/src\changes.rs:427:                return Err(StreamError::LimitExceeded);
rust/crates/stream/src\changes.rs:491:    if value.len().saturating_add(headers.payload_bytes()) > crate::MAX_RECORD_BYTES {
rust/crates/stream/src\changes.rs:492:        return Err(StreamError::LimitExceeded);
rust/crates/stream/src\browser\tests.rs:748:            Some(Err(StreamError::LimitExceeded))
rust/crates/stream/src\browser\port.rs:83:            let frame = bounded_buffer(value, 9, MAX_COMMAND_BYTES + 8)?;
rust/crates/stream/src\codec_fuzz.rs:219:            Err(StreamError::LimitExceeded) => None,
rust/crates/stream/src\codec_fuzz.rs:225:                Err(StreamError::LimitExceeded) => continue,
rust/crates/stream/src\codec_fuzz.rs:264:        Some(Err(StreamError::LimitExceeded)) => {}
rust/crates/stream/src\codec_fuzz.rs:352:            !matches!(encode(value, maximum), Err(StreamError::LimitExceeded))
rust/crates/stream/src\codec_fuzz.rs:353:                || !matches!(decode(&bytes, maximum), Err(StreamError::LimitExceeded))
rust/crates/stream/src\follow_filter.rs:46:        if requested.encoded_len() > crate::MAX_COMMAND_BYTES
rust/crates/stream/src\follow_filter.rs:47:            || requested.all_of.len() > crate::MAX_ITEMS
rust/crates/stream/src\follow_filter.rs:49:            return Err(StreamError::LimitExceeded);
rust/crates/stream/src\follow_filter.rs:229:        if filter.encoded_len() > crate::MAX_COMMAND_BYTES {
rust/crates/stream/src\follow_filter.rs:230:            return Err(StreamError::LimitExceeded);
rust/crates/stream/src\follow_filter.rs:317:    if filter.keys.len() > crate::MAX_ITEMS
rust/crates/stream/src\follow_filter.rs:319:        || filter.encoded_len() > crate::MAX_COMMAND_BYTES
rust/crates/stream/src\follow_filter.rs:321:        return Err(StreamError::LimitExceeded);
rust/crates/stream/src\follow_filter.rs:327:            return Err(StreamError::LimitExceeded);
rust/crates/stream/src\follow_filter.rs:368:        if attribute.any_of.len() > crate::MAX_ITEMS {
rust/crates/stream/src\follow_filter.rs:369:            return Err(StreamError::LimitExceeded);
rust/crates/stream/src\changes\tests.rs:334:        .native_prepare_commit(request.clone(), crate::MAX_RECORD_BYTES, meter.original())
rust/crates/stream/src\changes\tests.rs:352:        .native_prepare_commit(request, crate::MAX_RECORD_BYTES, meter.original())
rust/crates/stream/src\conformance.rs:965:    let value = Bytes::from(vec![0; crate::MAX_RECORD_BYTES / 2]);
rust/crates/stream/src\conformance.rs:967:    let count = crate::MAX_COMMAND_BYTES / value.len();
rust/crates/stream/src\conformance.rs:997:            limit: u32::try_from(count).map_err(|_| StreamError::LimitExceeded)?,
rust/crates/stream/src\conformance.rs:1002:        Err(StreamError::LimitExceeded) => return Ok(()),
rust/crates/stream/src\conformance.rs:1015:            > crate::MAX_COMMAND_BYTES
rust/crates/stream/src\conformance\soft.rs:212:        (Vec::new(), StreamError::LimitExceeded),
rust/crates/stream/src\conformance\soft.rs:215:            StreamError::LimitExceeded,
rust/crates/stream/src\conformance\soft.rs:222:            StreamError::LimitExceeded,
rust/crates/stream/src\head_tests.rs:9:    let payload = OwnedBytes::from(vec![42; MAX_RECORD_BYTES / 2]);
rust/crates/stream/src\head_tests.rs:10:    let count = MAX_COMMAND_BYTES / payload.len() + 1;
rust/crates/stream/src\head_tests.rs:36:        limit: u32::try_from(count).map_err(|_| StreamError::LimitExceeded)?,
rust/crates/stream/src\head_tests.rs:45:    let payload = vec![42; MAX_RECORD_BYTES / 2];
rust/crates/stream/src\head_tests.rs:54:        Err(StreamError::LimitExceeded)
rust/crates/stream/src\grpc\server.rs:19:    if request.get_ref().encoded_len() > crate::MAX_COMMAND_BYTES {
rust/crates/stream/src\grpc\server.rs:20:        Err(Status::from(&StreamError::LimitExceeded))
rust/crates/stream/src\http\follow.rs:67:    if bytes.len().saturating_add(headers.payload_bytes()) > MAX_RECORD_BYTES {
rust/crates/stream/src\http\follow.rs:130:    if mutations.len() > MAX_ITEMS {
rust/crates/stream/src\http\follow.rs:147:            if records.len() > MAX_ITEMS {
rust/crates/stream/src\http_validation.rs:105:    if value.len().saturating_add(headers.payload_bytes()) > crate::MAX_RECORD_BYTES {
rust/crates/stream/src\http_validation.rs:245:            || appends.len().saturating_add(forks.len()) > crate::MAX_ITEMS
rust/crates/stream/src\http_validation.rs:246:            || skipped.len() > crate::MAX_ITEMS
rust/crates/stream/src\http_validation.rs:266:            if start >= tail || tail - start > crate::MAX_ITEMS as u64 {
rust/crates/stream/src\http_validation.rs:306:            if excluded.len() > crate::MAX_ITEMS {
rust/crates/stream/src\http_codec.rs:2:use crate::{MAX_COMMAND_BYTES, TokenOperation, memory, wire, wire_codec};
rust/crates/stream/src\http_codec.rs:220:    if input.len() > MAX_COMMAND_BYTES {
rust/crates/stream/src\grpc\tests.rs:160:        Err(StreamError::LimitExceeded)
rust/crates/stream/src\grpc\tests.rs:260:        path: "x".repeat(crate::MAX_PATH_BYTES),
rust/crates/stream/src\grpc\tests.rs:263:                value: Bytes::from(vec![0_u8; crate::MAX_RECORD_BYTES]),
rust/crates/stream/src\http.rs:319:            if conflicts.len() > MAX_ITEMS {
rust/crates/stream/src\journal_provider.rs:236:                let maximum = u32::try_from(MAX_COMMAND_BYTES + 17).map_err(unavailable)?;
rust/crates/stream/src\journal_provider.rs:252:                if retained.length() as usize > MAX_COMMAND_BYTES {
rust/crates/stream/src\journal_provider.rs:253:                    return Err(StreamError::LimitExceeded);
rust/crates/stream/src\journal.rs:63:        if self.encoded_len() > crate::MAX_COMMAND_BYTES {
rust/crates/stream/src\journal.rs:64:            return Err(StreamError::LimitExceeded);
rust/crates/stream/src\http\http_follow_tests.rs:293:        assert_eq!(server.client.append(request.clone()).await, Err(StreamError::LimitExceeded));
rust/crates/stream/src\lib.rs:185:pub const MAX_RECORD_BYTES: usize = wire::StreamLimit::MaxRecordBytes as usize;
rust/crates/stream/src\lib.rs:187:pub const MAX_ITEMS: usize = wire::StreamLimit::MaxItems as usize;
rust/crates/stream/src\lib.rs:189:const _: () = assert!(REPLAY_PAGE as usize == MAX_ITEMS);
rust/crates/stream/src\lib.rs:191:pub const MAX_COMMAND_BYTES: usize = wire::StreamLimit::MaxCommandBytes as usize;
rust/crates/stream/src\lib.rs:195:pub const MAX_PATH_BYTES: usize = wire::StreamLimit::MaxPathBytes as usize;
rust/crates/stream/src\journal_provider\port.rs:54:            if !(18..=MAX_COMMAND_BYTES + 17).contains(&length) {
rust/crates/stream/src\http\session_tests.rs:276:            MAX_COMMAND_BYTES,
rust/crates/stream/src\http\session_tests.rs:304:    let client = HttpStream::new(endpoint, "fixture-token", MAX_COMMAND_BYTES)?;
rust/crates/stream/src\native\prepared.rs:41:            Err(StreamError::LimitExceeded)
rust/crates/stream/src\local\frame_checksum.rs:7:    maximum_body: (FRAME_TIME_BYTES + MAX_COMMAND_BYTES) as u64,
rust/crates/stream/src\lib\stream.rs:240:            self.next = self.next.checked_add(1).ok_or(StreamError::LimitExceeded)?;
rust/crates/stream/src\local\prepared_commit.rs:42:                                if size > MAX_COMMAND_BYTES {
rust/crates/stream/src\local\prepared_frame.rs:91:        if command.encoded_len() > MAX_COMMAND_BYTES {
rust/crates/stream/src\local\prepared_frame.rs:107:        if size > MAX_COMMAND_BYTES
rust/crates/stream/src\local\mod.rs:46:    IdempotencyObservation, MAX_COMMAND_BYTES, MemoryLimits, MemoryStream, ReadRequest,
rust/crates/stream/src\persistence.rs:4:    IdempotencyOutcome, MAX_ITEMS, MAX_RECORD_BYTES, Record, StreamError, StreamPath, wire,
rust/crates/stream/src\persistence.rs:105:        return Err(StreamError::LimitExceeded);
rust/crates/stream/src\persistence.rs:113:        .ok_or(StreamError::LimitExceeded)?;
rust/crates/stream/src\persistence.rs:135:                || receipt.end - receipt.start > MAX_ITEMS as u64
rust/crates/stream/src\persistence.rs:153:    if value.mutations.len() > MAX_ITEMS {
rust/crates/stream/src\persistence.rs:189:    if records.len() > MAX_ITEMS || end - start != records.len() as u64 {
rust/crates/stream/src\persistence.rs:195:            || record.payload_bytes() > MAX_RECORD_BYTES
rust/crates/stream/src\lib\receipt.rs:51:            || request.mutations().count() > MAX_ITEMS
rust/crates/stream/src\lib\receipt.rs:52:            || request.conditions().count() > MAX_ITEMS
rust/crates/stream/src\lib\receipt.rs:91:                        if records.is_empty() || records.len() > MAX_ITEMS {
rust/crates/stream/src\lib\receipt.rs:121:                            || records.len() > MAX_ITEMS
rust/crates/stream/src\lib\receipt.rs:172:                        return Err(StreamError::LimitExceeded.into());
rust/crates/stream/src\lib\receipt.rs:185:                        return Err(StreamError::LimitExceeded.into());
rust/crates/stream/src\lib\receipt.rs:252:            || count > MAX_ITEMS
rust/crates/stream/src\lib\receipt.rs:253:            || self.skipped.len() > MAX_ITEMS
rust/crates/stream/src\lib\receipt.rs:265:                    || a.tail - a.start > MAX_ITEMS as u64
rust/crates/stream/src\lib\receipt.rs:274:                    || f.tail - f.forked_at > MAX_ITEMS as u64
rust/crates/stream/src\lib\provider.rs:113:    /// Invalid limits or keys return LimitExceeded before revision waits or IO.
rust/crates/stream/src\persistence\tests.rs:210:            _ => first.value = Bytes::from(vec![0; MAX_RECORD_BYTES + 1]),
rust/crates/stream/src\preparation.rs:123:                skipped.push(u32::try_from(index).map_err(|_| StreamError::LimitExceeded)?);
rust/crates/stream/src\preparation.rs:245:            skipped.push(u32::try_from(index).map_err(|_| StreamError::LimitExceeded)?);
rust/crates/stream/src\preparation.rs:277:        .checked_add(u64::try_from(records.len()).map_err(|_| StreamError::LimitExceeded)?)
rust/crates/stream/src\preparation.rs:278:        .ok_or(StreamError::LimitExceeded)?;
rust/crates/stream/src\preparation.rs:351:                        u64::try_from(records.len()).map_err(|_| StreamError::LimitExceeded)?,
rust/crates/stream/src\preparation.rs:353:                    .ok_or(StreamError::LimitExceeded)?;
rust/crates/stream/src\preparation.rs:384:        .checked_add(u64::try_from(bodies.len()).map_err(|_| StreamError::LimitExceeded)?)
rust/crates/stream/src\preparation.rs:385:        .ok_or(StreamError::LimitExceeded)?;
rust/crates/stream/src\preparation.rs:388:            .checked_add(u64::try_from(index).map_err(|_| StreamError::LimitExceeded)?)
rust/crates/stream/src\preparation.rs:389:            .ok_or(StreamError::LimitExceeded)?;
rust/crates/stream/src\record_headers.rs:130:        validation.finish(crate::MAX_RECORD_BYTES)
rust/crates/stream/src\record_headers.rs:213:    /// Value and header bytes counted together against `MAX_RECORD_BYTES`.
rust/crates/stream/src\postgres\capability.rs:71:            Some(Err(StreamError::LimitExceeded))
rust/crates/stream/src\lib\path_text_tests\baseline.rs:18:            || path.len() > MAX_PATH_BYTES
rust/crates/stream/src\lib\path_text_tests\baseline.rs:27:            count = count.checked_add(1).ok_or(StreamError::LimitExceeded)?;
rust/crates/stream/src\lib\path_text_tests\baseline.rs:39:        if count > MAX_ITEMS {
rust/crates/stream/src\lib\path_text_tests\baseline.rs:40:            return Err(StreamError::LimitExceeded);
rust/crates/stream/src\lib\path.rs:1:use crate::{MAX_ITEMS, MAX_PATH_BYTES, StreamError};
rust/crates/stream/src\lib\path.rs:76:            || path.len() > MAX_PATH_BYTES
rust/crates/stream/src\lib\path.rs:85:            count = count.checked_add(1).ok_or(StreamError::LimitExceeded)?;
rust/crates/stream/src\lib\path.rs:97:        if count > MAX_ITEMS {
rust/crates/stream/src\lib\path.rs:98:            return Err(StreamError::LimitExceeded);
rust/crates/stream/src\lib\path.rs:130:        if length.is_none_or(|length| length > MAX_PATH_BYTES) {
rust/crates/stream/src\lib\path.rs:133:        if self.as_str().split('/').count() + suffix[1..].split('/').count() > MAX_ITEMS {
rust/crates/stream/src\lib\path.rs:134:            return Ok(Err(StreamError::LimitExceeded));
rust/crates/stream/src\request.rs:5:    MAX_COMMAND_BYTES, MAX_ITEMS, MAX_RECORD_BYTES, StreamError, StreamPath,
rust/crates/stream/src\request.rs:9:    if records.is_empty() || records.len() > MAX_ITEMS {
rust/crates/stream/src\request.rs:10:        return Err(StreamError::LimitExceeded);
rust/crates/stream/src\request.rs:13:        if record.payload_bytes() > MAX_RECORD_BYTES {
rust/crates/stream/src\request.rs:14:            return Err(StreamError::LimitExceeded);
rust/crates/stream/src\request.rs:22:    (bytes <= MAX_COMMAND_BYTES)
rust/crates/stream/src\request.rs:24:        .ok_or(StreamError::LimitExceeded)
rust/crates/stream/src\request.rs:49:    if paths.len() > MAX_ITEMS {
rust/crates/stream/src\request.rs:50:        return Err(StreamError::LimitExceeded);
rust/crates/stream/src\request.rs:55:        .map_err(|_| StreamError::LimitExceeded)?;
rust/crates/stream/src\request.rs:61:    if paths.len() > MAX_ITEMS {
rust/crates/stream/src\request.rs:62:        return Err(StreamError::LimitExceeded);
rust/crates/stream/src\request.rs:597:        || request.conditions().count() > MAX_ITEMS
rust/crates/stream/src\request.rs:598:        || request.mutations().count() > MAX_ITEMS
rust/crates/stream/src\request.rs:600:        return Err(StreamError::LimitExceeded);
rust/crates/stream/src\request.rs:903:            binding.matches_with(&facts, |_| Err::<(), _>(StreamError::LimitExceeded)),
rust/crates/stream/src\request.rs:904:            Err(StreamError::LimitExceeded)
rust/crates/stream/src\lib\error.rs:19:    LimitExceeded,
rust/crates/stream/src\lib\error.rs:95:        Self::LimitExceeded,
rust/crates/stream/src\lib\error.rs:127:            Self::LimitExceeded => "limit_exceeded",
rust/crates/stream/src\lib\domain.rs:350:            return Err(StreamError::LimitExceeded);
rust/crates/stream/src\record_header_tests.rs:3:    CommitRequest, IdempotencyKey, MAX_RECORD_BYTES, OwnedBytes, RecordHeaders, StreamError,
rust/crates/stream/src\record_header_tests.rs:29:            value: vec![0; MAX_RECORD_BYTES - 1].into(),
rust/crates/stream/src\record_header_tests.rs:40:        .value = vec![0; MAX_RECORD_BYTES].into();
rust/crates/stream/src\record_header_tests.rs:43:        Err(StreamError::LimitExceeded)
rust/crates/stream/src\record_header_tests.rs:48:        Err(StreamError::LimitExceeded)
rust/crates/stream/src\record_header_tests.rs:66:        Err(StreamError::LimitExceeded)
rust/crates/stream/src\record_headers\validation.rs:17:            return Err(StreamError::LimitExceeded);
rust/crates/stream/src\record_headers\validation.rs:28:            return Err(StreamError::LimitExceeded);
rust/crates/stream/src\record_headers\validation.rs:46:            Err(StreamError::LimitExceeded)
rust/crates/stream/src\record_headers\prepared\tests.rs:54:            value: vec![1; crate::MAX_RECORD_BYTES - "λ🙂".len()].into(),
rust/crates/stream/src\memory\capacity.rs:2:    if limit == 0 || usize::try_from(limit).map_or(true, |limit| limit > MAX_ITEMS) {
rust/crates/stream/src\memory\capacity.rs:3:        Err(StreamError::LimitExceeded)
rust/crates/stream/src\memory\capacity.rs:182:    if destination.as_str().len().saturating_add(node.max_suffix()) > crate::MAX_PATH_BYTES
rust/crates/stream/src\memory\capacity.rs:183:        || destination.as_str().split('/').count() + node.depth() - 1 > MAX_ITEMS
rust/crates/stream/src\memory\commit.rs:123:            .ok_or(StreamError::LimitExceeded)?;
rust/crates/stream/src\record_headers\prepared\controls.rs:25:        if maximum > crate::MAX_RECORD_BYTES
rust/crates/stream/src\record_headers\prepared\controls.rs:132:        StreamError::LimitExceeded => io::ErrorKind::InvalidInput.into(),
rust/crates/stream/src\local\tests\native\reads.rs:63:                    Some(Err(StreamError::LimitExceeded))
rust/crates/stream/src\local\tests\native\reads.rs:70:                    Some(Ok(Err(StreamError::LimitExceeded)))
rust/crates/stream/src\session\authority.rs:95:            .filter(|total| *total <= crate::MAX_COMMAND_BYTES)
rust/crates/stream/src\wire_codec\head.rs:62:    if super::requests::heads_len(&value) > crate::MAX_COMMAND_BYTES {
rust/crates/stream/src\wire_codec\head.rs:63:        return Err(StreamError::LimitExceeded);
rust/crates/stream/src\session\calls.rs:333:            return Err(StreamError::LimitExceeded);
rust/crates/stream/src\session\calls.rs:341:                .step_by(crate::MAX_RECORD_BYTES)
rust/crates/stream/src\session\calls.rs:344:                    let end = (start + crate::MAX_RECORD_BYTES).min(reply.len());
rust/crates/stream/src\session\interest.rs:205:                    || invocation.invocation.len() > crate::MAX_COMMAND_BYTES
rust/crates/stream/src\session\interest.rs:244:                    || open.len() > crate::MAX_COMMAND_BYTES
rust/crates/stream/src\memory\tests\subtree.rs:129:    let value = vec![0u8; crate::MAX_RECORD_BYTES / 2];
rust/crates/stream/src\memory\tests\subtree.rs:130:    let count = crate::MAX_COMMAND_BYTES / value.len() + 1;
rust/crates/stream/src\memory\tests\subtree.rs:131:    assert!(count < crate::MAX_ITEMS);
rust/crates/stream/src\memory\tests\subtree.rs:147:        limit: u32::try_from(count).map_err(|_| StreamError::LimitExceeded)?,
rust/crates/stream/src\memory\tests\subtree.rs:151:        Err(StreamError::LimitExceeded)
rust/crates/stream/src\memory\tests\subtree.rs:176:        "x".repeat(crate::MAX_PATH_BYTES - "agents/".len())
rust/crates/stream/src\session\frame.rs:38:            > crate::MAX_RECORD_BYTES
rust/crates/stream/src\session\frame.rs:40:            return Err(StreamError::LimitExceeded);
rust/crates/stream/src\wire_codec\status_contract.rs:27:        StreamError::LimitExceeded,
rust/crates/stream/src\wire_codec\status_contract.rs:203:                StreamError::InvalidPath => StreamError::LimitExceeded,
rust/crates/stream/src\wire_codec\status_contract.rs:204:                StreamError::LimitExceeded => StreamError::InvalidPath,
rust/crates/stream/src\session\connection.rs:96:            if state.waiters.len() == crate::MAX_ITEMS {
rust/crates/stream/src\wire_codec\status.rs:54:        (Code::InvalidArgument, "limit_exceeded") => StreamError::LimitExceeded,
rust/crates/stream/src\wire_codec\status.rs:97:        StreamError::LimitExceeded => Status::invalid_argument("limit_exceeded"),
rust/crates/stream/src\session\client.rs:77:const FRAME_BYTES: u64 = crate::MAX_RECORD_BYTES as u64 + 128;
rust/crates/stream/src\session\client.rs:377:        let (commands, receiver) = mpsc::channel(crate::MAX_ITEMS);
rust/crates/stream/src\session\client.rs:460:        if frame.encoded_len() > crate::MAX_COMMAND_BYTES {
rust/crates/stream/src\session\client.rs:461:            return Err(StreamError::LimitExceeded);
rust/crates/stream/src\wire_codec\session.rs:13:pub const MAX_ACTOR_CALL_REPLY_BYTES: usize = crate::MAX_COMMAND_BYTES + 128;
rust/crates/stream/src\wire_codec\session.rs:29:            if !bytes.is_empty() && bytes.len() <= crate::MAX_RECORD_BYTES)
rust/crates/stream/src\wire_codec\session.rs:78:        .and_then(|index| index.checked_mul(crate::MAX_RECORD_BYTES))
rust/crates/stream/src\wire_codec\session.rs:82:        || chunk.len() > crate::MAX_RECORD_BYTES
rust/crates/stream/src\wire_codec\session.rs:84:        || chunk.len() != (total - start).min(crate::MAX_RECORD_BYTES)
rust/crates/stream/src\wire_codec\session.rs:103:            if !frame.is_empty() && frame.len() <= crate::MAX_RECORD_BYTES
rust/crates/stream/src\wire_codec\session.rs:118:    if bytes.len() > crate::MAX_COMMAND_BYTES {
rust/crates/stream/src\wire_codec\session.rs:119:        return Err(StreamError::LimitExceeded);
rust/crates/stream/src\wire_codec\session.rs:355:        .ok_or(StreamError::LimitExceeded)?;
rust/crates/stream/src\wire_codec\session.rs:359:        .ok_or(StreamError::LimitExceeded)?;
rust/crates/stream/src\wire_codec\session.rs:378:    let length = u64::try_from(frame.encoded_len()).map_err(|_| StreamError::LimitExceeded)?;
rust/crates/stream/src\memory\tests\native_controls.rs:729:            matches!(&failure, StreamError::LimitExceeded)
rust/crates/stream/src\session\client\input.rs:295:        if bytes.is_empty() || bytes.len() > crate::MAX_RECORD_BYTES {
rust/crates/stream/src\session\client\input.rs:309:        let length = u32::try_from(value.encoded_len()).map_err(|_| StreamError::LimitExceeded)?;
rust/crates/stream/src\session\client\input.rs:336:        if value.encoded_len() > crate::MAX_COMMAND_BYTES {
rust/crates/stream/src\session\client\input.rs:337:            return Err(StreamError::LimitExceeded);
rust/crates/stream/src\session\client\call.rs:247:        || invocation.len() > crate::MAX_COMMAND_BYTES
rust/crates/stream/src\wasm\provider_rpc.rs:51:    if usize::try_from(bytes.length()).map_err(|_| StreamError::LimitExceeded)? > maximum {
rust/crates/stream/src\wasm\provider_rpc.rs:52:        return Err(StreamError::LimitExceeded);
rust/crates/stream/src\wasm\provider_rpc.rs:116:        self.call_with_limit(operation, input, MAX_COMMAND_BYTES)
rust/crates/stream/src\wasm\provider_rpc.rs:126:        if input.len() > MAX_COMMAND_BYTES {
rust/crates/stream/src\wasm\provider_rpc.rs:127:            return Err(StreamError::LimitExceeded);
rust/crates/stream/src\wasm\provider_rpc.rs:144:        if input.len() > MAX_COMMAND_BYTES {
rust/crates/stream/src\wasm\provider_rpc.rs:145:            return Err(StreamError::LimitExceeded);
rust/crates/stream/src\wasm\provider_rpc.rs:407:                        MAX_COMMAND_BYTES
rust/crates/stream/src\session\mod.rs:75:const DATA_FRAME_BYTES: usize = crate::MAX_RECORD_BYTES + 128;
rust/crates/stream/src\session\mod.rs:97:            pending_frames: crate::MAX_ITEMS,
rust/crates/stream/src\session\mod.rs:98:            pending_bytes: crate::MAX_COMMAND_BYTES,
rust/crates/stream/src\session\mod.rs:317:        || limits.pending_bytes < crate::MAX_RECORD_BYTES + 128
rust/crates/stream/src\session\mod.rs:348:            bytes: crate::MAX_COMMAND_BYTES as u64,
rust/crates/stream/src\session\mod.rs:349:            frames: crate::MAX_ITEMS as u32,
rust/crates/stream/src\session\mod.rs:575:        if input.encoded_len() > crate::MAX_COMMAND_BYTES {
rust/crates/stream/src\session\mod.rs:576:            return Err(StreamError::LimitExceeded);
rust/crates/stream/src\session\mod.rs:627:                    receive_bytes: crate::MAX_COMMAND_BYTES as u64,
rust/crates/stream/src\session\mod.rs:628:                    receive_frames: u32::try_from(crate::MAX_ITEMS)
rust/crates/stream/src\session\mod.rs:729:                if bytes.is_empty() || bytes.len() > crate::MAX_RECORD_BYTES {
rust/crates/stream/src\session\mod.rs:730:                    return Err(StreamError::LimitExceeded);
rust/crates/stream/src\wasm\mod.rs:13:    MAX_COMMAND_BYTES, MemoryStream, StreamError, StreamProvider, StreamProviderExt as _, memory,
rust/crates/stream/src\session\client\pool.rs:254:                    .filter(|total| *total <= crate::MAX_COMMAND_BYTES)
rust/crates/stream/src\wasm\wasm_session.rs:38:        if credential.length() as usize > MAX_COMMAND_BYTES {
rust/crates/stream/src\wasm\wasm_session.rs:39:            return Err(js_error(StreamError::LimitExceeded));
rust/crates/stream/src\wasm\socket.rs:104:            let (sender, receiver) = mpsc::channel(crate::MAX_ITEMS);
rust/crates/stream/src\wasm\socket.rs:131:                    Ok(data) if data.byte_length() as usize > MAX_COMMAND_BYTES => {
rust/crates/stream/src\wasm\socket.rs:132:                        Some(StreamError::LimitExceeded)
rust/crates/stream/src\wasm\socket.rs:230:                                if bytes.len() > MAX_COMMAND_BYTES
rust/crates/stream/src\wasm\socket.rs:232:                                        > MAX_COMMAND_BYTES
rust/crates/stream/src\wire_codec\session\tests.rs:7:    let total = crate::MAX_RECORD_BYTES + 1;
rust/crates/stream/src\wire_codec\session\tests.rs:15:        item: Some(Item::ActorCall(vec![7; crate::MAX_RECORD_BYTES].into())),
rust/crates/stream/src\wire_codec\session\tests.rs:160:            Err(StreamError::LimitExceeded)
rust/crates/stream/src\session\sockets.rs:64:            if bytes.is_empty() || bytes.len() > crate::MAX_RECORD_BYTES {
rust/crates/stream/src\session\sockets.rs:65:                return Err(StreamError::LimitExceeded);
rust/crates/stream/src\wasm\session_server.rs:106:            if view.length() as usize > MAX_COMMAND_BYTES {
rust/crates/stream/src\wasm\session_server.rs:107:                return Err(StreamError::LimitExceeded);
rust/crates/stream/src\wasm\validation.rs:46:    if input.len() > MAX_COMMAND_BYTES {
rust/crates/stream/src\wasm\validation.rs:47:        Err(js_error(StreamError::LimitExceeded))
rust/crates/stream/src\wasm\validation.rs:72:        .map_err(|_| StreamError::LimitExceeded)
rust/crates/stream/src\wasm\validation.rs:106:    if input.len() > MAX_COMMAND_BYTES {
rust/crates/stream/src\wasm\validation.rs:125:        .map_err(|_| StreamError::LimitExceeded)
rust/crates/stream/src\wasm\validation.rs:543:                if excluded.len() > crate::MAX_ITEMS {
rust/crates/stream/src\wire_codec\session\resource.rs:41:    if resource.len() > 3 * crate::MAX_PATH_BYTES + "streams:/".len() {
rust/crates/stream/src\wire_codec\session\resource.rs:42:        return Err(StreamError::LimitExceeded);
rust/crates/stream/src\record_headers\prepared\controls\tests.rs:42:            maximum: crate::MAX_RECORD_BYTES,
rust/crates/stream/src\record_headers\prepared\controls\tests.rs:47:            maximum: crate::MAX_RECORD_BYTES,
rust/crates/stream/src\record_headers\prepared\controls\tests.rs:52:            maximum: crate::MAX_RECORD_BYTES,
rust/crates/stream/src\record_headers\prepared\controls\tests.rs:57:            maximum: crate::MAX_RECORD_BYTES,
rust/crates/stream/src\record_headers\prepared\controls\tests.rs:62:            maximum: crate::MAX_RECORD_BYTES,
rust/crates/stream/src\record_headers\prepared\controls\tests.rs:67:            maximum: crate::MAX_RECORD_BYTES,
rust/crates/stream/src\record_headers\prepared\controls\tests.rs:72:            maximum: crate::MAX_RECORD_BYTES,
rust/crates/stream/src\record_headers\prepared\controls\tests.rs:82:            maximum: crate::MAX_RECORD_BYTES,
rust/crates/stream/src\record_headers\prepared\controls\tests.rs:87:            maximum: crate::MAX_RECORD_BYTES,
rust/crates/stream/src\wire_codec\session\framing.rs:18:        Self::with_decoder(crate::MAX_COMMAND_BYTES, decode_frame)
rust/crates/stream/src\wire_codec\session\framing.rs:94:                return Err(StreamError::LimitExceeded);
rust/crates/stream/src\wire_codec\session\framing.rs:135:    if length > crate::MAX_COMMAND_BYTES {
rust/crates/stream/src\wire_codec\session\framing.rs:136:        return Err(StreamError::LimitExceeded);
rust/crates/stream/src\wire_codec\session\framing.rs:142:            .map_err(|_| StreamError::LimitExceeded)?
rust/crates/stream/src\wasm\session_io.rs:189:                if bytes.length() as usize > MAX_COMMAND_BYTES {
rust/crates/stream/src\wasm\session_io.rs:190:                    return Poll::Ready(Some(Err(StreamError::LimitExceeded.into())));
rust/crates/stream/src\session\client\link.rs:307:            > u32::try_from(crate::MAX_ITEMS / 2).map_err(|_| StreamError::Capacity)?
rust/crates/stream/src\session\client\link.rs:308:            && link.credit.bytes > (crate::MAX_COMMAND_BYTES / 2) as u64
rust/crates/stream/src\session\client\link.rs:483:    let sender = Arc::new(Outbound::new(crate::MAX_ITEMS, crate::MAX_COMMAND_BYTES));
rust/crates/stream/src\session\client\link.rs:485:        bytes: crate::MAX_COMMAND_BYTES as u64,
rust/crates/stream/src\session\client\link.rs:486:        frames: u32::try_from(crate::MAX_ITEMS).map_err(|_| StreamError::Capacity)?,
rust/crates/stream/src\session\client\link.rs:514:    if bytes > crate::MAX_COMMAND_BYTES {
rust/crates/stream/src\session\client\link.rs:515:        return Err(StreamError::LimitExceeded.into());
rust/crates/stream/src\wasm\tests\cases_0.rs:72:    for _ in 0..crate::MAX_ITEMS {
rust/crates/stream/src\wasm\tests\cases_0.rs:170:        source: "a".repeat(crate::MAX_PATH_BYTES + 1),
rust/crates/stream/src\memory\tests\key_seek.rs:153:            for page in records.chunks(crate::MAX_ITEMS) {
rust/crates/stream/src\session\client\pool\fixture.rs:76:                    bytes: crate::MAX_COMMAND_BYTES as u64,
rust/crates/stream/src\session\client\pool\fixture.rs:86:                sender: Arc::new(Outbound::new(4, crate::MAX_COMMAND_BYTES)),
rust/crates/stream/src\session\client\pool\fixture.rs:148:            bytes: crate::MAX_COMMAND_BYTES as u64,
rust/crates/stream/src\wire_codec\requests.rs:260:    let field_overhead = 1 + prost::length_delimiter_len(crate::MAX_COMMAND_BYTES);
rust/crates/stream/src\wire_codec\requests.rs:491:            for length in [0, 127, 128, 16_383, 16_384, crate::MAX_RECORD_BYTES] {
rust/crates/stream/src\wire_codec\requests.rs:610:            vec![OwnedBytes::from(vec![0; crate::MAX_RECORD_BYTES + 1])],
rust/crates/stream/src\wire_codec\requests.rs:611:            vec![OwnedBytes::from_static(b""); crate::MAX_ITEMS + 1],
rust/crates/stream/src\wire_codec\requests.rs:613:                OwnedBytes::from(vec![0; crate::MAX_RECORD_BYTES]);
rust/crates/stream/src\wire_codec\requests.rs:614:                crate::MAX_COMMAND_BYTES / crate::MAX_RECORD_BYTES + 1
rust/crates/stream/src\wire_codec\requests.rs:681:    let mut maximum = 2 * crate::MAX_COMMAND_BYTES + 64;
rust/crates/stream/src\wire_codec\requests.rs:692:            .ok_or(crate::StreamError::LimitExceeded)?;
rust/crates/stream/src\session\client\pool\ordering.rs:530:    let maximum = Bytes::from(vec![1; crate::MAX_COMMAND_BYTES]);
rust/crates/stream/src\session\client\pool\ordering.rs:547:    prop_assert_eq!(pool.credential_bytes, crate::MAX_COMMAND_BYTES);
rust/crates/stream/src\wasm\tests\accepted_reply.rs:96:                            crate::MAX_ITEMS
rust/crates/stream/src\wasm\tests\accepted_reply.rs:106:        assert!(wire.encoded_len() <= MAX_COMMAND_BYTES);
rust/crates/stream/src\wasm\tests\accepted_reply.rs:110:        assert!(reply_bytes.get() > MAX_COMMAND_BYTES);
rust/crates/stream/src\wasm\tests\accepted_reply.rs:128:            assert_eq!(provider.tail(path).await.unwrap(), crate::MAX_ITEMS as u64);
rust/crates/stream/src\session\client\pool\route_searches.rs:66:    let sender = Arc::new(Outbound::new(2 * n + 4, crate::MAX_COMMAND_BYTES));
rust/crates/stream/src\wire_codec\records.rs:80:        return Err(StreamError::LimitExceeded);
rust/crates/stream/src\wire_codec\records.rs:120:    if record.payload_bytes() > crate::MAX_RECORD_BYTES {
rust/crates/stream/src\wire_codec\records.rs:121:        return Err(StreamError::LimitExceeded);
rust/crates/stream/src\wire_codec\records.rs:153:        .filter(|length| *length <= crate::MAX_COMMAND_BYTES)
rust/crates/stream/src\memory\tests\cases_0.rs:2:use crate::MAX_COMMAND_BYTES;
rust/crates/stream/src\memory\tests\cases_0.rs:85:    let records = vec![crate::AppendRecord::from(vec![0_u8; MAX_RECORD_BYTES]); 16];
rust/crates/stream/src\memory\tests\cases_0.rs:88:    let mut high = crate::MAX_PATH_BYTES;
rust/crates/stream/src\memory\tests\cases_0.rs:102:        if wire.encoded_len() <= MAX_COMMAND_BYTES {
rust/crates/stream/src\memory\tests\cases_0.rs:127:    assert!(next_wire.encoded_len() > MAX_COMMAND_BYTES);
rust/crates/stream/src\memory\tests\cases_0.rs:479:            u64::try_from(records.len()).map_err(|_| StreamError::LimitExceeded)?,
rust/crates/stream/src\memory\tests\cases_0.rs:541:    assert!(StreamPath::new("a".repeat(crate::MAX_PATH_BYTES)).is_ok());
rust/crates/stream/src\memory\tests\cases_0.rs:543:        StreamPath::new("a".repeat(crate::MAX_PATH_BYTES + 1)),
rust/crates/stream/src\wasm\remote.rs:20:            tonic_web_wasm_client::limits::UnaryResponseLimits::new(MAX_COMMAND_BYTES)
rust/crates/stream/src\wasm\remote.rs:46:        .max_encoding_message_size(MAX_COMMAND_BYTES)
rust/crates/stream/src\wasm\remote.rs:203:                        .ok_or(StreamError::LimitExceeded)?
rust/crates/stream/src\session\tests\sockets.rs:437:        Bytes::from(vec![2; crate::MAX_RECORD_BYTES]),
rust/crates/stream/src\session\client\tests.rs:469:    append(&provider, "a", crate::MAX_ITEMS).await?;
rust/crates/stream/src\session\client\tests.rs:471:    for sequence in 1..=u64::try_from(crate::MAX_ITEMS + 17)? {
rust/crates/stream/src\session\client\tests.rs:508:    for sequence in 1..=crate::MAX_ITEMS + 8 {
rust/crates/stream/src\session\client\tests.rs:533:        (crate::MAX_ITEMS + 9) as u64
rust/crates/stream/src\memory\snapshot\tests.rs:272:                u64::try_from(index).map_err(|_| StreamError::LimitExceeded)?
rust/crates/stream/src\memory\snapshot\tests.rs:379:        let tail = u64::try_from(expected.len()).map_err(|_| StreamError::LimitExceeded)?;
rust/crates/stream/src\memory\provider.rs:302:    if path.as_str().len() + 1 + relative.as_str().len() > crate::MAX_PATH_BYTES {
rust/crates/stream/src\memory\provider.rs:441:            if page_bytes > crate::MAX_COMMAND_BYTES {
rust/crates/stream/src\memory\provider.rs:442:                return Err(StreamError::LimitExceeded);
rust/crates/stream/src\memory\provider.rs:899:                .ok_or(StreamError::LimitExceeded)?;
rust/crates/stream/src\session\tests\properties.rs:144:        bytes: crate::MAX_COMMAND_BYTES as u64,
rust/crates/stream/src\session\tests\calls.rs:434:    let original = Bytes::from(vec![7; crate::MAX_RECORD_BYTES]);
rust/crates/stream/src\session\tests\calls.rs:673:                crate::MAX_RECORD_BYTES + 7
rust/crates/stream/src\session\tests\calls.rs:720:    let body = Bytes::from(vec![9; 3 * crate::MAX_RECORD_BYTES + 7]);
rust/crates/stream/src\session\tests\calls.rs:788:    assert_eq!(first.1.as_ref(), &body[..crate::MAX_RECORD_BYTES]);
rust/crates/stream/src\session\tests\calls.rs:791:        &body[crate::MAX_RECORD_BYTES..2 * crate::MAX_RECORD_BYTES]
rust/crates/stream/src\memory\prepared.rs:117:        range.tail = expected.checked_add(1).ok_or(StreamError::LimitExceeded)?;
rust/crates/stream/src\memory\prepared.rs:120:        fact.end = expected.checked_add(1).ok_or(StreamError::LimitExceeded)?;
rust/crates/stream/src\memory\prepared.rs:612:        return Err(StreamError::LimitExceeded);
rust/crates/stream/src\memory\prepared.rs:618:        return Err(StreamError::LimitExceeded);
rust/crates/stream/src\memory\prepared.rs:634:        return Err(StreamError::LimitExceeded);
rust/crates/stream/src\memory\prepared.rs:656:    if maximum > MAX_RECORD_BYTES {
rust/crates/stream/src\memory\prepared.rs:657:        return Err(StreamError::LimitExceeded);
rust/crates/stream/src\wasm\stream_error_code.rs:7:    LimitExceeded,
rust/crates/stream/src\memory\mod.rs:13:use crate::MAX_RECORD_BYTES;
rust/crates/stream/src\memory\mod.rs:41:    ForkRequest, IdempotencyKey, IdempotencyObservation, IdempotencyOutcome, MAX_ITEMS,
rust/crates/stream/src\session\tests\queue_scaling.rs:59:            pending_bytes: crate::MAX_COMMAND_BYTES,
rust/crates/stream/src\session\tests\queue_scaling.rs:100:                bytes: crate::MAX_COMMAND_BYTES as u64,
rust/crates/stream/src\memory\snapshot\mod.rs:375:    crate::MAX_COMMAND_BYTES
rust/crates/stream/src\memory\snapshot\mod.rs:378:            crate::MAX_ITEMS
rust/crates/stream/src\memory\snapshot\mod.rs:379:                .checked_mul(crate::MAX_PATH_BYTES.checked_mul(2)?.checked_add(128)?)?,
rust/crates/stream/src\session\client\socket.rs:28:        if open.is_empty() || open.len() > crate::MAX_COMMAND_BYTES {
rust/crates/stream/src\session\client\socket.rs:95:        if frame.is_empty() || frame.len() > crate::MAX_RECORD_BYTES {
rust/crates/stream/src\wire_codec\read_response_tests\cases_0.rs:72:    let maximum = crate::MAX_COMMAND_BYTES as u64;
rust/crates/stream/src\wire_codec\provider_cursor.rs:25:    if input.len() > crate::MAX_COMMAND_BYTES {
rust/crates/stream/src\wire_codec\provider_cursor.rs:26:        return Err(StreamError::LimitExceeded);
rust/crates/stream/src\session\client\tests\application.rs:191:            Err(StreamError::LimitExceeded),
rust/crates/stream/src\session\client\tests\application.rs:196:            Err(StreamError::LimitExceeded),
rust/crates/stream/src\session\client\tests\reads.rs:378:    assert!(matches!(outcome, Some(Err(StreamError::LimitExceeded))));
rust/crates/stream/src\session\client\tests\reads.rs:773:            let oversized = StreamPath::new("x".repeat(crate::MAX_PATH_BYTES))?;
rust/crates/stream/src\session\client\tests\credential_progress.rs:164:                    receive_bytes: crate::MAX_COMMAND_BYTES as u64,
rust/crates/stream/src\session\client\tests\credential_progress.rs:165:                    receive_frames: crate::MAX_ITEMS as u32,
rust/crates/stream/src\session\client\tests\outbound.rs:46:                receive_bytes: crate::MAX_COMMAND_BYTES as u64,
rust/crates/stream/src\session\client\tests\outbound.rs:47:                receive_frames: u32::try_from(crate::MAX_ITEMS)
rust/crates/stream/src\session\client\tests\credentials.rs:38:                receive_bytes: crate::MAX_COMMAND_BYTES as u64,
rust/crates/stream/src\session\client\tests\credentials.rs:39:                receive_frames: crate::MAX_ITEMS as u32,
```
