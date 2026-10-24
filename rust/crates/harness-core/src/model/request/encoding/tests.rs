use super::*;

fn message(index: usize) -> ModelMessage {
    ModelMessage {
        role: ModelRole::User,
        content: ModelContent::Text(format!("message {index}: escaped \\\" ] , {{}}\n")),
    }
}

fn request(count: usize) -> Result<ModelRequest> {
    Ok(ModelRequest {
        model: Model::new(
            "scripted".into(),
            "encoding".into(),
            "1".into(),
            serde_json::json!({"quoted": "\"messages\":[]", "nested": [{"z": 2, "a": 1}]}),
        )?,
        messages: (0..count).map(message).collect(),
        tools: Vec::new(),
        max_output_tokens: Some(17),
    })
}

fn push(request: &mut ModelRequest, index: usize, io: Option<&PreparationIo>) -> Result<()> {
    let ModelContent::Text(text) = message(index).content else {
        unreachable!()
    };
    let message = ModelMessage {
        role: ModelRole::User,
        content: ModelContent::Text(crate::executor::stock_string_copy(&text, io)?),
    };
    crate::executor::stock_vec_push(&mut request.messages, message, io)
}

fn exercise(io: Option<&PreparationIo>) -> Result<()> {
    for count in [1, 10, 100, 1_000] {
        let prepared = PreparedModelRequest::prepare(
            crate::model::clone_request(&request(count)?, io)?,
            Limits::default(),
            io,
        )?;
        let mut request = prepared.request;
        let mut bytes = prepared.bytes;
        let mut state = Vec::new();
        for added in 0..5 {
            let previous = request.messages.len();
            push(&mut request, previous, io)?;
            let (next, digest, next_state) =
                EncodingPrefix::append(bytes, state, previous, &request, io)?;
            bytes = next;
            state = next_state;
            let expected = crate::contract::canonical_json_bytes(&request)?;
            assert_eq!(
                bytes.as_slice(),
                expected,
                "canonical request changed at {count}+{added}"
            );
            assert_eq!(digest, *blake3::hash(&expected).as_bytes());
            assert_eq!(
                state[0].messages_digest(io)?,
                *blake3::hash(&crate::contract::canonical_json_bytes(&request.messages)?)
                    .as_bytes()
            );
        }
    }
    Ok(())
}

#[test]
fn suffix_encoding_preserves_canonical_request_and_array_digests() -> Result<()> {
    exercise(None)
}

#[test]
fn a_shared_encoding_is_copied_without_changing_its_immutable_peer() -> Result<()> {
    let prepared = PreparedModelRequest::prepare(request(10)?, Limits::default(), None)?;
    let original = prepared.bytes.clone();
    let expected_original = original.to_vec();
    let mut request = prepared.request;
    push(&mut request, 10, None)?;
    let (bytes, digest, state) =
        EncodingPrefix::append(prepared.bytes, Vec::new(), 10, &request, None)?;
    assert_eq!(original.as_slice(), expected_original);
    assert_eq!(
        bytes.as_slice(),
        crate::contract::canonical_json_bytes(&request)?
    );
    assert_eq!(digest, *blake3::hash(&bytes).as_bytes());
    assert!(state.is_empty(), "shared storage needs a fresh private cut");
    Ok(())
}

#[test]
fn continuation_finish_keeps_the_hash_cut_bound_to_its_canonical_bytes() -> Result<()> {
    for count in [1, 10, 100, 1_000] {
        let mut prepared = PreparedModelRequest::prepare(request(count)?, Limits::default(), None)?;
        for added in 0..5 {
            let previous = prepared.request().messages.len();
            let mut continued = prepared.into_continuation();
            continued.messages().push(message(added));
            prepared = continued.finish(Limits::default(), None)?;
            let expected = crate::contract::canonical_json_bytes(prepared.request())?;
            assert_eq!(prepared.bytes(), expected);
            assert_eq!(
                prepared.request_digest(),
                *blake3::hash(&expected).as_bytes()
            );
            let digests = prepared
                .cached_message_digests(Some(previous), None)?
                .ok_or_else(|| Error::Invalid("continuation lost its hash cut".into()))?;
            assert_eq!(
                digests.0,
                *blake3::hash(&crate::contract::canonical_json_bytes(
                    &prepared.request().messages
                )?,)
                .as_bytes()
            );
            assert_eq!(
                digests.1,
                Some(
                    *blake3::hash(&crate::contract::canonical_json_bytes(
                        &prepared.request().messages[..previous]
                    )?,)
                    .as_bytes()
                )
            );
        }
        let peer = prepared.clone();
        let original = peer.bytes().to_vec();
        let mut continued = prepared.into_continuation();
        continued.messages().push(message(7));
        let prepared = continued.finish(Limits::default(), None)?;
        assert_eq!(peer.bytes(), original);
        assert_ne!(prepared.bytes(), peer.bytes());
        assert_eq!(
            prepared.bytes(),
            crate::contract::canonical_json_bytes(prepared.request())?
        );
        assert!(
            prepared.encoding.is_empty(),
            "shared bytes must invalidate their private hash cut"
        );
        let mut continued = prepared.into_continuation();
        continued.messages().push(message(8));
        let changed = Limits {
            render_bytes: Limits::default().render_bytes - 1,
            ..Limits::default()
        };
        let prepared = continued.finish(changed, None)?;
        assert!(
            prepared.encoding.is_empty(),
            "changed admission limits require a fresh encoding"
        );
        assert_eq!(
            prepared.bytes(),
            crate::contract::canonical_json_bytes(prepared.request())?
        );
    }
    Ok(())
}

#[cfg(all(
    feature = "filesystem",
    feature = "test-support",
    not(target_arch = "wasm32")
))]
#[test]
fn every_encoding_ledger_keeps_original_custody_through_growth_and_retirement() -> Result<()> {
    for native in [false, true] {
        let measured =
            crate::test_support::accounted::accounted(native, 4 * 1024 * 1024, false, |io| {
                exercise(Some(io))
            })
            .map_err(|(cause, _)| Error::Storage(cause))?;
        measured.0?;
        if native {
            assert_eq!(
                measured.5,
                acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
            );
        }
    }
    Ok(())
}

#[cfg(all(
    feature = "filesystem",
    feature = "test-support",
    not(target_arch = "wasm32")
))]
#[test]
fn native_prepared_inventory_includes_sparse_maps_and_cached_hash_storage() -> Result<()> {
    let measured = crate::test_support::accounted::accounted(
        true,
        4 * 1024 * 1024,
        false,
        |io| -> Result<()> {
            let prepared = PreparedModelRequest::prepare(
                crate::model::clone_request(&request(10)?, Some(io))?,
                Limits::default(),
                Some(io),
            )?;
            assert!(prepared.request.retained_payload_bytes()? > prepared.request.payload_bytes()?);
            assert_eq!(
                prepared.logical_owned_bytes()?,
                prepared.preparation_owned_bytes(io)?,
                "native output omitted retained sparse-map slots"
            );
            let mut continuation = prepared.into_continuation();
            let ModelContent::Text(text) = message(10).content else {
                unreachable!()
            };
            let next = ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text(crate::executor::stock_string_copy(&text, Some(io))?),
            };
            crate::executor::stock_vec_push(continuation.messages(), next, Some(io))?;
            let prepared = continuation.finish(Limits::default(), Some(io))?;
            assert!(!prepared.encoding.is_empty());
            assert_eq!(
                prepared.logical_owned_bytes()?,
                prepared.preparation_owned_bytes(io)?,
                "native output omitted its incremental hash storage"
            );
            Ok(())
        },
    )
    .map_err(|(cause, _)| Error::Storage(cause))?;
    measured.0?;
    assert_eq!(
        measured.5,
        acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
    );
    Ok(())
}

#[cfg(all(
    feature = "filesystem",
    feature = "test-support",
    not(target_arch = "wasm32")
))]
type SuffixWork = (u64, u64, u64, u64);

#[cfg(all(
    feature = "filesystem",
    feature = "test-support",
    not(target_arch = "wasm32")
))]
fn measure_suffix(count: usize, io: &PreparationIo) -> Result<(SuffixWork, u64)> {
    let prepared = PreparedModelRequest::prepare(
        crate::model::clone_request(&request(count)?, Some(io))?,
        Limits::default(),
        Some(io),
    )?;
    let mut request = prepared.request;
    push(&mut request, 0, Some(io))?;
    let (bytes, _, state) =
        EncodingPrefix::append(prepared.bytes, Vec::new(), count, &request, Some(io))?;
    // Isolate suffix work from geometric capacity growth, which is separately
    // checked with real relocation admission by Streams.
    let mut buffer = Buffer::new(bytes, Some(io)).unwrap_or_else(|_| unreachable!());
    buffer.reserve(buffer.capacity() + 4_096, Some(io))?;
    let bytes = buffer.freeze();
    let previous = request.messages.len();
    push(&mut request, 0, Some(io))?;
    let before = io.receipt();
    let (bytes, _, state) = EncodingPrefix::append(bytes, state, previous, &request, Some(io))?;
    let after = io.receipt();
    let suffix = (
        after.bytes_encoded - before.bytes_encoded,
        after.bytes_hashed - before.bytes_hashed,
        after.bytes_copied - before.bytes_copied,
        after.items_examined - before.items_examined,
    );
    let before = io.receipt();
    let full = PreparedModelRequest::prepare(request, Limits::default(), Some(io))?;
    let full_bytes = io.receipt().bytes_encoded - before.bytes_encoded;
    assert_eq!(bytes.as_slice(), full.bytes());
    assert!(state[0].end < bytes.len());
    Ok((suffix, full_bytes))
}

#[cfg(all(
    feature = "filesystem",
    feature = "test-support",
    not(target_arch = "wasm32")
))]
#[test]
fn admitted_suffix_encoding_and_hashing_do_not_revisit_historical_messages() -> Result<()> {
    for native in [false, true] {
        let mut expected = None;
        let mut full_encoding = Vec::new();
        for count in [1, 10, 100, 1_000] {
            let measured =
                crate::test_support::accounted::accounted(native, 4 * 1024 * 1024, false, |io| {
                    measure_suffix(count, io)
                })
                .map_err(|(cause, _)| Error::Storage(cause))?;
            let (suffix, full_bytes) = measured.0?;
            assert_eq!(
                *expected.get_or_insert(suffix),
                suffix,
                "incremental encoding revisited {count} historical messages, native={native}"
            );
            full_encoding.push(full_bytes);
            if native {
                assert_eq!(
                    measured.5,
                    acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
                );
            }
            eprintln!(
                "CONTINUATION_ENCODING native={native} messages={count} suffix={suffix:?} full_bytes={full_bytes}"
            );
        }
        assert!(
            full_encoding.windows(2).all(|pair| pair[1] > pair[0]),
            "full encoding negative control did not grow"
        );
    }
    Ok(())
}
