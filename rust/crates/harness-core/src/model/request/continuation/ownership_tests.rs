//! Physical continuation storage and its original allocation custody.
use super::*;

fn check_message_growth(io: Option<&PreparationIo>) -> Result<()> {
    let mut prepared = PreparedModelRequest::prepare(
        crate::model::clone_request(&request(1)?, io)?,
        Limits::default(),
        io,
    )?;
    let mut relocated = 0usize;
    let mut allocations = 0usize;
    for step in 1..=1_000 {
        let old_len = prepared.request.messages.len();
        let old_capacity = prepared.request.messages.capacity();
        let mut continuation = prepared.into_continuation();
        crate::executor::stock_vec_push(
            continuation.messages(),
            ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text(crate::executor::stock_string_copy("new text", io)?),
            },
            io,
        )?;
        prepared = continuation.finish(Limits::default(), io)?;
        if prepared.request.messages.capacity() != old_capacity {
            allocations += 1;
            relocated += old_len;
        }
        assert!(
            relocated < 2 * prepared.request.messages.len(),
            "message growth relocated the prefix on every step at {step}"
        );
    }
    assert!(
        allocations <= 11,
        "message slots did not grow geometrically"
    );
    Ok(())
}

#[test]
fn message_slots_grow_geometrically_instead_of_relocating_every_step() -> Result<()> {
    check_message_growth(None)?;
    for native in [false, true] {
        let measured =
            crate::test_support::accounted::accounted(native, 4 * 1024 * 1024, false, |io| {
                check_message_growth(Some(io))
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

#[test]
fn an_empty_continuation_reuses_its_existing_request_header() -> Result<()> {
    for native in [false, true] {
        for count in [1, 10, 100, 1_000] {
            let measured =
            crate::test_support::accounted::accounted(native, 4 * 1024 * 1024, false, |io| {
                let mut original = request(count)?;
                original.tools = tools();
                original.tools[0].description = "retained description ".repeat(count);
                let expected = crate::contract::canonical_json_bytes(&original)?;
                let mut prepared = PreparedModelRequest::prepare(
                    crate::model::clone_request(&original, Some(io))?,
                    Limits::default(),
                    Some(io),
                )?;
                // Keep the original first hash cut for later transcript staging.
                prepared = prepared.into_continuation().finish(Limits::default(), Some(io))?;
                assert!(prepared.cached_message_digests(None, Some(io))?.is_some());
                let pointer = prepared.bytes().as_ptr();
                let digest = prepared.request_digest();
                let inventory = prepared.preparation_owned_bytes(io)?;
                let owned = io.owned_bytes()?;
                let receipt = io.receipt();
                for step in 1..=1_000 {
                    prepared = prepared
                        .into_continuation()
                        .finish(Limits::default(), Some(io))?;
                    assert_eq!(prepared.bytes().as_ptr(), pointer);
                    assert_eq!(prepared.request_digest(), digest);
                    assert_eq!(prepared.preparation_owned_bytes(io)?, inventory);
                    assert_eq!(io.receipt(), receipt, "unchanged request repeated preparation work");
                    assert_eq!(
                        io.owned_bytes()?,
                        owned,
                        "continuation admitted its request header again at {step}, native={native}"
                    );
                }
                assert_eq!(prepared.bytes(), expected);
                // The former empty append still encodes the whole tool envelope.
                let _previous = encoding::EncodingPrefix::append(
                    prepared.bytes, prepared.encoding, count, &prepared.request, Some(io), &mut 0,
                )?;
                assert!(io.receipt().bytes_encoded > receipt.bytes_encoded,
                    "former empty append did not expose repeated encoding");
                Ok::<_, Error>(())
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
    }
    Ok(())
}

#[test]
fn continuations_observe_original_cancellation_before_reusing_declarations() -> Result<()> {
    for native in [false, true] {
        for append in [false, true] {
            let cancellation = acyclic_fs::CancellationToken::new();
            let budget = acyclic_fs::performance::work_budget(1_000_000, 4 * 1024 * 1024);
            let work = native
                .then(|| acyclic_fs::NativeMaterializationWork::new(budget, &cancellation))
                .transpose()
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let baseline = work
                .as_ref()
                .map(acyclic_fs::NativeMaterializationWork::live_allocation_bytes);
            let io = match &work {
                Some(work) => PreparationIo::from_native_work(work)?,
                None => PreparationIo::from_budget(budget, &cancellation)?,
            };
            let mut original = request(1)?;
            original.tools = tools();
            let prepared = PreparedModelRequest::prepare(
                crate::model::clone_request(&original, Some(&io))?,
                Limits::default(),
                Some(&io),
            )?;
            let prepared = prepared
                .into_continuation()
                .finish(Limits::default(), Some(&io))?;
            let mut continuation = prepared.into_continuation();
            if append {
                crate::executor::stock_vec_push(
                    continuation.messages(),
                    ModelMessage {
                        role: ModelRole::User,
                        content: ModelContent::Text(crate::executor::stock_string_copy(
                            "new text",
                            Some(&io),
                        )?),
                    },
                    Some(&io),
                )?;
            }
            cancellation.cancel();
            let failure = continuation
                .finish(Limits::default(), Some(&io))
                .err()
                .ok_or_else(|| {
                    Error::Invalid("request continuation ignored cancellation".into())
                })?;
            assert_eq!(failure.code(), crate::wire::ErrorCode::Cancelled);
            drop(failure);
            drop(io);
            assert_eq!(
                work.as_ref()
                    .map(acyclic_fs::NativeMaterializationWork::live_allocation_bytes),
                baseline
            );
        }
    }
    Ok(())
}

#[test]
fn continuation_retires_dead_suffix_slots_and_retains_only_its_live_storage() -> Result<()> {
    for native in [false, true] {
        let measured =
            crate::test_support::accounted::accounted(native, 4 * 1024 * 1024, false, |io| {
                let mut prepared = PreparedModelRequest::prepare(
                    crate::model::clone_request(&request(1)?, Some(io))?,
                    Limits::default(),
                    Some(io),
                )?;
                for step in 1..=1_000 {
                    let before_owned = io.owned_bytes()?;
                    let before_inventory = prepared.preparation_owned_bytes(io)?;
                    let mut continuation = prepared.into_continuation();
                    crate::executor::stock_vec_push(
                        continuation.messages(),
                        ModelMessage {
                            role: ModelRole::User,
                            content: ModelContent::Text(crate::executor::stock_string_copy(
                                "new text",
                                Some(io),
                            )?),
                        },
                        Some(io),
                    )?;
                    prepared = continuation.finish(Limits::default(), Some(io))?;
                    let live_growth = prepared.preparation_owned_bytes(io)? - before_inventory;
                    assert_eq!(
                        io.owned_bytes()?,
                        before_owned + live_growth,
                        "continuation retained dead suffix storage at {step}, native={native}"
                    );
                }
                Ok::<_, Error>(())
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
