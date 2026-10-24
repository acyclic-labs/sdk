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
        let measured =
            crate::test_support::accounted::accounted(native, 4 * 1024 * 1024, false, |io| {
                let prepared = PreparedModelRequest::prepare(
                    crate::model::clone_request(&request(1)?, Some(io))?,
                    Limits::default(),
                    Some(io),
                )?;
                // The first continuation installs its one hash-state allocation.
                let mut prepared = prepared
                    .into_continuation()
                    .finish(Limits::default(), Some(io))?;
                let owned = io.owned_bytes()?;
                for step in 1..=1_000 {
                    prepared = prepared
                        .into_continuation()
                        .finish(Limits::default(), Some(io))?;
                    assert_eq!(
                        io.owned_bytes()?,
                        owned,
                        "continuation admitted its request header again at {step}, native={native}"
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
