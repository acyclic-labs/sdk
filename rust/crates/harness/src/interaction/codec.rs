use super::*;

pub(super) fn decode_interaction_json<'loan, 'io, T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    schema: &'static crate::executor::json::JsonValueSchema,
    typed_slot: u64,
    work: &'loan mut Option<&'io mut dyn crate::store::history::HistoryIo>,
) -> Result<(T, crate::wire_codec::CodecQuota<'loan, 'io>)> {
    #[cfg(test)]
    {
        let mut decoded = None;
        let decode = move || {
            let work = work;
            decode_interaction_json_inner::<T>(bytes, schema, typed_slot, work)
        };
        let allocation = acyclic_test_support::measure(|| {
            decoded = Some(decode());
        });
        if std::any::type_name::<T>() == std::any::type_name::<ApprovalDecisionFile>() {
            acyclic_test_support::differential::record((
                "decision-decode-allocations",
                allocation.count_total,
            ));
            acyclic_test_support::differential::record((
                "decision-decode-bytes",
                allocation.bytes_total,
            ));
        }
        decoded.expect("decoder completed")
    }
    #[cfg(not(test))]
    decode_interaction_json_inner(bytes, schema, typed_slot, work)
}

fn decode_interaction_json_inner<'loan, 'io, T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    schema: &'static crate::executor::json::JsonValueSchema,
    typed_slot: u64,
    work: &'loan mut Option<&'io mut dyn crate::store::history::HistoryIo>,
) -> Result<(T, crate::wire_codec::CodecQuota<'loan, 'io>)> {
    if work.is_none() {
        let value = crate::contract::json_from_slice(bytes)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        return Ok((value, crate::wire_codec::CodecQuota::new(work, 0)?));
    }
    let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
    // Request/response Serde labels, including UUID and RawValue errors.
    let diagnostic =
        crate::executor::json::audited_serde_diagnostic_bytes(bytes.len() as u64, 1477)?;
    let mut items = 0_u64;
    let native = custody
        .work()
        .as_deref()
        .is_some_and(original_native_decode_mode);
    let bounds = crate::executor::json::JsonBounds {
        maximum_bytes: bytes.len() as u64,
        maximum_items: bytes.len() as u64,
        maximum_depth: 256,
    };
    let decoded = {
        let mut charge = |charge| -> Result<()> {
            match charge {
                crate::executor::json::JsonCodecCharge::Work { items: visited, .. } => {
                    items = items.checked_add(visited).ok_or_else(|| {
                        boundary(
                            custody.work(),
                            Invalid,
                            "interaction JSON item count overflows",
                        )
                    })?;
                }
                crate::executor::json::JsonCodecCharge::Admission {
                    maximum_owned_bytes,
                    value_shape,
                } => {
                    let value_owned =
                        crate::executor::json::json_value_decode_owned_bytes(value_shape)?;
                    let content_slot =
                        std::mem::size_of::<serde::__private229::de::Content<'static>>().max(
                            std::mem::size_of::<(
                                serde::__private229::de::Content<'static>,
                                serde::__private229::de::Content<'static>,
                            )>(),
                        ) as u64;
                    let planned = content_slot
                        .checked_add(typed_slot)
                        .and_then(|slot| items.checked_mul(10)?.checked_mul(slot))
                        .and_then(|owned| owned.checked_add(value_owned))
                        .and_then(|owned| owned.checked_add(maximum_owned_bytes))
                        .and_then(|owned| owned.checked_add(diagnostic))
                        .ok_or_else(|| {
                            boundary(
                                custody.work(),
                                Invalid,
                                "interaction JSON custody overflows",
                            )
                        })?;
                    let observer = custody.observer()?;
                    observer.admit_owned(planned)?;
                    observer.retain_owned(planned)?;
                    custody.adopt(planned)?;
                    return Ok(());
                }
                _ => {}
            }
            custody.observer()?.codec_charge(charge)
        };
        crate::executor::json::json_from_slice_bounded(
            std::marker::PhantomData::<T>,
            bytes,
            bounds,
            bytes.len() as u64,
            native.then_some(Invalid),
            native.then_some(schema),
            &mut charge,
        )
    };
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let decoded = decoded.map_err(|error| custody.json_error(error));
    let value = decoded?;
    Ok((value, custody))
}

pub(super) fn interaction_request_owned_bytes(
    request: &Interaction,
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<u64> {
    let overflow = || boundary(work, Invalid, "interaction DTO ownership overflows");
    match request {
        Interaction::Question {
            prompt,
            response_schema,
        } => (prompt.capacity() as u64)
            .checked_add(crate::executor::json::json_value_logical_owned_bytes(
                response_schema,
            )?)
            .ok_or_else(overflow),
        Interaction::Form { title, schema } => (title.capacity() as u64)
            .checked_add(crate::executor::json::json_value_logical_owned_bytes(
                schema,
            )?)
            .ok_or_else(overflow),
        Interaction::Choice {
            prompt, options, ..
        } => {
            let slots = options
                .capacity()
                .checked_mul(std::mem::size_of::<ChoiceOption>())
                .ok_or_else(overflow)? as u64;
            let mut owned = (prompt.capacity() as u64)
                .checked_add(slots)
                .ok_or_else(overflow)?;
            for option in options {
                owned = owned
                    .checked_add(option.id.capacity() as u64)
                    .and_then(|bytes| bytes.checked_add(option.label.capacity() as u64))
                    .and_then(|bytes| {
                        bytes.checked_add(
                            option
                                .description
                                .as_ref()
                                .map_or(0, |description| description.capacity() as u64),
                        )
                    })
                    .ok_or_else(overflow)?;
            }
            Ok(owned)
        }
        Interaction::Approval { prompt, .. } => Ok(prompt.capacity() as u64),
    }
}
