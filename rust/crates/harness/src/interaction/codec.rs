use super::*;

pub(super) fn decode_interaction_json<'loan, 'io, T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    schema: crate::contract::JsonValueSchema,
    typed_slot: u64,
    work: &'loan mut Option<&'io mut dyn crate::store::history::HistoryIo>,
) -> Result<(T, crate::wire_codec::CodecQuota<'loan, 'io>)> {
    if work.is_none() {
        let value = crate::contract::json_from_slice(bytes)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        return Ok((value, crate::wire_codec::CodecQuota::new(work, 0)?));
    }
    let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
    let diagnostic = interaction_decode_diagnostic_bytes(bytes.len() as u64)?;
    let mut items = 0_u64;
    let native = interaction_native_mode(custody.work());
    let bounds = crate::contract::JsonBounds {
        maximum_bytes: bytes.len() as u64,
        maximum_items: bytes.len() as u64,
        maximum_depth: 256,
    };
    let decoded = {
        let mut charge = |charge| -> Result<()> {
            match charge {
                crate::contract::JsonCodecCharge::Work { items: visited, .. } => {
                    items = items.checked_add(visited).ok_or_else(|| {
                        interaction_denial(
                            custody.work(),
                            crate::wire::ErrorCode::Invalid,
                            "interaction JSON item count overflows",
                        )
                    })?;
                }
                crate::contract::JsonCodecCharge::Admission {
                    maximum_owned_bytes,
                    value_shape,
                } => {
                    let value_owned = crate::contract::json_value_decode_owned_bytes(value_shape)?;
                    let planned = interaction_decode_owned_bytes(
                        items,
                        typed_slot,
                        maximum_owned_bytes,
                        value_owned,
                        diagnostic,
                    )
                    .ok_or_else(|| {
                        interaction_denial(
                            custody.work(),
                            crate::wire::ErrorCode::Invalid,
                            "interaction JSON custody overflows",
                        )
                    })?;
                    let observer = custody.work().as_deref_mut().expect("observed interaction");
                    observer.admit_owned(planned)?;
                    observer.retain_owned(planned)?;
                    custody.adopt(planned)?;
                    return Ok(());
                }
                _ => {}
            }
            custody
                .work()
                .as_deref_mut()
                .expect("observed interaction")
                .codec_charge(charge)
        };
        crate::contract::json_from_slice_bounded(
            std::marker::PhantomData::<T>,
            bytes,
            bounds,
            bytes.len() as u64,
            native.then_some(crate::wire::ErrorCode::Invalid),
            native.then_some(schema),
            &mut charge,
        )
    };
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let decoded = decoded.map_err(|mut error| {
        if let Error::NativeDecode {
            custody: error_custody,
            ..
        } = &mut error
            && error_custody.is_none()
        {
            // Pure original split: no admission/cancellation can replace the first Serde cause after its actual diagnostic allocation.
            let bytes = custody.retained();
            *error_custody = Some(
                custody
                    .work()
                    .as_deref_mut()
                    .expect("observed interaction")
                    .take_native_custody(bytes)
                    .expect("preclaimed interaction decoder custody"),
            );
            custody.transfer();
        }
        error
    });
    let value = decoded?;
    Ok((value, custody))
}

pub(super) fn interaction_native_mode(
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
) -> bool {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    {
        work.as_deref()
            .is_some_and(|observer| observer.original_native_work().is_some())
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    {
        let _ = work;
        false
    }
}

pub(super) fn interaction_denial(
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
    code: crate::wire::ErrorCode,
    reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if interaction_native_mode(work) {
        return Error::NativeBoundary { code, reason };
    }
    let _ = work;
    if code == crate::wire::ErrorCode::Conflict {
        Error::Conflict(reason.into())
    } else {
        Error::Invalid(reason.into())
    }
}

pub(super) fn interaction_decode_diagnostic_bytes(input_bytes: u64) -> Result<u64> {
    const LABELS: &[&str] = &[
        "Interaction",
        "Question",
        "Choice",
        "Form",
        "Approval",
        "ChoiceOption",
        "OperationId",
        "InteractionResponse",
        "value",
        "option_ids",
        "approved",
        "reason",
        "a boolean",
        "internally tagged enum InteractionResponse",
        "struct variant InteractionResponse::Question",
        "struct variant InteractionResponse::Choice",
        "struct variant InteractionResponse::Form",
        "struct variant InteractionResponse::Approval",
        "type",
        "question",
        "choice",
        "form",
        "approval",
        "prompt",
        "response_schema",
        "options",
        "min_selections",
        "max_selections",
        "title",
        "schema",
        "operation_id",
        "action_digest",
        "id",
        "label",
        "description",
        "internally tagged enum Interaction",
        "struct variant Interaction::Question",
        "struct variant Interaction::Choice",
        "struct variant Interaction::Form",
        "struct variant Interaction::Approval",
        "struct ChoiceOption",
        "any value",
        "any valid JSON value",
        "a string key",
        "a string",
        "u32",
        "u8",
        "an array of length 32",
        "field identifier",
        "variant identifier",
        "variant index 0 <= i < 4",
        "field index 0 <= i < 4",
        "1 element in sequence",
        " elements in sequence",
        "1 element in map",
        " elements in map",
        "raw value",
        "$serde_json::private::RawValue",
        "a formatted UUID string",
        "a 16 byte array",
        "UUID parsing failed: ",
        "invalid character: found `` at ",
        "invalid length: expected 16 bytes, found ",
        "invalid group count: expected 5, found ",
        "invalid group length in group : expected , found ",
        "non-UTF8 input",
        "invalid length: found ",
        "failed to parse a UUID",
    ];
    crate::contract::audited_serde_diagnostic_bytes(
        input_bytes,
        LABELS.iter().map(|label| label.len() as u64 + 4).sum(),
    )
}

pub(super) fn interaction_decode_owned_bytes(
    items: u64,
    typed_slot: u64,
    codec_owned_bytes: u64,
    value_owned_bytes: u64,
    diagnostic_bytes: u64,
) -> Option<u64> {
    let content_slot =
        std::mem::size_of::<serde::__private229::de::Content<'static>>().max(std::mem::size_of::<(
            serde::__private229::de::Content<'static>,
            serde::__private229::de::Content<'static>,
        )>()) as u64;
    let content_and_typed = content_slot.checked_add(typed_slot)?;
    items
        .checked_mul(10)?
        .checked_mul(content_and_typed)?
        .checked_add(value_owned_bytes)?
        .checked_add(codec_owned_bytes)?
        .checked_add(diagnostic_bytes)
}

pub(super) fn interaction_request_owned_bytes(
    request: &Interaction,
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<u64> {
    let overflow = || {
        interaction_denial(
            work,
            crate::wire::ErrorCode::Invalid,
            "interaction DTO ownership overflows",
        )
    };
    match request {
        Interaction::Question {
            prompt,
            response_schema,
        } => (prompt.capacity() as u64)
            .checked_add(crate::core::value_owned_bytes(response_schema)?)
            .ok_or_else(overflow),
        Interaction::Form { title, schema } => (title.capacity() as u64)
            .checked_add(crate::core::value_owned_bytes(schema)?)
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
