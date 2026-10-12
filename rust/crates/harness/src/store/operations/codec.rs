use super::*;

pub(crate) fn original_native_decode_mode(observer: &dyn crate::store::history::HistoryIo) -> bool {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    {
        observer.original_native_work().is_some()
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    {
        let _ = observer;
        false
    }
}

pub(crate) fn decode_preclaimed_json_with_mode<T, F>(
    bytes: &[u8],
    bounds: crate::contract::JsonBounds,
    maximum_owned_bytes: u64,
    code: crate::wire::ErrorCode,
    native: bool,
    value_schema: crate::contract::JsonValueSchema,
    charge: &mut F,
) -> Result<T>
where
    T: serde::de::DeserializeOwned,
    F: FnMut(crate::contract::JsonCodecCharge) -> Result<()>,
{
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if native {
        return crate::contract::json_from_slice_bounded(
            std::marker::PhantomData,
            bytes,
            bounds,
            maximum_owned_bytes,
            Some(code),
            Some(value_schema),
            charge,
        );
    }
    let _ = (native, code, value_schema);
    crate::contract::json_from_slice_bounded(
        std::marker::PhantomData,
        bytes,
        bounds,
        maximum_owned_bytes,
        None,
        None,
        charge,
    )
}

pub(crate) fn retain_original_decode_error<T>(
    mut result: Result<T>,
    reservation: &mut u64,
    observer: &mut dyn crate::store::history::HistoryIo,
) -> Result<T> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Err(Error::NativeDecode { custody, .. }) = &mut result
        && custody.is_none()
    {
        *custody = Some(
            observer
                .take_native_custody(*reservation)
                .expect("original native decoder reservation has source custody"),
        );
        *reservation = 0;
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    {
        let _ = (&mut result, reservation, observer);
    }
    result
}

pub(in crate::store) fn validate_canonical_json_with_work(
    bytes: &[u8],
    maximum_bytes: u64,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    let Some(observer) = work.as_deref_mut() else {
        let _: serde_json::Value = crate::executor::decode_json(bytes, None)?.0;
        return Ok(());
    };
    if bytes.len() as u64 > maximum_bytes {
        return Err(crate::store::guarded_boundary(
            work,
            crate::wire::ErrorCode::Storage,
            "index JSON exceeds its byte bound",
        ));
    }
    let (parsed, retained) = observer.decode_json_value(bytes)?;
    let encoded = crate::store::history::canonical_bytes_with_work(&parsed, maximum_bytes, work);
    drop(parsed);
    let result = match encoded {
        Ok(encoded) => {
            let identical = encoded == bytes;
            let capacity = encoded.capacity() as u64;
            drop(encoded);
            let verified = if identical {
                Ok(())
            } else {
                Err(crate::store::guarded_boundary(
                    work,
                    crate::wire::ErrorCode::Storage,
                    "execution journal JSON is not canonical",
                ))
            };
            release_owned_after_drop(verified, capacity, work)
        }
        Err(error) => Err(error),
    };
    release_owned_after_drop(result, retained, work)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::store) struct OperationLocation {
    pub(in crate::store) authority: Authority,
    pub(in crate::store) operation_id: OperationId,
    pub(in crate::store) revision: u64,
    pub(in crate::store) intent_digest: [u8; 32],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::store) effect_position: Option<u64>,
}

pub(in crate::store) fn decode_index<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    maximum_bytes: u64,
    labels: &[&str],
    admission: fn(crate::contract::JsonValueShape, &mut dyn crate::store::HistoryIo) -> Result<u64>,
    retained: fn(&T) -> (u64, u64),
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<T> {
    validate_canonical_json_with_work(bytes, maximum_bytes, work)?;
    let Some(observer) = work.as_deref_mut() else {
        return crate::contract::json_from_slice(bytes).map_err(|error| {
            Error::Storage(format!("execution journal content is invalid: {error}"))
        });
    };
    let diagnostic = crate::contract::audited_serde_diagnostic_bytes(
        bytes.len() as u64,
        labels.iter().map(|label| label.len() as u64 + 4).sum(),
    )?;
    let mut reservation = 0;
    let mut observed_failure = false;
    let native = original_native_decode_mode(observer);
    let decoded = decode_preclaimed_json_with_mode::<T, _>(
        bytes,
        crate::contract::JsonBounds {
            maximum_bytes,
            maximum_items: maximum_bytes,
            maximum_depth: 256,
        },
        bytes.len() as u64,
        crate::wire::ErrorCode::Storage,
        native,
        crate::contract::JsonValueSchema::None,
        &mut |charge| {
            let result = if let crate::contract::JsonCodecCharge::Admission {
                maximum_owned_bytes,
                value_shape,
            } = charge
            {
                let extra = admission(value_shape, observer)?;
                let planned = maximum_owned_bytes
                    .checked_add(extra)
                    .and_then(|bytes| bytes.checked_add(diagnostic))
                    .ok_or_else(|| Error::Invalid("index reservation overflows".into()))?;
                observer.retain_owned(planned)?;
                reservation = planned;
                Ok(())
            } else {
                observer.codec_charge(charge)
            };
            if result.is_err() {
                observed_failure = true;
            }
            result
        },
    );
    let decoded =
        retain_original_decode_error(decoded, &mut reservation, observer).map_err(|error| {
            match error {
                Error::Invalid(message) if !observed_failure => {
                    Error::Storage(format!("execution journal content is invalid: {message}"))
                }
                other => other,
            }
        });
    let value = match decoded {
        Ok(value) => value,
        Err(error) => {
            let _ = observer.release_owned(reservation);
            return Err(error);
        }
    };
    let (owned, copied) = retained(&value);
    let Some(scratch) = reservation.checked_sub(owned) else {
        drop(value);
        observer.release_owned(reservation)?;
        return Err(Error::Storage(
            "decoded index exceeds its admitted storage".into(),
        ));
    };
    let result = observer
        .release_owned(scratch)
        .and_then(|()| observer.copy_bytes(copied));
    if let Err(error) = result {
        drop(value);
        let _ = observer.release_owned(owned);
        return Err(error);
    }
    Ok(value)
}

pub(in crate::store) fn decode_location_with_work(
    bytes: &[u8],
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<OperationLocation> {
    decode_index(
        bytes,
        acyclic_stream::MAX_RECORD_BYTES as u64,
        &[
            "OperationLocation",
            "Authority",
            "AggregateKind",
            "authority",
            "operation_id",
            "revision",
            "intent_digest",
            "effect_position",
            "kind",
            "id",
            "agent",
            "conversation",
            "session",
            "turn",
            "task",
        ],
        |_, _| Ok(0),
        |location: &OperationLocation| {
            (
                location.authority.id.capacity() as u64,
                location.authority.id.len() as u64,
            )
        },
        work,
    )
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::store) struct MessageLocation {
    pub(in crate::store) location: OperationLocation,
    pub(in crate::store) message_id: uuid::Uuid,
    pub(in crate::store) sequence: u64,
}

pub(in crate::store) fn decode_message_location_with_work(
    bytes: &[u8],
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<MessageLocation> {
    decode_index(
        bytes,
        acyclic_stream::MAX_RECORD_BYTES as u64,
        &[
            "MessageLocation",
            "OperationLocation",
            "Authority",
            "AggregateKind",
            "location",
            "message_id",
            "sequence",
            "authority",
            "operation_id",
            "revision",
            "intent_digest",
            "effect_position",
            "kind",
            "id",
            "agent",
            "conversation",
            "session",
            "turn",
            "task",
        ],
        |_, _| Ok(0),
        |message: &MessageLocation| {
            (
                message.location.authority.id.capacity() as u64,
                message.location.authority.id.len() as u64,
            )
        },
        work,
    )
}
