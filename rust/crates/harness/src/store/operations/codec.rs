use super::*;
use crate::executor::json::{JsonBounds, JsonCodecCharge, JsonValueSchema, JsonValueShape};

pub(crate) fn original_native_decode_mode(_observer: &dyn HistoryIo) -> bool {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    return _observer.original_native_work().is_some();
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    false
}

pub(crate) fn decode_preclaimed_json_with_mode<T, F>(
    bytes: &[u8],
    bounds: JsonBounds,
    maximum_owned_bytes: u64,
    code: crate::wire::ErrorCode,
    native: bool,
    value_schema: &'static JsonValueSchema,
    charge: &mut F,
) -> Result<T>
where
    T: serde::de::DeserializeOwned,
    F: FnMut(JsonCodecCharge) -> Result<()>,
{
    let native = native && cfg!(all(feature = "filesystem", not(target_arch = "wasm32")));
    crate::executor::json::json_from_slice_bounded(
        std::marker::PhantomData,
        bytes,
        bounds,
        maximum_owned_bytes,
        native.then_some(code),
        native.then_some(value_schema),
        charge,
    )
}

pub(crate) fn retain_original_decode_error<T>(
    result: Result<T>,
    _reservation: &mut u64,
    _observer: &mut dyn HistoryIo,
) -> Result<T> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let mut result = result;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Err(Error::NativeJson { custody, .. }) = &mut result
        && custody.is_none()
    {
        *custody = Some(_observer.take_native_custody(*_reservation)?);
        *_reservation = 0;
    }
    result
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::store) struct OperationLocation<A = Authority> {
    pub(in crate::store) authority: A,
    pub(in crate::store) operation_id: OperationId,
    pub(in crate::store) revision: u64,
    pub(in crate::store) intent_digest: [u8; 32],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::store) effect_position: Option<u64>,
}

pub(in crate::store) fn decode_index<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    maximum_bytes: u64,
    labels: &str,
    admission: fn(JsonValueShape, &mut dyn HistoryIo) -> Result<u64>,
    retained: fn(&T) -> (u64, u64),
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<T> {
    if bytes.len() as u64 > maximum_bytes {
        return Err(boundary(work, Storage, "index JSON exceeds its byte bound"));
    }
    let Some(observer) = work.as_deref_mut() else {
        return crate::contract::json_from_slice(bytes)
            .map_err(|error| Error::Storage(error.to_string()));
    };
    let diagnostic = crate::executor::json::audited_serde_diagnostic_bytes(
        bytes.len() as u64,
        labels
            .split_ascii_whitespace()
            .map(|label| label.len() as u64 + 4)
            .sum(),
    )?;
    let mut reservation = 0;
    let mut observed_failure = false;
    let native = original_native_decode_mode(observer);
    let decoded = decode_preclaimed_json_with_mode::<T, _>(
        bytes,
        JsonBounds {
            maximum_bytes,
            maximum_items: maximum_bytes,
            maximum_depth: 256,
        },
        bytes.len() as u64,
        Storage,
        native,
        JsonValueSchema::None,
        &mut |charge| {
            let result = if let JsonCodecCharge::Admission {
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
            observed_failure |= result.is_err();
            result
        },
    );
    let decoded =
        retain_original_decode_error(decoded, &mut reservation, observer).map_err(|error| {
            match error {
                Error::Invalid(message) if !observed_failure => Error::Storage(message),
                other => other,
            }
        });
    let mut quota = CodecQuota::from_retained(work, reservation);
    let value = decoded?;
    let (owned, copied) = retained(&value);
    if owned > quota.retained() {
        return Err(Error::Storage(
            "decoded index exceeds its admitted storage".into(),
        ));
    }
    quota.replace(owned)?;
    let observer = quota.observer()?;
    observer.copy_bytes(copied)?;
    quota.transfer();
    Ok(value)
}

pub(in crate::store) fn decode_location_with_work(
    bytes: &[u8],
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<OperationLocation> {
    decode_index(
        bytes,
        acyclic_stream::MAX_RECORD_BYTES as u64,
        "OperationLocation Authority AggregateKind authority operation_id revision intent_digest effect_position kind id agent conversation session turn task",
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
pub(in crate::store) struct MessageLocation<A = Authority> {
    pub(in crate::store) location: OperationLocation<A>,
    pub(in crate::store) message_id: uuid::Uuid,
    pub(in crate::store) sequence: u64,
}

pub(in crate::store) fn decode_message_location_with_work(
    bytes: &[u8],
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<MessageLocation> {
    decode_index(
        bytes,
        acyclic_stream::MAX_RECORD_BYTES as u64,
        "MessageLocation OperationLocation Authority AggregateKind location message_id sequence authority operation_id revision intent_digest effect_position kind id agent conversation session turn task",
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
