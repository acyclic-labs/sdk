//! Bounded JSON on the original native ledger.
use super::*;

/// Counts actual closed DTO labels plus serde's expected-list punctuation.
pub(crate) const fn pi_serde_diagnostic_labels_bytes(labels: &[&str]) -> u64 {
    let mut total = 0_u64;
    let mut index = 0;
    while index < labels.len() {
        total += labels[index].len() as u64 + 4;
        index += 1;
    }
    total
}

pub(crate) fn guarded_pi_bytes<T: Serialize>(
    value: &T,
    maximum_bytes: u64,
    work: &NativeMaterializationWork,
) -> Result<(Vec<u8>, NativeAllocation)> {
    encode_pi(Some(maximum_bytes), None, work, |bounds, charge| {
        json_bytes_bounded(value, bounds, Some(ErrorCode::Invalid), &mut |event| {
            charge(event)
        })
    })
}

fn encode_pi<T>(
    maximum_bytes: Option<u64>,
    maximum_owned_bytes: Option<u64>,
    work: &NativeMaterializationWork,
    encode: impl FnOnce(JsonBounds, &mut dyn FnMut(JsonCodecCharge) -> Result<()>) -> Result<T>,
) -> Result<(T, NativeAllocation)> {
    let remaining = work.remaining()?;
    let mut custody = work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
    let mut diagnostic = work.claim(
        sdk_json_diagnostic_bytes(),
        0,
        acyclic_fs::WorkCounters::UNCHARGED,
    )?;
    let mut parser_quota: Option<NativeAllocation> = None;
    let bounds = JsonBounds {
        maximum_bytes: maximum_bytes.map_or(remaining.bytes_encoded, |maximum| {
            maximum.min(remaining.bytes_encoded)
        }),
        maximum_items: remaining.items_examined,
        maximum_depth: 64,
    };
    let result = encode(bounds, &mut |charge| {
        if let JsonCodecCharge::Release { bytes } = charge {
            return custody.release(bytes).map_err(acyclic_harness::Error::from);
        }
        work.cancellation()
            .check()
            .map_err(|_| acyclic_harness::Error::from(NativeTreeMaterializationError::Cancelled))?;
        match charge {
            JsonCodecCharge::Admission {
                maximum_owned_bytes,
                ..
            } => {
                let parser_owned = maximum_owned_bytes
                    .checked_sub(sdk_json_diagnostic_bytes())
                    .ok_or(Error::NativeBoundary {
                        code: ErrorCode::Invalid,
                        reason: "SDK raw parser quota omits its diagnostic inventory",
                    })?;
                let mut quota = work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
                quota.resize_retained(parser_owned)?;
                parser_quota = Some(quota);
                Ok(())
            }
            JsonCodecCharge::Retain { bytes } => {
                let observed = custody.bytes().checked_add(bytes).ok_or_else(|| {
                    acyclic_harness::Error::from(NativeTreeMaterializationError::Work(
                        WorkError::Overflow,
                    ))
                })?;
                if let Some(maximum) = maximum_owned_bytes {
                    if observed > maximum {
                        return Err(acyclic_harness::Error::from(
                            NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                                counter: "pi_value_owned_bytes",
                                observed,
                                maximum,
                            }),
                        ));
                    }
                }
                if let Some(mut quota) = parser_quota.take() {
                    custody.absorb(&mut quota)?;
                    custody
                        .substitute(observed)
                        .map_err(acyclic_harness::Error::from)
                } else {
                    custody
                        .resize_retained(observed)
                        .map_err(acyclic_harness::Error::from)
                }
            }
            charge => native_buffer_codec_charge(work, &mut custody, charge),
        }
    });
    match result {
        Ok(value) => Ok((value, custody)),
        Err(Error::NativeEncode {
            code,
            source,
            custody: None,
        }) => {
            if let Some(mut quota) = parser_quota {
                diagnostic.absorb(&mut quota)?;
            }
            if custody.bytes() != 0 {
                diagnostic.absorb(&mut custody)?;
            }
            Err(Error::NativeEncode {
                code,
                source,
                custody: Some(diagnostic),
            })
        }
        Err(error @ Error::NativeWork { custody: None, .. }) if custody.bytes() == 0 => Err(error),
        Err(Error::NativeWork {
            cause,
            custody: None,
        }) => Err(Error::NativeWork {
            cause,
            custody: Some(custody),
        }),
        Err(error) => Err(error),
    }
}

pub(crate) fn native_buffer_codec_charge(
    work: &NativeMaterializationWork,
    custody: &mut NativeAllocation,
    charge: JsonCodecCharge,
) -> Result<()> {
    if let JsonCodecCharge::Release { bytes } = charge {
        return custody.release(bytes).map_err(acyclic_harness::Error::from);
    }
    use acyclic_harness::store::HistoryIo;
    work.cancellation()
        .check()
        .map_err(|_| NativeTreeMaterializationError::Cancelled)?;
    match charge {
        JsonCodecCharge::Read { bytes } => work
            .charge(WorkCounters {
                source_bytes_read: bytes,
                ..WorkCounters::default()
            })
            .map_err(acyclic_harness::Error::from),
        JsonCodecCharge::Admission {
            maximum_owned_bytes,
            ..
        } => {
            custody.resize_retained(maximum_owned_bytes)?;
            Ok(())
        }
        JsonCodecCharge::Retain { bytes } => {
            let retained = custody
                .bytes()
                .checked_add(bytes)
                .ok_or(NativeTreeMaterializationError::Work(WorkError::Overflow))?;
            custody.resize_retained(retained)?;
            Ok(())
        }
        JsonCodecCharge::Release { .. } => unreachable!(),
        charge => acyclic_harness::store::native_work_io(work)?.codec_charge(charge),
    }
}

/// Reads only the original immutable upload.
#[allow(
    clippy::too_many_arguments,
    reason = "original immutable operation plus actual scoped content reader"
)]
pub(crate) async fn read_pi_input_with_native_work<T: DeserializeOwned>(
    owner: &PiOwnerConfig,
    filesystem: &Host,
    operation: OperationId,
    label: &str,
    maximum_owned_bytes: u64,
    value_schema: JsonValueSchema,
    schema_label_bytes: u64,
    context: &TaskContext,
    work: &NativeMaterializationWork,
) -> Result<Option<(T, NativeAllocation)>> {
    let Some((reference, custody)) =
        observe_pi_input(owner, filesystem, operation, label, work).await?
    else {
        return Ok(None);
    };
    let reference = (reference, custody);
    let bytes = context
        .read_file_with_native_work(&reference.0, work)
        .await?;
    let maximum = owner
        .policy()
        .maximum_payload_bytes
        .min(crate::MAXIMUM_ADAPTER_STATE_BYTES);
    let decoded = guarded_pi_decode(
        bytes.as_ref(),
        maximum,
        maximum_owned_bytes,
        ErrorCode::Storage,
        value_schema,
        schema_label_bytes,
        work,
    )?;
    drop(bytes);
    drop(reference);
    Ok(Some(decoded))
}

/// Original SDK canonical bytes, with each actual producer callback using the caller's existing native ledger and cancellation rather than a fresh meter.
pub(crate) fn canonical_pi_value<T: Serialize + ?Sized>(
    value: &T,
    maximum_bytes: u64,
    work: &NativeMaterializationWork,
) -> Result<(Vec<u8>, NativeAllocation)> {
    encode_pi(Some(maximum_bytes), None, work, |bounds, charge| {
        canonical_json_bytes_bounded(value, bounds, Some(ErrorCode::Invalid), &mut |event| {
            charge(event)
        })
    })
}

/// The exact existing BLAKE3 identity over those canonical bytes.
pub(crate) fn digest_pi_value<T: Serialize + ?Sized>(
    value: &T,
    maximum: u64,
    work: &NativeMaterializationWork,
) -> Result<(String, NativeAllocation)> {
    let bytes = canonical_pi_value(value, maximum, work)?;
    work.charge(WorkCounters {
        bytes_hashed: bytes.0.len() as u64,
        ..WorkCounters::default()
    })?;
    let digest = blake3::hash(&bytes.0);
    drop(bytes);
    work.display(&format_args!("{}", digest.to_hex()))
        .map_err(acyclic_harness::Error::from)
}

pub(crate) fn guarded_pi_decode<T: DeserializeOwned>(
    bytes: &[u8],
    maximum_bytes: u64,
    maximum_owned_bytes: u64,
    code: ErrorCode,
    value_schema: JsonValueSchema,
    schema_label_bytes: u64,
    work: &NativeMaterializationWork,
) -> Result<(T, NativeAllocation)> {
    let remaining = work.remaining()?;
    let mut custody = work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
    let mut retained_owned = maximum_owned_bytes;
    let bounds = JsonBounds {
        maximum_bytes,
        maximum_items: remaining.items_examined,
        maximum_depth: 64,
    };
    let result = json_from_slice_bounded(
        std::marker::PhantomData::<T>,
        bytes,
        bounds,
        maximum_owned_bytes,
        Some(code),
        Some(value_schema),
        &mut |charge| {
            if let JsonCodecCharge::Release { bytes } = charge {
                return custody.release(bytes).map_err(acyclic_harness::Error::from);
            }
            match charge {
                JsonCodecCharge::Admission {
                    maximum_owned_bytes: admission_bytes,
                    value_shape,
                } => {
                    work.cancellation()
                        .check()
                        .map_err(|_| NativeTreeMaterializationError::Cancelled)?;
                    let diagnostic = json_typed_diagnostic_owned_bytes(
                        value_shape,
                        schema_label_bytes,
                        Some(code),
                    )?;
                    let owned = json_value_decode_owned_bytes(value_shape)?;
                    retained_owned = maximum_owned_bytes
                        .checked_add(owned)
                        .ok_or(NativeTreeMaterializationError::Work(WorkError::Overflow))?;
                    let admitted = admission_bytes
                        .checked_add(owned)
                        .and_then(|bytes| bytes.checked_add(diagnostic))
                        .ok_or(NativeTreeMaterializationError::Work(WorkError::Overflow))?;
                    custody
                        .resize_retained(admitted)
                        .map_err(acyclic_harness::Error::from)
                }
                charge => native_buffer_codec_charge(work, &mut custody, charge),
            }
        },
    );
    match result {
        Ok(value) => {
            custody.substitute(retained_owned)?;
            Ok((value, custody))
        }
        Err(Error::NativeDecode {
            code,
            source,
            custody: None,
        }) => Err(Error::NativeDecode {
            code,
            source,
            custody: Some(custody),
        }),
        Err(error) => {
            // The SDK has already destroyed its partial DTO and parser here.
            custody.substitute(0)?;
            drop(custody);
            Err(error)
        }
    }
}

/// Produces the SDK encoder's actual Value on the caller's original native ledger.
pub(crate) fn guarded_pi_value<T: Serialize>(
    value: &T,
    maximum_owned_bytes: u64,
    work: &NativeMaterializationWork,
) -> Result<(serde_json::Value, NativeAllocation)> {
    encode_pi(None, Some(maximum_owned_bytes), work, |bounds, charge| {
        json_value_bounded(value, bounds, Some(ErrorCode::Invalid), &mut |event| {
            charge(event)
        })
    })
}
