//! Bounded JSON on the original native ledger.
use super::*;

pub(crate) fn guarded_pi_bytes<T: Serialize>(
    value: &T,
    maximum_bytes: u64,
    work: &NativeMaterializationWork,
) -> Result<(Vec<u8>, NativeAllocation)> {
    encode_pi(Some(maximum_bytes), work, |bounds, charge| {
        json_bytes_bounded(value, bounds, Some(ErrorCode::Invalid), &mut |event| {
            charge(event)
        })
    })
}

fn encode_pi<T>(
    maximum_bytes: Option<u64>,
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
                quota.resize(parser_owned)?;
                parser_quota = Some(quota);
                Ok(())
            }
            JsonCodecCharge::Retain { bytes } => {
                let observed = custody.bytes().checked_add(bytes).ok_or_else(|| {
                    acyclic_harness::Error::from(NativeTreeMaterializationError::Work(
                        WorkError::Overflow,
                    ))
                })?;
                if let Some(mut quota) = parser_quota.take() {
                    custody.absorb(&mut quota)?;
                }
                custody
                    .resize(observed)
                    .map_err(acyclic_harness::Error::from)
            }
            charge => native_buffer_codec_charge(&mut custody, charge),
        }
    });
    match result {
        Ok(value) => Ok((value, custody)),
        Err(Error::NativeJson {
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
            Err(Error::NativeJson {
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
    custody: &mut NativeAllocation,
    charge: JsonCodecCharge,
) -> Result<()> {
    if let JsonCodecCharge::Release { bytes } = charge {
        return custody.release(bytes).map_err(acyclic_harness::Error::from);
    }
    use acyclic_harness::store::HistoryIo;
    let work = custody.work();
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
            custody.resize(maximum_owned_bytes)?;
            Ok(())
        }
        JsonCodecCharge::Retain { bytes } => {
            let retained = custody
                .bytes()
                .checked_add(bytes)
                .ok_or(NativeTreeMaterializationError::Work(WorkError::Overflow))?;
            custody.resize(retained)?;
            Ok(())
        }
        JsonCodecCharge::Release { .. } => unreachable!(),
        charge => acyclic_harness::store::HistoryWork::new(
            acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
        )
        .codec_charge(charge),
    }
}

/// Reads only the original immutable upload.
#[allow(
    clippy::too_many_arguments,
    reason = "original immutable operation plus actual scoped content reader"
)]
pub(crate) async fn read_pi_input<T: DeserializeOwned + acyclic_harness::HeapLayout>(
    owner: &PiOwnerConfig,
    filesystem: &Host,
    operation: OperationId,
    label: &str,
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
        .read_file(
            &reference.0,
            Some(&acyclic_harness::filesystem::PreparationIo::from_native_work(work)?),
        )
        .await?;
    let maximum = owner
        .policy()
        .maximum_payload_bytes
        .min(crate::MAXIMUM_ADAPTER_STATE_BYTES);
    let decoded = guarded_pi_decode(
        bytes.as_ref(),
        maximum,
        ErrorCode::Storage,
        JsonValueSchema::None,
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
    encode_pi(Some(maximum_bytes), work, |bounds, charge| {
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
) -> Result<blake3::Hash> {
    let bytes = canonical_pi_value(value, maximum, work)?;
    work.charge(WorkCounters {
        bytes_hashed: bytes.0.len() as u64,
        ..WorkCounters::default()
    })?;
    Ok(blake3::hash(&bytes.0))
}

/// Decodes `T`, retaining its root plus the heap its layout admitted for
/// this input.
///
/// The retained charge is derived from the actual input, never from a configured payload
/// cap, so a small value never holds a cap-sized allocation for its lifetime.
pub(crate) fn guarded_pi_decode<T: DeserializeOwned + acyclic_harness::HeapLayout>(
    bytes: &[u8],
    maximum_bytes: u64,
    code: ErrorCode,
    value_schema: &'static JsonValueSchema,
    schema_label_bytes: u64,
    work: &NativeMaterializationWork,
) -> Result<(T, NativeAllocation)> {
    let remaining = work.remaining()?;
    let mut custody = work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
    let mut retained_owned = 0;
    let bounds = JsonBounds {
        maximum_bytes,
        maximum_items: remaining.items_examined,
        maximum_depth: 64,
    };
    let result = json_from_slice_bounded(
        std::marker::PhantomData::<T>,
        bytes,
        bounds,
        Some(code),
        Some(value_schema),
        &mut |charge| {
            if let JsonCodecCharge::Release { bytes } = charge {
                return custody.release(bytes).map_err(acyclic_harness::Error::from);
            }
            match charge {
                JsonCodecCharge::Admission {
                    maximum_owned_bytes,
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
                    retained_owned = (std::mem::size_of::<T>() as u64)
                        .checked_add(maximum_owned_bytes)
                        .ok_or(NativeTreeMaterializationError::Work(WorkError::Overflow))?;
                    let admitted = retained_owned
                        .checked_add(diagnostic)
                        .ok_or(NativeTreeMaterializationError::Work(WorkError::Overflow))?;
                    custody
                        .resize(admitted)
                        .map_err(acyclic_harness::Error::from)
                }
                charge => native_buffer_codec_charge(&mut custody, charge),
            }
        },
    );
    match result {
        Ok(value) => {
            custody.release(custody.bytes().checked_sub(retained_owned).ok_or(
                acyclic_fs::NativeTreeMaterializationError::Work(acyclic_fs::WorkError::Overflow),
            )?)?;
            Ok((value, custody))
        }
        Err(Error::NativeJson {
            code,
            source,
            custody: None,
        }) => Err(Error::NativeJson {
            code,
            source,
            custody: Some(custody),
        }),
        Err(error) => {
            // The SDK has already destroyed its partial DTO and parser here.
            custody.release(custody.bytes())?;
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
    let encoded = encode_pi(None, work, |bounds, charge| {
        json_value_bounded(value, bounds, Some(ErrorCode::Invalid), &mut |event| {
            charge(event)
        })
    })?;
    let observed = encoded.1.bytes();
    if observed > maximum_owned_bytes {
        return Err(acyclic_harness::Error::from(
            NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                counter: "pi_value_owned_bytes",
                observed,
                maximum: maximum_owned_bytes,
            }),
        ));
    }
    Ok(encoded)
}
