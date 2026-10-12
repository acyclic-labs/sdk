use super::*;
use crate::kernel::{EncodeWorkStep, Encoder, KernelAccounting};

const SNAPSHOT_DOMAIN: &[u8] = b"acyclic-fs-native-publication-snapshot-v1\0";
const CONTEXT_DOMAIN: &[u8] = b"acyclic-fs-native-publication-context-v1\0";

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn native_publication_evidence(
    context: NativeSnapshotContext,
    operation: crate::OperationId,
    permit: crate::PublicationPermit,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(crate::Digest, acyclic_stream::OwnedBytes), crate::FsError> {
    let original = accounting
        .native_work()
        .ok_or(crate::NativeTreeMaterializationError::InvalidLayout)?
        .clone();
    let body_bytes = 33
        + crate::kernel::CONFIG_ENCODED_BYTES
        + if !matches!(context, NativeSnapshotContext::Authored { .. }) {
            88
        } else {
            0
        }
        + match permit {
            crate::PublicationPermit::Unrestricted => 1,
            crate::PublicationPermit::Lease { .. } => 57,
            crate::PublicationPermit::Reservation { .. } => 73,
        };
    let body_bytes = body_bytes
        + if matches!(context, NativeSnapshotContext::Selected { .. }) {
            32
        } else {
            0
        };
    let size = SNAPSHOT_DOMAIN.len() + 2 + body_bytes;
    let (bytes, allocation) =
        crate::kernel::codec::encode_owned(accounting, |observe, reserve| {
            let mut encoder = Encoder::with_exact_capacity(
                SNAPSHOT_DOMAIN,
                1,
                size,
                |step| {
                    match step {
                        crate::kernel::EncodeAllocationStep::BeforeHeader { bytes } => {
                            observe(EncodeWorkStep::Admit(WorkCounters {
                                bytes_copied: bytes,
                                ..WorkCounters::UNCHARGED
                            }))?;
                        }
                        crate::kernel::EncodeAllocationStep::HeaderWritten { bytes } => {
                            observe(EncodeWorkStep::Completed(WorkCounters {
                                bytes_copied: bytes,
                                ..WorkCounters::UNCHARGED
                            }))?;
                        }
                        _ => {}
                    }
                    observe(EncodeWorkStep::Allocation(step))
                },
                reserve,
            )?;
            let delta = WorkCounters {
                bytes_encoded: body_bytes as u64,
                bytes_copied: body_bytes as u64,
                ..WorkCounters::UNCHARGED
            };
            observe(EncodeWorkStep::Admit(delta))?;
            encoder.fixed(&operation.into_bytes());
            encoder.u8(match context {
                NativeSnapshotContext::Authored { .. } => 0,
                NativeSnapshotContext::Lazy(_) => 1,
                NativeSnapshotContext::Selected { .. } => 2,
            });
            encoder.fixed(&context.workspace_id().into_bytes());
            crate::kernel::encode_config(&mut encoder, context.config());
            if let NativeSnapshotContext::Lazy(cut) | NativeSnapshotContext::Selected { cut, .. } =
                context
            {
                encoder.fixed(&cut.source.identity);
                encoder.u64(cut.source.epoch);
                encoder.fixed(&cut.overlay.into_bytes());
                encoder.fixed(&cut.shadows.into_bytes());
            }
            if let NativeSnapshotContext::Selected { input, .. } = context {
                encoder.fixed(input.as_bytes());
            }
            encode_permit(&mut encoder, permit);
            observe(EncodeWorkStep::Completed(delta))?;
            Ok(encoder.finish())
        })?;
    let owned_bytes = bytes.capacity() as u64;
    let hashed = WorkCounters {
        bytes_hashed: (CONTEXT_DOMAIN.len() + bytes.len()) as u64,
        ..WorkCounters::UNCHARGED
    };
    let result = (|| {
        accounting.admit(hashed)?;
        let mut hash = blake3::Hasher::new();
        hash.update(CONTEXT_DOMAIN);
        hash.update(&bytes);
        let digest = crate::Digest::from_bytes(*hash.finalize().as_bytes());
        accounting.record_completed(hashed)?;
        Ok::<_, crate::FsError>(digest)
    })();
    let digest = match result {
        Ok(digest) => digest,
        Err(error) => {
            drop(bytes);
            crate::kernel::retire_allocation(allocation, owned_bytes, accounting)?;
            return Err(error);
        }
    };
    let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) = allocation
    else {
        unreachable!("native publication evidence uses original allocation")
    };
    let bytes = acyclic_stream::OwnedBytes::from_vec_with_guard(
        bytes,
        allocation,
        original.runtime_meter(),
    )
    .map_err(|failure| -> crate::FsError {
        match failure.into_error() {
            acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
                original.runtime_error(error).into()
            }
            acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized => {
                crate::NativeTreeMaterializationError::InvalidLayout.into()
            }
        }
    })?;
    Ok((digest, bytes))
}

pub(super) fn encode_permit(encoder: &mut Encoder, permit: crate::PublicationPermit) {
    match permit {
        crate::PublicationPermit::Unrestricted => encoder.u8(0),
        crate::PublicationPermit::Lease {
            authority_id,
            workspace_id,
            lease_id,
            expires_at_millis,
        } => {
            encoder.u8(1);
            encoder.fixed(&authority_id);
            encoder.fixed(&workspace_id);
            encoder.fixed(&lease_id);
            encoder.u64(expires_at_millis);
        }
        crate::PublicationPermit::Reservation {
            operation_id,
            gate_tail,
            expected,
        } => {
            encoder.u8(2);
            encoder.fixed(&operation_id);
            encoder.u64(gate_tail);
            encoder.u64(expected.epoch.get());
            encoder.u64(expected.sequence.get());
            encoder.fixed(expected.digest.as_bytes());
        }
    }
}

pub(super) fn decode_publication_context(
    evidence: &[u8],
    operation: crate::OperationId,
    accounting: &mut KernelAccounting<'_>,
) -> Result<NativeSnapshotContext, crate::FsError> {
    let maximum =
        SNAPSHOT_DOMAIN.len() + 2 + 33 + crate::kernel::CONFIG_ENCODED_BYTES + 88 + 32 + 73;
    crate::kernel::decode_owned(accounting, |mut observe| {
        let mut decoder = crate::kernel::Decoder::new::<crate::FsError>(
            evidence,
            SNAPSHOT_DOMAIN,
            1,
            maximum as u64,
            &mut observe,
        )?;
        if crate::OperationId::from_bytes(decoder.fixed::<16, crate::FsError>(&mut observe)?)
            != operation
        {
            return Err(invalid_context());
        }
        let kind = decoder.u8(&mut observe)?;
        let workspace_id =
            WorkspaceId::from_bytes(decoder.fixed::<16, crate::FsError>(&mut observe)?);
        let config = crate::kernel::decode_config(&mut decoder, &mut observe)?;
        let context = match kind {
            0 => NativeSnapshotContext::Authored {
                workspace_id,
                config,
            },
            1 | 2 => {
                let cut = super::qualification::LazyPublicationCut {
                    workspace_id,
                    config,
                    source: SourceReference {
                        identity: decoder.fixed::<16, crate::FsError>(&mut observe)?,
                        epoch: decoder.u64(&mut observe)?,
                    },
                    overlay: LazyOverlayId::from_bytes(
                        decoder.fixed::<32, crate::FsError>(&mut observe)?,
                    ),
                    shadows: LazyShadowId(decoder.fixed::<32, crate::FsError>(&mut observe)?),
                };
                if kind == 2 {
                    NativeSnapshotContext::Selected {
                        cut,
                        input: crate::Digest::from_bytes(
                            decoder.fixed::<32, crate::FsError>(&mut observe)?,
                        ),
                    }
                } else {
                    NativeSnapshotContext::Lazy(cut)
                }
            }
            tag => {
                return Err(crate::kernel::CanonicalDecodeError::UnknownTag {
                    field: "publication view",
                    tag,
                }
                .into());
            }
        };
        match decoder.u8(&mut observe)? {
            0 => {}
            1 => {
                decoder.fixed::<16, crate::FsError>(&mut observe)?;
                decoder.fixed::<16, crate::FsError>(&mut observe)?;
                decoder.fixed::<16, crate::FsError>(&mut observe)?;
                decoder.u64(&mut observe)?;
            }
            2 => {
                decoder.fixed::<16, crate::FsError>(&mut observe)?;
                decoder.u64(&mut observe)?;
                crate::Epoch::new(decoder.u64(&mut observe)?).map_err(|_| invalid_context())?;
                decoder.u64(&mut observe)?;
                decoder.fixed::<32, crate::FsError>(&mut observe)?;
            }
            tag => {
                return Err(crate::kernel::CanonicalDecodeError::UnknownTag {
                    field: "publication permit",
                    tag,
                }
                .into());
            }
        }
        decoder.finish()?;
        Ok(context)
    })
    .map(|(context, _)| context)
}

fn invalid_context() -> crate::FsError {
    crate::kernel::CanonicalDecodeError::Invariant(crate::kernel::CanonicalInvariant::Static(
        "original publication context differs",
    ))
    .into()
}
