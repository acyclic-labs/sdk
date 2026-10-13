use super::*;
use crate::kernel::{EncodeWorkStep, Encoder};
const SNAPSHOT_DOMAIN: &[u8] = b"acyclic-fs-native-publication-snapshot-v1\0";
const CONTEXT_DOMAIN: &[u8] = b"acyclic-fs-native-publication-context-v1\0";

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn publication_evidence(
    context: NativeSnapshotContext,
    operation: crate::OperationId,
    permit: crate::PublicationPermit,
    ctx: &mut KernelAccounting<'_>,
) -> Result<(crate::Digest, acyclic_stream::OwnedBytes), crate::FsError> {
    let body = 33
        + crate::kernel::CONFIG_ENCODED_BYTES
        + if matches!(context, NativeSnapshotContext::Authored { .. }) {
            0
        } else {
            88
        }
        + if matches!(context, NativeSnapshotContext::Selected { .. }) {
            32
        } else {
            0
        }
        + match permit {
            crate::PublicationPermit::Unrestricted => 1,
            crate::PublicationPermit::Lease { .. } => 57,
            crate::PublicationPermit::Reservation { .. } => 73,
        };
    let (bytes, guard) = crate::kernel::codec::encode_owned(ctx, |observe, reserve| {
        let mut encoder = Encoder::with_exact_capacity(
            SNAPSHOT_DOMAIN,
            1,
            SNAPSHOT_DOMAIN.len() + 2 + body,
            |step| {
                match step {
                    crate::kernel::EncodeAllocationStep::BeforeHeader { bytes } => {
                        observe(EncodeWorkStep::Admit(WorkCounters {
                            bytes_copied: bytes,
                            ..WorkCounters::UNCHARGED
                        }))?
                    }
                    crate::kernel::EncodeAllocationStep::HeaderWritten { bytes } => {
                        observe(EncodeWorkStep::Completed(WorkCounters {
                            bytes_copied: bytes,
                            ..WorkCounters::UNCHARGED
                        }))?
                    }
                    _ => {}
                }
                observe(EncodeWorkStep::Allocation(step))
            },
            reserve,
        )?;
        let encoded = WorkCounters {
            bytes_encoded: body as u64,
            bytes_copied: body as u64,
            ..WorkCounters::UNCHARGED
        };
        observe(EncodeWorkStep::Admit(encoded))?;
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
        observe(EncodeWorkStep::Completed(encoded))?;
        Ok(encoder.finish())
    })?;
    let owned = Owned::new(bytes, guard, 0);
    let hashed = WorkCounters {
        bytes_hashed: (CONTEXT_DOMAIN.len() + owned.value.len()) as u64,
        ..WorkCounters::UNCHARGED
    };
    let result = (|| {
        ctx.admit(hashed)?;
        let mut hash = blake3::Hasher::new();
        hash.update(CONTEXT_DOMAIN);
        hash.update(&owned.value);
        ctx.record_completed(hashed)?;
        Ok::<_, crate::FsError>(crate::Digest::from_bytes(*hash.finalize().as_bytes()))
    })();
    let digest = match result {
        Ok(digest) => digest,
        Err(error) => {
            let bytes = owned.value.capacity() as u64;
            drop(owned.value);
            crate::kernel::retire_allocation(owned.allocation, bytes, ctx)?;
            return Err(error);
        }
    };
    Ok((
        digest,
        crate::kernel::owned_read_bytes((owned.value, owned.allocation).into())?,
    ))
}

pub(super) fn decode_publication_context(
    bytes: &[u8],
    operation: crate::OperationId,
    ctx: &mut KernelAccounting<'_>,
) -> Result<NativeSnapshotContext, crate::FsError> {
    let maximum =
        SNAPSHOT_DOMAIN.len() + 2 + 33 + crate::kernel::CONFIG_ENCODED_BYTES + 88 + 32 + 73;
    crate::kernel::decode_owned(ctx, |mut observe| {
        let mut decoder = crate::kernel::Decoder::new::<crate::FsError>(
            bytes,
            SNAPSHOT_DOMAIN,
            1,
            maximum as u64,
            &mut observe,
        )?;
        if decoder.fixed::<16, crate::FsError>(&mut observe)? != operation.into_bytes() {
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
                let cut = qualification::LazyPublicationCut {
                    workspace_id,
                    config,
                    source: SourceReference {
                        identity: decoder.fixed::<16, crate::FsError>(&mut observe)?,
                        epoch: decoder.u64(&mut observe)?,
                    },
                    overlay: LazyOverlayId(decoder.fixed::<32, crate::FsError>(&mut observe)?),
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
