use super::*;
use crate::kernel::{
    CanonicalDecodeError, CanonicalInvariant, Decoder, EncodeAllocationStep, EncodeWorkStep,
    Encoder, ReadAllocationStep,
};
const DOMAIN: &[u8] = b"acyclic-fs-lazy-directory-cursor-v1\0";
impl LazyDirectoryCursor {
    /// Encodes a frozen continuation on its original native ledger.
    pub fn encode(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(Vec<u8>, crate::NativeAllocation), LazyWorkspaceError> {
        let mut ctx = KernelAccounting::NativeOriginal {
            work,
            cancellation: work.cancellation(),
        };
        let directory = self
            .directory
            .unicode_text_with_accounting(&mut ctx, work.cancellation())?;
        let encoded = encode_fields(self, &directory.0, &mut ctx);
        let bytes = directory.0.capacity() as u64;
        drop(directory.0);
        crate::kernel::retire_allocation(directory.1, bytes, &mut ctx)?;
        let (value, allocation) = encoded?;
        let KernelOwnedAllocation::NativeOriginal(allocation) = allocation else {
            return Err(LazyWorkspaceError::NativeLedgerMismatch);
        };
        Ok((value, allocation))
    }
    /// Decodes a bounded frozen continuation on the supplied native ledger.
    pub fn decode(
        bytes: &[u8],
        profile: crate::FilesystemProfile,
        limits: crate::VolumeLimits,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Self, LazyWorkspaceError> {
        let mut ctx = KernelAccounting::NativeOriginal {
            work,
            cancellation: work.cancellation(),
        };
        let input = crate::kernel::decode_owned(&mut ctx, |mut observe| {
            decode_fields(bytes, profile, limits, &mut observe)
        })?;
        let directory = NamespacePath::parse_in_profile(
            &input.0.1,
            profile,
            limits,
            &mut ctx,
            work.cancellation(),
        )?;
        let bytes = input.0.1.capacity() as u64;
        let mut allocation = input.1;
        let temporary = crate::kernel::split_allocation(&mut allocation, bytes)?;
        drop(input.0.1);
        crate::kernel::retire_allocation(temporary, bytes, &mut ctx)?;
        let mut directory_allocation = directory.1;
        allocation.absorb(&mut directory_allocation)?;
        Ok(Self {
            cut: input.0.0,
            directory: directory.0,
            phase: input.0.2,
            allocation,
        })
    }
}
fn encode_fields(
    cursor: &LazyDirectoryCursor,
    directory: &str,
    ctx: &mut KernelAccounting<'_>,
) -> Result<(Vec<u8>, KernelOwnedAllocation), crate::FsError> {
    let size = DOMAIN.len()
        + 2
        + 141
        + directory.len()
        + match &cursor.phase {
            LazyDirectoryPhase::Source(Some(_)) => 32,
            LazyDirectoryPhase::Authored(Some(name)) => 5 + name.as_bytes().len(),
            _ => 0,
        };
    crate::kernel::codec::encode_owned(ctx, |observe, reserve| {
        let mut encoder = Encoder::with_exact_capacity(
            DOMAIN,
            1,
            size,
            |step| {
                match step {
                    EncodeAllocationStep::BeforeHeader { bytes } => {
                        observe(EncodeWorkStep::Admit(WorkCounters {
                            bytes_copied: bytes,
                            ..WorkCounters::UNCHARGED
                        }))?;
                    }
                    EncodeAllocationStep::HeaderWritten { bytes } => {
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
        let body = WorkCounters {
            bytes_encoded: (size - DOMAIN.len() - 2) as u64,
            bytes_copied: (size - DOMAIN.len() - 2) as u64,
            ..WorkCounters::UNCHARGED
        };
        observe(EncodeWorkStep::Admit(body))?;
        let cut = cursor.cut;
        encoder.fixed(&cut.workspace_id.into_bytes());
        encoder.fixed(cut.authored_generation.digest().as_bytes());
        encoder.fixed(&cut.source.identity);
        encoder.u64(cut.source.epoch);
        encoder.fixed(&cut.overlay.into_bytes());
        encoder.fixed(&cut.shadows.into_bytes());
        encoder.bounded_bytes(directory.as_bytes())?;
        match &cursor.phase {
            LazyDirectoryPhase::Source(None) => encoder.u8(0),
            LazyDirectoryPhase::Source(Some(cursor)) => {
                encoder.u8(1);
                encoder.fixed(&cursor.source.identity);
                encoder.u64(cursor.source.epoch);
                encoder.u64(cursor.token);
            }
            LazyDirectoryPhase::Authored(None) => encoder.u8(2),
            LazyDirectoryPhase::Authored(Some(name)) => {
                encoder.u8(3);
                encoder.u8(name.encoding().tag());
                encoder.bounded_bytes(name.as_bytes())?;
            }
        }
        observe(EncodeWorkStep::Completed(body))?;
        Ok(encoder.finish())
    })
}

fn decode_fields(
    bytes: &[u8],
    profile: crate::FilesystemProfile,
    limits: crate::VolumeLimits,
    observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), crate::FsError>,
) -> Result<(LazySnapshotRef, String, LazyDirectoryPhase), crate::FsError> {
    let maximum = DOMAIN.len() as u64
        + 175
        + u64::from(limits.maximum_path_bytes)
        + u64::from(limits.maximum_component_bytes);
    let mut decoder = Decoder::new(bytes, DOMAIN, 1, maximum, observe)?;
    let workspace = WorkspaceId::from_bytes(decoder.fixed::<16, crate::FsError>(observe)?);
    let generation = crate::GenerationId::new(crate::Digest::from_bytes(
        decoder.fixed::<32, crate::FsError>(observe)?,
    ));
    let source = SourceReference {
        identity: decoder.fixed::<16, crate::FsError>(observe)?,
        epoch: decoder.u64(observe)?,
    };
    let overlay = LazyOverlayId::from_bytes(decoder.fixed::<32, crate::FsError>(observe)?);
    let shadows = LazyShadowId::from_bytes(decoder.fixed::<32, crate::FsError>(observe)?);
    observe(ReadAllocationStep::BeforeHash(
        LAZY_SNAPSHOT_DOMAIN.len() as u64 + 136,
    ))?;
    let cut = LazySnapshotRef::new(workspace, generation, source, overlay, shadows);
    observe(ReadAllocationStep::Hashed(
        LAZY_SNAPSHOT_DOMAIN.len() as u64 + 136,
    ))?;
    let directory = String::from_utf8(decoder.bounded_bytes(limits.maximum_path_bytes, observe)?)
        .map_err(|_| {
        CanonicalDecodeError::Invariant(CanonicalInvariant::Static("cursor directory is not UTF-8"))
    })?;
    let phase = match decoder.u8(observe)? {
        0 => LazyDirectoryPhase::Source(None),
        1 => LazyDirectoryPhase::Source(Some(SourceCursor {
            source: SourceReference {
                identity: decoder.fixed::<16, crate::FsError>(observe)?,
                epoch: decoder.u64(observe)?,
            },
            token: decoder.u64(observe)?,
        })),
        2 => LazyDirectoryPhase::Authored(None),
        3 => {
            let encoding =
                crate::kernel::NameEncoding::from_tag(decoder.u8(observe)?).map_err(|error| {
                    CanonicalDecodeError::Invariant(CanonicalInvariant::Name(error))
                })?;
            if !encoding.is_admitted_in_profile(profile) {
                return Err(CanonicalDecodeError::Invariant(CanonicalInvariant::Static(
                    "cursor name has another profile",
                ))
                .into());
            }
            let name = LogicalName::new(
                encoding,
                decoder.bounded_bytes(limits.maximum_component_bytes, observe)?,
                limits.maximum_component_bytes,
            )
            .map_err(|error| CanonicalDecodeError::Invariant(CanonicalInvariant::Name(error)))?;
            LazyDirectoryPhase::Authored(Some(name))
        }
        tag => {
            return Err(CanonicalDecodeError::UnknownTag {
                field: "cursor phase",
                tag,
            }
            .into());
        }
    };
    decoder.finish()?;
    Ok((cut, directory, phase))
}
