use super::*;
use crate::kernel::{
    CanonicalDecodeError, CanonicalInvariant, Decoder, EncodeAllocationStep, EncodeWorkStep,
    Encoder, ReadAllocationStep,
};
const DOMAIN: &[u8] = b"acyclic-fs-lazy-directory-cursor-v4\0";
#[cfg(feature = "test-support")]
#[path = "directory_cursor_fuzz.rs"]
/// On-demand checks of the funded directory continuation codec.
pub mod codec_fuzz;
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
        let mut input = crate::kernel::decode_owned(&mut ctx, |mut observe| {
            decode_fields(bytes, profile, limits, &mut observe)
        })?;
        input.0.0.source.retain_decoded(&mut input.1, &mut ctx)?;
        match &mut input.0.2 {
            LazyDirectoryPhase::Source(Some(cursor)) => {
                cursor.source.retain_decoded(&mut input.1, &mut ctx)?;
            }
            LazyDirectoryPhase::Authored(Some(position)) => {
                crate::kernel::retain_decoded_bytes(&mut position.path, &mut input.1, &mut ctx)?;
            }
            _ => {}
        }
        let mut directory = NamespacePath::parse_in_profile(
            &input.0.1,
            profile,
            limits,
            &mut ctx,
            work.cancellation(),
        )?;
        let bytes = input.0.1.capacity() as u64;
        let temporary = crate::kernel::split_allocation(&mut input.1, bytes)?;
        drop(input.0.1);
        crate::kernel::retire_allocation(temporary, bytes, &mut ctx)?;
        input.1.absorb(&mut directory.1)?;
        Ok(Self {
            cut: input.0.0,
            directory: directory.0,
            phase: input.0.2,
            allocation: input.1,
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
        + 85
        + cursor.cut.source.encoded_len()
        + cursor.cut.authored_generation.object_id().encoded_len()
        + directory.len()
        + match &cursor.phase {
            LazyDirectoryPhase::Source(Some(source)) => source.source.encoded_len() + 8,
            LazyDirectoryPhase::Authored(Some(position)) => {
                position.root.encoded_len() + 8 + position.path.len()
            }
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
        let cut = &cursor.cut;
        encoder.fixed(&cut.workspace_id.into_bytes());
        encoder.object(cut.authored_generation.object_id());
        cut.source.encode(|part| encoder.fixed(part));
        encoder.fixed(&cut.overlay.into_bytes());
        encoder.fixed(&cut.shadows.into_bytes());
        encoder.bounded_bytes(directory.as_bytes())?;
        match &cursor.phase {
            LazyDirectoryPhase::Source(None) => encoder.u8(0),
            LazyDirectoryPhase::Source(Some(cursor)) => {
                encoder.u8(1);
                cursor.source.encode(|part| encoder.fixed(part));
                encoder.u64(cursor.token);
            }
            LazyDirectoryPhase::Authored(None) => encoder.u8(2),
            LazyDirectoryPhase::Authored(Some(position)) => {
                encoder.u8(3);
                encoder.object(position.root);
                encoder.bounded_bytes(position.path.as_slice())?;
                encoder.u32(position.offset);
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
    let maximum = bytes.len() as u64;
    let mut decoder = Decoder::new(bytes, DOMAIN, 1, maximum, observe)?;
    let workspace = WorkspaceId::from_bytes(decoder.fixed::<16, crate::FsError>(observe)?);
    let generation =
        crate::GenerationId::from_root(decoder.object(crate::ObjectKind::GenerationRoot, observe)?);
    let source = SourceReference::decode(&mut decoder, observe)?;
    let overlay = LazyOverlayId::from_bytes(decoder.fixed::<32, crate::FsError>(observe)?);
    let shadows = LazyShadowId::from_bytes(decoder.fixed::<32, crate::FsError>(observe)?);
    let hash_bytes = LazySnapshotRef::hash_bytes(generation, &source);
    observe(ReadAllocationStep::BeforeHash(hash_bytes))?;
    let cut = LazySnapshotRef::new(workspace, generation, source, overlay, shadows);
    observe(ReadAllocationStep::Hashed(hash_bytes))?;
    let directory = String::from_utf8(decoder.bounded_bytes(limits.maximum_path_bytes, observe)?)
        .map_err(|_| {
        CanonicalDecodeError::Invariant(CanonicalInvariant::Static("cursor directory is not UTF-8"))
    })?;
    let phase = match decoder.u8(observe)? {
        0 => LazyDirectoryPhase::Source(None),
        1 => LazyDirectoryPhase::Source(Some(SourceCursor::detached(
            SourceReference::decode(&mut decoder, observe)?,
            decoder.u64(observe)?,
        ))),
        2 => LazyDirectoryPhase::Authored(None),
        3 => {
            let root = decoder.object(crate::ObjectKind::TreePage, observe)?;
            let path = decoder.bounded_bytes(u32::from(limits.maximum_page_height) * 4, observe)?;
            if path.len() % 4 != 0 || path.len() / 4 >= usize::from(limits.maximum_page_height) {
                return Err(CanonicalDecodeError::Invariant(CanonicalInvariant::Static(
                    "cursor tree path has invalid height",
                ))
                .into());
            }
            let offset = decoder.u32(observe)?;
            LazyDirectoryPhase::Authored(Some(crate::kernel::PagePosition {
                root,
                path: acyclic_stream::OwnedBytes::from(path),
                offset,
            }))
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
