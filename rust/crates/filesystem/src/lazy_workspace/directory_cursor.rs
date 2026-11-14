use super::*;
use crate::kernel::{
    CanonicalDecodeError, CanonicalInvariant, Decoder, EncodeAllocationStep, EncodeWorkStep,
    Encoder, ReadAllocationStep,
};
const DOMAIN: &[u8] = b"acyclic-fs-lazy-directory-cursor-v5\0";
#[cfg(all(feature = "test-support", not(target_arch = "wasm32")))]
#[path = "directory_cursor_fuzz.rs"]
/// On-demand checks of the funded directory continuation codec.
pub mod codec_fuzz;
impl LazyDirectoryCursor {
    /// Encodes a frozen continuation on its original native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn encode(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(Vec<u8>, crate::NativeAllocation), LazyWorkspaceError> {
        let mut ctx = KernelAccounting::NativeOriginal {
            work,
            cancellation: work.cancellation(),
        };
        let encoded = self.encode_with_accounting(&mut ctx);
        let (value, allocation) = encoded?;
        let KernelOwnedAllocation::NativeOriginal(allocation) = allocation else {
            return Err(LazyWorkspaceError::NativeLedgerMismatch);
        };
        Ok((value, allocation))
    }
    /// Encodes the same continuation on any original filesystem accounting context.
    pub fn encode_with_accounting(
        &self,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(Vec<u8>, KernelOwnedAllocation), LazyWorkspaceError> {
        let cxl = cancellation(ctx).clone();
        let directory = self.directory.unicode_text_with_accounting(ctx, &cxl)?;
        let encoded = encode_fields(self, &directory.0, ctx);
        let bytes = directory.0.capacity() as u64;
        drop(directory.0);
        crate::kernel::retire_allocation(directory.1, bytes, ctx)?;
        encoded.map_err(Into::into)
    }
    /// Decodes a bounded frozen continuation on the supplied native ledger.
    #[cfg(not(target_arch = "wasm32"))]
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
        Self::decode_with_accounting(bytes, profile, limits, &mut ctx)
    }
    /// Decodes without introducing another allowance or cursor registry.
    pub fn decode_with_accounting(
        bytes: &[u8],
        profile: crate::FilesystemProfile,
        limits: crate::VolumeLimits,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Self, LazyWorkspaceError> {
        let cxl = cancellation(ctx).clone();
        let mut input = crate::kernel::decode_owned(ctx, |mut observe| {
            decode_fields(bytes, limits, &mut observe)
        })?;
        if let DirectoryCut::Computed(cut) = &mut input.0.0 {
            cut.source.retain_decoded(&mut input.1, ctx)?;
        }
        match &mut input.0.2 {
            LazyDirectoryPhase::Source(Some(cursor)) => {
                cursor.source.retain_decoded(&mut input.1, ctx)?;
            }
            LazyDirectoryPhase::Authored(Some(position)) => {
                crate::kernel::retain_decoded_bytes(&mut position.path, &mut input.1, ctx)?;
            }
            _ => {}
        }
        let mut directory =
            NamespacePath::parse_in_profile(&input.0.1, profile, limits, ctx, &cxl)?;
        let bytes = input.0.1.capacity() as u64;
        let temporary = crate::kernel::split_allocation(&mut input.1, bytes)?;
        drop(input.0.1);
        crate::kernel::retire_allocation(temporary, bytes, ctx)?;
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
        + 22
        + match &cursor.cut {
            DirectoryCut::Authored { .. } => 0,
            DirectoryCut::Computed(cut) => 64 + cut.source.encoded_len(),
        }
        + cursor.cut.generation().object_id().encoded_len()
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
        encoder.u8(match cut {
            DirectoryCut::Authored { .. } => 0,
            DirectoryCut::Computed(_) => 1,
        });
        encoder.fixed(&cut.workspace().into_bytes());
        encoder.object(cut.generation().object_id());
        if let DirectoryCut::Computed(cut) = cut {
            cut.source.encode(|part| encoder.fixed(part));
            encoder.fixed(&cut.overlay.into_bytes());
            encoder.fixed(&cut.shadows.into_bytes());
        }
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
    limits: crate::VolumeLimits,
    observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), crate::FsError>,
) -> Result<(DirectoryCut, String, LazyDirectoryPhase), crate::FsError> {
    let maximum = bytes.len() as u64;
    let mut decoder = Decoder::new(bytes, DOMAIN, 1, maximum, observe)?;
    let kind = decoder.u8(observe)?;
    let workspace = WorkspaceId::from_bytes(decoder.fixed::<16, crate::FsError>(observe)?);
    let generation =
        crate::GenerationId::from_root(decoder.object(crate::ObjectKind::GenerationRoot, observe)?);
    let cut = match kind {
        0 => DirectoryCut::Authored {
            workspace,
            generation,
        },
        1 => {
            let source = SourceReference::decode(&mut decoder, observe)?;
            let overlay = LazyOverlayId::from_bytes(decoder.fixed::<32, crate::FsError>(observe)?);
            let shadows = LazyShadowId::from_bytes(decoder.fixed::<32, crate::FsError>(observe)?);
            let hash_bytes = LazySnapshotRef::hash_bytes(generation, &source);
            observe(ReadAllocationStep::BeforeHash(hash_bytes))?;
            let cut = LazySnapshotRef::new(workspace, generation, source, overlay, shadows);
            observe(ReadAllocationStep::Hashed(hash_bytes))?;
            DirectoryCut::Computed(cut)
        }
        tag => {
            return Err(CanonicalDecodeError::UnknownTag {
                field: "directory cut",
                tag,
            }
            .into());
        }
    };
    let directory = String::from_utf8(decoder.bytes(observe)?).map_err(|_| {
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
    if matches!(cut, DirectoryCut::Authored { .. })
        && matches!(phase, LazyDirectoryPhase::Source(_))
    {
        return Err(CanonicalDecodeError::Invariant(CanonicalInvariant::Static(
            "authored directory cursor has a source phase",
        ))
        .into());
    }
    decoder.finish()?;
    Ok((cut, directory, phase))
}
