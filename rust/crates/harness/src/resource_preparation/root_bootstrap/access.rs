use super::*;
use acyclic_fs::NativeMaterializationWork;
use std::{collections::BTreeSet, mem::size_of};
impl CreatedResourceAccess {
    pub(crate) fn root_bootstrap(
        context: RootBootstrapContentContext<'_>,
        request: &TaskVolumeSnapshot,
        binding: crate::runtime::ContentBindings,
        work: &NativeMaterializationWork,
    ) -> Result<Self> {
        let observer = crate::filesystem::PreparationIo::from_native_work(work)?;
        let mut borrowed = observer.history_io();
        let mut io: Option<&mut dyn HistoryIo> = Some(&mut borrowed);
        let mut labels = BTreeSet::new();
        insert_label(
            &mut labels,
            request.volume.capability(VolumeOperation::Read, &mut io)?,
            &mut io,
        )?;
        // The creator authenticates the exact reader/writer before constructing this seal.
        if binding.writer.is_some() {
            insert_label(
                &mut labels,
                request.volume.capability(VolumeOperation::Write, &mut io)?,
                &mut io,
            )?;
        }
        let mut content = Vec::new();
        reserve(&mut content, 1, &mut io)?;
        let original = io.as_deref_mut().expect("native observer");
        original.retain_owned(size_of::<Self>() as u64)?;
        let authority = original.clone_authority(context.audience())?;
        content.push(CreatedContentBinding {
            volume: request.volume.copy(&mut io)?,
            authority: Some(authority),
            default_writer: request.volume.class() == VolumeClass::AgentPrivate
                && binding.writer.is_some(),
            binding,
            artifact_writer: false,
            scratch_creation: None,
        });
        let mut access = Self {
            operation: context.original_root.operation_id,
            admission_digest: context.admission_digest,
            grants: Capabilities(labels),
            content,
            references: Vec::new(),
            proofs: Vec::new(),
            publication_lease: None,
            native_custody: None,
        };
        access.native_custody = Some(observer.take_native_custody(access.logical_owned_bytes()?)?);
        Ok(access)
    }
    pub(crate) fn merge_root(
        mut self,
        mut next: Self,
        work: &NativeMaterializationWork,
    ) -> Result<Self> {
        work.remaining()?;
        if self.operation != next.operation
            || self.admission_digest != next.admission_digest
            || self.content.len() != 1
            || next.content.len() != 1
            || self.content[0].volume.class() == next.content[0].volume.class()
            || self
                .native_custody
                .as_ref()
                .is_none_or(|g| !g.work().shares_allocation_context(work))
            || next
                .native_custody
                .as_ref()
                .is_none_or(|g| !g.work().shares_allocation_context(work))
        {
            return Err(Error::NativeBoundary {
                code: Unauthorized,
                reason: "invalid original root content",
            });
        }
        let observer = crate::filesystem::PreparationIo::from_native_work(work)?;
        (|| {
            let mut borrowed = observer.history_io();
            let mut io: Option<&mut dyn HistoryIo> = Some(&mut borrowed);
            charge(&mut io, 2)?;
            let original = io.as_deref_mut().expect("native observer");
            original.absorb_native_custody(self.native_custody.take().expect("checked custody"))?;
            original.absorb_native_custody(next.native_custody.take().expect("checked custody"))?;
            reserve(&mut self.content, 2, &mut io)?;
            let moved = slots(2, size_of::<CreatedContentBinding>())?;
            let original = io.as_deref_mut().expect("native observer");
            original.admit_copy(moved)?;
            original.copy_bytes(moved)?;
            self.content.append(&mut next.content);
            self.grants.0.append(&mut next.grants.0);
            drop(next);
            self.native_custody = Some(original.take_native_custody(self.logical_owned_bytes()?)?);
            Ok(self)
        })()
    }
}
