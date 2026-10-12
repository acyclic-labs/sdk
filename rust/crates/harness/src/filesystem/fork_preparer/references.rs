//! Exact child reader and allocation identities.
use super::*;
impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> FilesystemForkPreparer<A, O> {
    pub(super) fn controller(
        &self,
        io: Option<&PreparationIo>,
    ) -> Result<ParentProjectController<'_, A, O>> {
        ParentProjectController::new(
            &self.host,
            &self.parent,
            &self.verifier,
            &self.scope,
            &self.project,
            io,
        )
    }

    /// Projects the exact foreign reader grants for child admission.
    pub(crate) async fn required_reference_grants(
        &self,
        request: &ForkRequest,
        io: Option<&PreparationIo>,
    ) -> Result<Vec<ReferenceGrant>> {
        let attempt;
        let io = match io {
            Some(io) => Some(io),
            None => {
                attempt = self.attempt(self.task_preparation.as_ref())?;
                attempt.io.as_ref()
            }
        };
        self.check_request(request, io, self.task_preparation.as_deref())?;
        let mut grants = self.reference_grants(request, io).await?;
        if request.preparation.summary.is_some() {
            let summary = self.prepare_bound_summary(request, io)?.ok_or_else(|| {
                stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "summary source was not authenticated",
                )
            })?;
            let (_, references) = crate::filesystem::summary_fork::split_references(&summary, io)?;
            add_summary_readers(request, &mut grants, &references, io)?;
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work)
            && let Some(task) = &self.task_preparation
        {
            task.verify_creation_with_native_work(work)?.await?;
        }
        Ok(grants)
    }

    pub(super) async fn reference_grants(
        &self,
        request: &ForkRequest,
        io: Option<&PreparationIo>,
    ) -> Result<Vec<ReferenceGrant>> {
        let maximum_messages = usize::try_from(request.preparation.maximum_inherited_messages)
            .map_err(|_| {
                stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "fork message bound is too large",
                )
            })?;
        #[cfg(not(target_arch = "wasm32"))]
        let native = io.and_then(PreparationIo::original_native_work);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = native
            && let Some(task) = &self.task_preparation
        {
            task.verify_creation_with_native_work(work)?.await?;
        }
        let controller = self.controller(io.filter(|io| io.original_native_work().is_some()))?;
        let (grants, custody) = controller
            .inherited_reference_grants(
                &self.parent,
                request.child_agent,
                &request.attached_agents,
                request.preparation.inherited_through_sequence,
                maximum_messages,
                request.preparation.maximum_inherited_bytes,
                request.preparation.maximum_inherited_references as usize,
                self.resolver.as_ref(),
                self.history.as_ref(),
            )
            .await?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(custody) = custody {
            io.expect("native projection IO")
                .absorb_native_custody(custody)?;
        }
        #[cfg(target_arch = "wasm32")]
        let _ = custody;
        Ok(grants)
    }

    /// Builds the exact created identities and their allocation journal proofs.
    pub(crate) fn creation_allocations(&self, request: &ForkRequest, preparation: crate::TaskId,
        io: Option<&PreparationIo>) -> Result<Vec<crate::resource_preparation::ResourceCreationProofRef>> {
        if io.is_some_and(|io| io.original_native_work().is_some()) {
            self.check_request(request, io, self.task_preparation.as_deref())?;
        } else {
            let mut history = io.map(PreparationIo::history_io);
            request.validate(&mut history.as_mut().map(|history| history as &mut dyn crate::store::history::HistoryIo))?;
            for (volume, _) in creation_volumes(request) {
                if volume.provider() != &self.host.provider {
                    return Err(stock_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "fork resource belongs to another provider",
                    ));
                }
            }
        }
        let count = creation_volumes(request).count();
        let mut proofs = crate::model::vec_with_capacity(count, io)?;
        for (volume, _) in creation_volumes(request) {
            let resource = bounded_io::content_workspace(&self.host.provider, volume, io)?.into_resource();
            proofs.push(crate::resource_preparation::ResourceCreationProofRef {
                provider: match io { Some(io) => self.host.provider.clone_with_io(io)?, None => self.host.provider.clone() },
                preparation, resource, evidence: allocation_ref(&self.host.provider, volume, io)?.into_resource(),
            });
        }
        Ok(proofs)
    }
}

/// Requires the donor to read the inherited file or its authenticated manifest.
pub(in crate::filesystem) fn require_reference_donor(
    grants: &crate::Capabilities,
    reference: &ReferenceGrant,
    io: Option<&PreparationIo>,
) -> Result<()> {
    if io.is_none() {
        reference.validate(&mut None)?;
    }
    let mut history = io;
    let mut observer = history
        .as_mut()
        .map(|history| history as &mut dyn crate::store::history::HistoryIo);
    if crate::runtime::read_granted_with_work(grants, &reference.file, &mut observer)? {
        return Ok(());
    }
    if let Some(manifest) = &reference.attachment_manifest
        && crate::runtime::read_granted_with_work(grants, manifest, &mut observer)?
    {
        return Ok(());
    }
    Err(stock_boundary(
        io,
        crate::wire::ErrorCode::Unauthorized,
        "preparation task cannot delegate an inherited reference it cannot read",
    ))
}

pub(super) fn add_summary_readers(
    request: &ForkRequest,
    grants: &mut Vec<ReferenceGrant>,
    files: &std::collections::BTreeMap<[u8; 32], crate::conversation::FileRef>,
    io: Option<&PreparationIo>,
) -> Result<()> {
    let reject = |reason| stock_boundary(io, crate::wire::ErrorCode::Invalid, reason);
    let maximum = request.preparation.maximum_inherited_references as usize;
    if grants.len() > maximum {
        return Err(reject("summary reader grants exceed capture allowance"));
    }
    let candidates = files
        .len()
        .checked_mul(request.attached_agents.len() + 1)
        .and_then(|count| count.checked_add(grants.len()))
        .ok_or_else(|| reject("summary reader projection overflows"))?;
    crate::filesystem::PreparationIo::reserve(grants, candidates.min(maximum), io)?;
    for reader in
        std::iter::once(request.child_agent).chain(request.attached_agents.iter().copied())
    {
        for file in files.values() {
            if file.volume().owner() == &crate::conversation::VolumeOwner::Agent(reader) {
                continue;
            }
            let mut present = false;
            for grant in &*grants {
                crate::model::charge_items(io, 1)?;
                if grant.reader == reader && grant.file == *file {
                    present = true;
                    break;
                }
            }
            if present {
                continue;
            }
            if grants.len() == maximum {
                return Err(reject("summary reader grants exceed capture allowance"));
            }
            grants.push(ReferenceGrant {
                file: crate::executor::with_history_io(io, |work| file.copy(work))?,
                reader,
                attachment_manifest: None,
            });
        }
    }
    Ok(())
}

use crate::executor::stock_boundary;
