//! Project names reuse the same native/volume directory adapters as context.
use super::*;
use crate::{
    agent::{AgentPath, WorkspaceRole, WorkspaceTable},
    context::{ContextRoot, DiscoveryKind, DiscoverySource, VolumeDiscovery},
    conversation::{PrivateDirectoryEntry, PrivateDirectoryEntryKind},
};

fn entries(page: Vec<crate::context::DiscoveryEntry>) -> Vec<PrivateDirectoryEntry> {
    page.into_iter()
        .filter_map(|entry| {
            if entry.name == ".system" {
                return None;
            }
            Some(PrivateDirectoryEntry {
                name: entry.name,
                kind: match entry.kind {
                    DiscoveryKind::File => PrivateDirectoryEntryKind::File,
                    DiscoveryKind::Directory => PrivateDirectoryEntryKind::Directory,
                    DiscoveryKind::Other => return None,
                },
            })
        })
        .collect()
}

impl LocalHarness {
    async fn skill_directory_source(
        self: &Arc<Self>,
        agent: &AgentPath,
        rows: u32,
    ) -> Result<crate::hosting::DirectorySource> {
        let (agents, _) = self.agents().await?;
        let state = agents.state(agent).await.map_err(storage)?;
        let bound = self
            .agents
            .get()
            .ok_or_else(|| Error::Unsupported("local agents are not hosted".into()))?
            .server()
            .bound();
        let (scope, verifier) = bound.volume_scope(agent).await.map_err(storage)?;
        let reader = bound.content_reader(agent).await.map_err(storage)?;
        let mut binding =
            Binding::for_agent(&state, reader.as_ref(), verifier.clone(), &scope, None)?;
        binding.workspace = self
            .context_workspace(binding.project, &scope, &verifier)
            .await?;
        let snapshots = self
            .bound_skill_snapshots(
                &binding,
                None,
                &mut crate::context::DiscoveryWork::default(),
            )?
            .await?;
        Ok(Box::pin(async_stream::try_stream! {
            let mut remaining = snapshots.iter().map(|snapshot| snapshot.len()).sum::<usize>();
            let (mut source, mut offset) = (0, 0);
            while remaining != 0 {
                let mut entries = Vec::with_capacity((rows as usize).min(remaining));
                while entries.len() < rows as usize && remaining != 0 {
                    let snapshot = snapshots.get(source).ok_or_else(|| Error::Storage("skill source cursor escaped its selection".into()))?;
                    if offset == snapshot.len() { source += 1; offset = 0; continue; }
                    let skill = snapshot.get(offset).ok_or_else(|| Error::Storage("skill name cursor escaped its source".into()))?;
                    entries.push(PrivateDirectoryEntry { name: skill.name.clone(), kind: PrivateDirectoryEntryKind::File });
                    offset += 1;
                    remaining -= 1;
                }
                yield acyclic_harness_client::DirectoryQueryPage { entries, has_more: remaining != 0 };
            }
        }))
    }

    pub(in crate::local) async fn directory_source(
        self: &Arc<Self>,
        agent: &AgentPath,
        target: acyclic_harness_client::DirectoryTarget,
        path: &str,
        rows: u32,
    ) -> Result<crate::hosting::DirectorySource> {
        if target == acyclic_harness_client::DirectoryTarget::Skills {
            return self.skill_directory_source(agent, rows).await;
        }
        if target != acyclic_harness_client::DirectoryTarget::Project {
            return Err(Error::Invalid(
                "private directories use their immutable volume generation".into(),
            ));
        }
        self.agents().await?;
        let bound = self
            .agents
            .get()
            .ok_or_else(|| Error::Unsupported("local agents are not hosted".into()))?
            .server()
            .bound();
        let path = path.to_owned();
        let state = self.agents().await?.0.state(agent).await.map_err(storage)?;
        let project = WorkspaceTable::primary_in(&state.volumes)
            .filter(|entry| entry.role == WorkspaceRole::User)
            .ok_or_else(|| Error::Invalid("selected agent has no project binding".into()))?;
        #[cfg(any(test, not(target_arch = "wasm32")))]
        if self.checkout_context && self.workspace.is_some() {
            let (scope, verifier) = bound.volume_scope(agent).await.map_err(storage)?;
            let workspace = self
                .context_workspace(project.volume()?, &scope, &verifier)
                .await?
                .ok_or_else(|| Error::Invalid("selected native context view is absent".into()))?;
            let path = if project.root.is_empty() {
                path
            } else if path.is_empty() {
                project.root.clone()
            } else {
                format!("{}/{path}", project.root)
            };
            let local = Arc::clone(self);
            return Ok(Box::pin(async_stream::try_stream! {
                let (selected, pending) = workspace.selected_view().await?;
                if let Some((operation, _)) = pending { Err(Error::Indeterminate(OperationId::from_bytes(operation.into_bytes())))?; }
                let view = selected.as_ref().unwrap_or_else(|| workspace.view());
                let cancellation = acyclic_fs::CancellationToken::new();
                let mut selected_cut = None;
                let mut cursor = None;
                loop {
                    // Work credit applies to one source page, never the total
                    // number of names in the directory or query lifetime.
                    let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::performance::BOUNDARY_BUDGET, &cancellation)
                        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
                    let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
                    local.authorize_checkout_context(&workspace, view.workspace(), &io, (&verifier, &scope)).await?;
                    let cut = match selected_cut {
                        Some(cut) => cut,
                        None => view.snapshot(&mut acyclic_fs::kernel::KernelAccounting::NativeOriginal { work: &work, cancellation: &cancellation }).await?,
                    };
                    selected_cut = Some(cut);
                    let mut source = crate::context::native_discovery::LazyDiscovery { view, cut, work: &work };
                    let page = source.page(&path, Some(&cut), cursor.take(), rows).await?;
                    local.authorize_checkout_context(&workspace, view.workspace(), &io, (&verifier, &scope)).await?;
                    let Some(page) = page else { break; };
                    cursor = page.next;
                    yield acyclic_harness_client::DirectoryQueryPage { entries: entries(page.entries), has_more: cursor.is_some() };
                    if cursor.is_none() { break; }
                }
            }));
        }
        let root = ContextRoot {
            volume: project.volume()?.clone(),
            directory: project.root.clone(),
        };
        let reader = bound.content_reader(agent).await.map_err(storage)?;
        Ok(Box::pin(async_stream::try_stream! {
            let mut source = VolumeDiscovery { reader: reader.as_ref(), root: &root, io: None };
            let mut revision = None;
            let mut cursor = None;
            loop {
                let Some(page) = source.page(&path, revision.as_ref(), cursor.take(), rows).await? else { break; };
                revision = Some(page.revision);
                cursor = page.next;
                yield acyclic_harness_client::DirectoryQueryPage { entries: entries(page.entries), has_more: cursor.is_some() };
                if cursor.is_none() { break; }
            }
        }))
    }
}
